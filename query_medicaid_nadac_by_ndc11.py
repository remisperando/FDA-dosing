#!/usr/bin/env python3
"""Query Medicaid NADAC yearly datasets by NDC11 and join to mAb NDC rows.

Input CSV is expected to contain at least these columns:
- input_name
- brand_name
- labeler_name
- ndc11

For each NDC11, the script pulls:
- latest NDC description from the 2025 NADAC dataset
- latest NADAC per unit and pricing unit from each yearly dataset (2025-2022)

Examples:
  /workspaces/FDA-dosing/.venv/bin/python query_medicaid_nadac_by_ndc11.py
  /workspaces/FDA-dosing/.venv/bin/python query_medicaid_nadac_by_ndc11.py \
    --input-csv mabs_ndc_codes.csv \
    --output-csv mabs_ndc_nadac_2025_2022.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.request

API_BASE = "https://data.medicaid.gov/api/1/datastore/query"
NDC_DIGITS_PATTERN = re.compile(r"\D+")

# User-supplied year mapping in order 2025, 2024, 2023, 2022.
NADAC_DATASET_BY_YEAR: dict[int, str] = {
    2025: "f38d0706-1239-442c-a3cc-40ef1b686ac0",
    2024: "99315a95-37ac-4eee-946a-3c523b4c481e",
    2023: "4a00010a-132b-4e4d-a611-543c9521280f",
    2022: "dfa2ab14-06c2-457a-9e36-5cb6d80f8d93",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Join mAb NDC11 list to Medicaid NADAC yearly prices")
    parser.add_argument("--input-csv", default="mabs_ndc_codes.csv", help="input CSV path with ndc11 column")
    parser.add_argument(
        "--output-csv",
        default="mabs_ndc_nadac_2025_2022.csv",
        help="output CSV path with joined NADAC columns",
    )
    parser.add_argument("--sleep", type=float, default=0.0, help="optional delay between API requests in seconds")
    parser.add_argument("--retries", type=int, default=3, help="retries for transient API failures (default: 3)")
    parser.add_argument("--timeout", type=int, default=30, help="request timeout in seconds (default: 30)")
    parser.add_argument(
        "--row-limit",
        type=int,
        default=500,
        help="rows per datastore page request (default: 500)",
    )
    parser.add_argument(
        "--ndc-batch-size",
        type=int,
        default=200,
        help="NDC count per IN-query batch (default: 200)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="for testing: only process first N input rows (0 means all)",
    )
    return parser.parse_args()


def normalize_ndc11(value: str) -> str:
    """Normalize input NDC to 11-digit numeric string when possible."""
    digits = NDC_DIGITS_PATTERN.sub("", (value or "").strip())
    return digits if len(digits) == 11 else ""


def read_input_rows(input_csv: str, limit: int = 0) -> list[dict[str, str]]:
    """Read input rows from CSV and enforce required columns."""
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"input_name", "brand_name", "labeler_name", "ndc11"}
        fields = set(reader.fieldnames or [])
        missing = sorted(required - fields)
        if missing:
            missing_cols = ", ".join(missing)
            raise ValueError(f"Input CSV is missing required columns: {missing_cols}")

        rows = []
        for row in reader:
            ndc11 = normalize_ndc11(row.get("ndc11", ""))
            if not ndc11:
                continue

            rows.append(
                {
                    "input_name": (row.get("input_name", "") or "").strip(),
                    "brand_name": (row.get("brand_name", "") or "").strip(),
                    "labeler_name": (row.get("labeler_name", "") or "").strip(),
                    "ndc11": ndc11,
                }
            )

            if limit > 0 and len(rows) >= limit:
                break

    return rows


def post_datastore_query(dataset_id: str, query_payload: dict, timeout: int, retries: int) -> dict:
    """POST one datastore query request with retry handling."""
    url = f"{API_BASE}/{dataset_id}/0"
    body = json.dumps(query_payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )

    attempt = 0
    while True:
        attempt += 1
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            if err.code in {429, 500, 502, 503, 504} and attempt < retries:
                time.sleep(0.5 * attempt)
                continue
            raise
        except urllib.error.URLError:
            if attempt < retries:
                time.sleep(0.5 * attempt)
                continue
            raise


def chunked(values: list[str], chunk_size: int) -> list[list[str]]:
    """Split a list into fixed-size chunks."""
    return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]


def fetch_latest_nadac_for_year(
    *,
    ndcs: list[str],
    dataset_id: str,
    timeout: int,
    retries: int,
    row_limit: int,
    ndc_batch_size: int,
    sleep_seconds: float,
) -> dict[str, dict[str, str]]:
    """Fetch latest NADAC rows for a year using paginated batch queries."""
    latest_by_ndc: dict[str, dict[str, str]] = {}
    ndc_batches = chunked(ndcs, ndc_batch_size)

    for batch_index, batch in enumerate(ndc_batches, start=1):
        batch_set = set(batch)
        offset = 0

        while True:
            payload = {
                "properties": ["ndc", "ndc_description", "nadac_per_unit", "pricing_unit", "effective_date", "as_of_date"],
                "conditions": [{"property": "ndc", "operator": "in", "value": batch}],
                "sorts": [
                    {"property": "ndc", "order": "asc"},
                    {"property": "effective_date", "order": "desc"},
                ],
                "limit": row_limit,
                "offset": offset,
                "count": False,
                "schema": False,
                "results": True,
                "keys": True,
            }

            result = post_datastore_query(
                dataset_id=dataset_id,
                query_payload=payload,
                timeout=timeout,
                retries=retries,
            )
            rows = result.get("results", [])
            if not rows:
                break

            for row in rows:
                ndc = normalize_ndc11(str(row.get("ndc", "") or ""))
                if not ndc or ndc not in batch_set:
                    continue

                effective_date = str(row.get("effective_date", "") or "")
                existing = latest_by_ndc.get(ndc)

                if not existing or effective_date > existing.get("effective_date", ""):
                    latest_by_ndc[ndc] = {
                        "ndc": ndc,
                        "ndc_description": str(row.get("ndc_description", "") or ""),
                        "nadac_per_unit": str(row.get("nadac_per_unit", "") or ""),
                        "pricing_unit": str(row.get("pricing_unit", "") or ""),
                        "effective_date": effective_date,
                        "as_of_date": str(row.get("as_of_date", "") or ""),
                    }

            if len(rows) < row_limit:
                break

            offset += row_limit
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        print(
            f"Completed dataset {dataset_id} batch {batch_index}/{len(ndc_batches)}",
            file=sys.stderr,
        )

    return latest_by_ndc


def build_joined_rows(
    input_rows: list[dict[str, str]],
    timeout: int,
    retries: int,
    sleep_seconds: float,
    row_limit: int,
    ndc_batch_size: int,
) -> list[dict[str, str]]:
    """Join input NDC rows to yearly latest NADAC values."""
    unique_ndcs = sorted({row["ndc11"] for row in input_rows})
    cache: dict[int, dict[str, dict[str, str]]] = {}

    for year in sorted(NADAC_DATASET_BY_YEAR.keys(), reverse=True):
        dataset_id = NADAC_DATASET_BY_YEAR[year]
        cache[year] = fetch_latest_nadac_for_year(
            ndcs=unique_ndcs,
            dataset_id=dataset_id,
            timeout=timeout,
            retries=retries,
            row_limit=row_limit,
            ndc_batch_size=ndc_batch_size,
            sleep_seconds=sleep_seconds,
        )
        print(
            f"Completed year {year}: matched {len(cache[year])}/{len(unique_ndcs)} NDCs",
            file=sys.stderr,
        )

    joined_rows: list[dict[str, str]] = []
    for row in input_rows:
        ndc11 = row["ndc11"]

        data_2025 = cache.get(2025, {}).get(ndc11, {})
        data_2024 = cache.get(2024, {}).get(ndc11, {})
        data_2023 = cache.get(2023, {}).get(ndc11, {})
        data_2022 = cache.get(2022, {}).get(ndc11, {})

        joined_rows.append(
            {
                "input_name": row["input_name"],
                "brand_name": row["brand_name"],
                "labeler_name": row["labeler_name"],
                "ndc11": ndc11,
                "ndc_description": data_2025.get("ndc_description", ""),
                "pricing_unit": data_2025.get("pricing_unit", ""),
                "nadac_per_unit_2025": data_2025.get("nadac_per_unit", ""),
                "nadac_per_unit_2024": data_2024.get("nadac_per_unit", ""),
                "nadac_per_unit_2023": data_2023.get("nadac_per_unit", ""),
                "nadac_per_unit_2022": data_2022.get("nadac_per_unit", ""),
            }
        )

    return joined_rows


def write_output_csv(output_csv: str, rows: list[dict[str, str]]) -> None:
    """Write joined NADAC output CSV."""
    fieldnames = [
        "input_name",
        "brand_name",
        "labeler_name",
        "ndc11",
        "ndc_description",
        "pricing_unit",
        "nadac_per_unit_2025",
        "nadac_per_unit_2024",
        "nadac_per_unit_2023",
        "nadac_per_unit_2022",
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()

    try:
        if args.row_limit < 1:
            raise ValueError("--row-limit must be >= 1")
        if args.ndc_batch_size < 1:
            raise ValueError("--ndc-batch-size must be >= 1")

        input_rows = read_input_rows(args.input_csv, limit=args.limit)
        if not input_rows:
            print("No usable rows found in input CSV.", file=sys.stderr)
            return 2

        output_rows = build_joined_rows(
            input_rows=input_rows,
            timeout=args.timeout,
            retries=args.retries,
            sleep_seconds=args.sleep,
            row_limit=args.row_limit,
            ndc_batch_size=args.ndc_batch_size,
        )
        write_output_csv(args.output_csv, output_rows)
    except FileNotFoundError as err:
        print(f"File not found: {err}", file=sys.stderr)
        return 2
    except ValueError as err:
        print(str(err), file=sys.stderr)
        return 2
    except urllib.error.HTTPError as err:
        print(f"HTTP error {err.code}: {err.reason}", file=sys.stderr)
        return 2
    except urllib.error.URLError as err:
        print(f"Network error: {err.reason}", file=sys.stderr)
        return 2
    except TimeoutError:
        print("Request timed out", file=sys.stderr)
        return 2

    print(f"Wrote {len(output_rows)} rows to {args.output_csv}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
