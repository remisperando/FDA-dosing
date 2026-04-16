#!/usr/bin/env python3
"""Query openFDA NDC data for each mAb in mabs.csv and output 11-digit NDCs.

Reads a CSV with a `name` column (default: mabs.csv) and writes one output row
per package-level NDC for each input drug name. The script preserves raw NDC
values and also emits FDA-standard 11-digit NDC format (5-4-2).

Examples:
  /workspaces/FDA-dosing/.venv/bin/python query_fda_mab_ndc_codes.py
  /workspaces/FDA-dosing/.venv/bin/python query_fda_mab_ndc_codes.py --input-csv mabs.csv --output-csv mabs_ndc_codes.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

OPENFDA_NDC_URL = "https://api.fda.gov/drug/ndc.json"
NON_DIGIT_PATTERN = re.compile(r"\D+")


def read_mab_names(input_csv: str) -> list[str]:
    """Read unique mAb names from a CSV file with a `name` column."""
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "name" not in (reader.fieldnames or []):
            raise ValueError(f"Input CSV must contain a 'name' column: {input_csv}")

        names = {row["name"].strip().lower() for row in reader if row.get("name", "").strip()}

    return sorted(names)


def fetch_ndc_page(search: str, limit: int, skip: int, retries: int = 3) -> dict:
    """Fetch one page from openFDA NDC endpoint with retry handling."""
    params = {"search": search, "limit": limit, "skip": skip}
    url = OPENFDA_NDC_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})

    attempt = 0
    while True:
        attempt += 1
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            if err.code in {500, 502, 503, 504} and attempt < retries:
                time.sleep(0.5 * attempt)
                continue
            raise
        except urllib.error.URLError:
            if attempt < retries:
                time.sleep(0.5 * attempt)
                continue
            raise


def normalize_ndc_to_11(ndc_value: str) -> tuple[str, str, str, str]:
    """Normalize NDC to 11-digit 5-4-2 and return (ndc11, labeler, product, package)."""
    raw = (ndc_value or "").strip()
    if not raw:
        return "", "", "", ""

    if "-" in raw:
        parts = [p.strip() for p in raw.split("-") if p.strip()]
        if len(parts) == 3:
            labeler, product, package = parts
            if len(labeler) <= 5 and len(product) <= 4 and len(package) <= 2:
                labeler = labeler.zfill(5)
                product = product.zfill(4)
                package = package.zfill(2)
                return f"{labeler}{product}{package}", labeler, product, package

    digits = NON_DIGIT_PATTERN.sub("", raw)
    if len(digits) == 11:
        labeler = digits[:5]
        product = digits[5:9]
        package = digits[9:11]
        return digits, labeler, product, package

    if len(digits) == 10:
        # When hyphen layout is unavailable, infer using official 10-digit patterns.
        # Preference order here reflects the most common package-NDC shape in openFDA.
        for l_len, p_len, pk_len in ((5, 3, 2), (5, 4, 1), (4, 4, 2)):
            if l_len + p_len + pk_len != 10:
                continue
            labeler = digits[:l_len].zfill(5)
            product = digits[l_len : l_len + p_len].zfill(4)
            package = digits[l_len + p_len :].zfill(2)
            return f"{labeler}{product}{package}", labeler, product, package

    return "", "", "", ""


def build_rows_for_result(input_name: str, result: dict) -> list[dict[str, str]]:
    """Flatten one openFDA NDC result into package-level rows."""
    brand_name = str(result.get("brand_name", "") or "")
    labeler_name = str(result.get("labeler_name", "") or "")

    packaging = result.get("packaging")
    if not isinstance(packaging, list) or not packaging:
        packaging = [{"package_ndc": ""}]

    rows: list[dict[str, str]] = []
    for package in packaging:
        package_ndc_raw = str((package or {}).get("package_ndc", "") or "")
        ndc11, _, _, _ = normalize_ndc_to_11(package_ndc_raw)

        if not ndc11:
            continue

        rows.append(
            {
                "input_name": input_name,
                "brand_name": brand_name,
                "labeler_name": labeler_name,
                "ndc11": ndc11,
            }
        )

    return rows


def query_ndcs_for_name(name: str, page_size: int, max_records: int, sleep_seconds: float) -> list[dict[str, str]]:
    """Query all matching NDC records for one input drug name."""
    rows: list[dict[str, str]] = []
    skip = 0
    fetched = 0
    search = f'generic_name:"{name}"'

    while fetched < max_records:
        current_limit = min(page_size, max_records - fetched)

        try:
            payload = fetch_ndc_page(search=search, limit=current_limit, skip=skip)
        except urllib.error.HTTPError as err:
            if err.code == 404:
                break
            raise

        results = payload.get("results", [])
        if not results:
            break

        for result in results:
            rows.extend(build_rows_for_result(name, result))

        batch_count = len(results)
        fetched += batch_count
        skip += batch_count

        if batch_count < current_limit:
            break

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return rows


def write_rows(output_csv: str, rows: list[dict[str, str]]) -> None:
    """Write package-level NDC rows to CSV."""
    fieldnames = [
        "input_name",
        "brand_name",
        "labeler_name",
        "ndc11",
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch 11-digit NDC codes for mAbs from openFDA")
    parser.add_argument("--input-csv", default="mabs.csv", help="input CSV path with a name column")
    parser.add_argument("--output-csv", default="mabs_ndc_codes.csv", help="output CSV path")
    parser.add_argument("--page-size", type=int, default=100, help="records per request (default: 100)")
    parser.add_argument(
        "--max-records-per-name",
        type=int,
        default=500,
        help="maximum NDC product records to scan per input name (default: 500)",
    )
    parser.add_argument("--sleep", type=float, default=0.12, help="delay between paged requests in seconds")
    parser.add_argument("--limit", type=int, default=0, help="for testing: process only first N names (0 means all)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        names = read_mab_names(args.input_csv)
        if args.limit > 0:
            names = names[: args.limit]

        all_rows: list[dict[str, str]] = []
        for idx, name in enumerate(names, start=1):
            all_rows.extend(
                query_ndcs_for_name(
                    name=name,
                    page_size=args.page_size,
                    max_records=args.max_records_per_name,
                    sleep_seconds=args.sleep,
                )
            )
            print(f"Processed {idx}/{len(names)}: {name}", file=sys.stderr)

        write_rows(args.output_csv, all_rows)
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

    print(f"Wrote {len(all_rows)} rows to {args.output_csv}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
