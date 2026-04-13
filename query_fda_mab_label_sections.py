#!/usr/bin/env python3
"""Query openFDA label sections for a list of base monoclonal antibodies.

This script reads a CSV with a `base_name` column (for example `mabs_base.csv`),
queries openFDA drug label records, and exports long-form text for:
- indications_and_usage
- dosage_and_administration
- dosage_forms_and_strengths

Examples:
  /workspaces/FDA-dosing/.venv/bin/python query_fda_mab_label_sections.py
  /workspaces/FDA-dosing/.venv/bin/python query_fda_mab_label_sections.py --limit-mabs 5
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

OPENFDA_LABEL_URL = "https://api.fda.gov/drug/label.json"


def read_base_names(input_csv: str) -> list[str]:
    """Read unique base mAb names from a CSV file with a `base_name` column."""
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "base_name" not in (reader.fieldnames or []):
            raise ValueError(f"Input CSV must contain a 'base_name' column: {input_csv}")

        names = {row["base_name"].strip().lower() for row in reader if row.get("base_name", "").strip()}

    return sorted(names)


def fetch_label_page(search: str, limit: int, skip: int, retries: int = 3) -> dict:
    """Fetch one page from openFDA drug label endpoint."""
    params = {"search": search, "limit": limit, "skip": skip}
    url = OPENFDA_LABEL_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})

    attempt = 0
    while True:
        attempt += 1
        try:
            with urllib.request.urlopen(req, timeout=40) as resp:
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


def list_to_text(value: object) -> str:
    """Join openFDA section arrays into one long text blob."""
    if isinstance(value, list):
        parts = [str(v).strip() for v in value if str(v).strip()]
        return "\n\n".join(parts)
    if value is None:
        return ""
    return str(value).strip()


def first_openfda_values(item: dict, key: str) -> str:
    """Get pipe-separated openfda values for a key."""
    vals = item.get("openfda", {}).get(key, [])
    if not isinstance(vals, list):
        return ""
    cleaned = [str(v).strip() for v in vals if str(v).strip()]
    return "|".join(cleaned)


def normalize_text(value: str) -> str:
    """Normalize text for stable deduplication keys."""
    return value.strip().lower()


def effective_time_value(item: dict) -> int:
    """Convert openFDA effective_time values into a sortable integer."""
    raw_value = str(item.get("effective_time", "") or "").strip()
    try:
        return int(raw_value)
    except ValueError:
        return -1


def extract_label_row(base_name: str, item: dict) -> dict[str, str]:
    """Extract target sections and metadata from one label record."""
    return {
        "base_name": base_name,
        "set_id": str(item.get("set_id", "") or ""),
        "id": str(item.get("id", "") or ""),
        "effective_time": str(item.get("effective_time", "") or ""),
        "version": str(item.get("version", "") or ""),
        "generic_name": first_openfda_values(item, "generic_name"),
        "brand_name": first_openfda_values(item, "brand_name"),
        "manufacturer_name": first_openfda_values(item, "manufacturer_name"),
        "indications_and_usage": list_to_text(item.get("indications_and_usage")),
        "dosage_and_administration": list_to_text(item.get("dosage_and_administration")),
        "dosage_forms_and_strengths": list_to_text(item.get("dosage_forms_and_strengths")),
    }


def query_label_sections(
    base_names: list[str],
    page_size: int,
    max_records_per_mab: int,
    sleep_seconds: float,
) -> list[dict[str, str]]:
    """Query openFDA label records for all base mAbs and extract sections."""
    latest_rows: dict[tuple[str, str], dict[str, str]] = {}

    for idx, base_name in enumerate(base_names, start=1):
        search = f'openfda.generic_name:"{base_name}"'
        skip = 0
        fetched = 0

        while fetched < max_records_per_mab:
            current_limit = min(page_size, max_records_per_mab - fetched)

            try:
                payload = fetch_label_page(search=search, limit=current_limit, skip=skip)
            except urllib.error.HTTPError as err:
                if err.code == 404:
                    break
                raise

            results = payload.get("results", [])
            if not results:
                break

            for item in results:
                row = extract_label_row(base_name, item)
                generic_name = normalize_text(row["generic_name"])
                brand_name = normalize_text(row["brand_name"])
                dedupe_key = (generic_name, brand_name)

                existing_row = latest_rows.get(dedupe_key)
                if existing_row is None or effective_time_value(row) > effective_time_value(existing_row):
                    latest_rows[dedupe_key] = row

            batch_count = len(results)
            fetched += batch_count
            skip += batch_count

            if batch_count < current_limit:
                break

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        print(f"Processed {idx}/{len(base_names)}: {base_name}", file=sys.stderr)

    return sorted(
        latest_rows.values(),
        key=lambda row: (
            row["base_name"],
            normalize_text(row["generic_name"]),
            normalize_text(row["brand_name"]),
            row["effective_time"],
        ),
    )


def write_csv(rows: list[dict[str, str]], output_csv: str) -> None:
    """Write long text sections to CSV (quoted as needed)."""
    fieldnames = [
        "base_name",
        "set_id",
        "id",
        "effective_time",
        "version",
        "generic_name",
        "brand_name",
        "manufacturer_name",
        "indications_and_usage",
        "dosage_and_administration",
        "dosage_forms_and_strengths",
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch mAb label sections from openFDA")
    parser.add_argument("--input-csv", default="mabs_base.csv", help="input CSV with base_name column")
    parser.add_argument("--output-csv", default="mabs_label_sections.csv", help="output CSV path")
    parser.add_argument("--page-size", type=int, default=100, help="records per API call (default: 100)")
    parser.add_argument(
        "--max-records-per-mab",
        type=int,
        default=300,
        help="maximum label records to scan per base name (default: 300)",
    )
    parser.add_argument("--sleep", type=float, default=0.12, help="delay between paged requests in seconds")
    parser.add_argument(
        "--limit-mabs",
        type=int,
        default=0,
        help="for testing: only process first N base names (0 means all)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        base_names = read_base_names(args.input_csv)
        if args.limit_mabs > 0:
            base_names = base_names[: args.limit_mabs]

        rows = query_label_sections(
            base_names=base_names,
            page_size=args.page_size,
            max_records_per_mab=args.max_records_per_mab,
            sleep_seconds=args.sleep,
        )
        write_csv(rows, args.output_csv)
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

    print(f"Wrote {len(rows)} rows to {args.output_csv}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
