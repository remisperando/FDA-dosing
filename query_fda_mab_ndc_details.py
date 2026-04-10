#!/usr/bin/env python3
"""Query openFDA NDC data for mAb dosing and packaging details.

Reads a CSV with a `base_name` column (for example `mabs_base.csv`) and writes
one output row per (product, package, active_ingredient) combination.

Examples:
  /workspaces/FDA-dosing/.venv/bin/python query_fda_mab_ndc_details.py
  /workspaces/FDA-dosing/.venv/bin/python query_fda_mab_ndc_details.py --input-csv mabs_base.csv --output-csv mabs_ndc_details.csv
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
from collections import defaultdict

OPENFDA_NDC_URL = "https://api.fda.gov/drug/ndc.json"
STRENGTH_PER_VOLUME_PATTERN = re.compile(
    r"(?P<amount>\d*\.?\d+)\s*(?P<amount_unit>mg|g|mcg|ug)\s*/\s*(?P<volume>\d*\.?\d+)\s*(?P<volume_unit>ml|l)",
    re.IGNORECASE,
)
STRENGTH_PER_UNIT_VOLUME_PATTERN = re.compile(
    r"(?P<amount>\d*\.?\d+)\s*(?P<amount_unit>mg|g|mcg|ug)\s*/\s*(?P<volume_unit>ml|l)",
    re.IGNORECASE,
)
STRENGTH_PER_ONE_UNIT_PATTERN = re.compile(
    r"(?P<amount>\d*\.?\d+)\s*(?P<amount_unit>mg|g|mcg|ug)\s*/\s*1(?:\.0+)?(?:\s*(?P<den_unit>[a-z]+))?",
    re.IGNORECASE,
)
MAB_NAME_TOKEN_PATTERN = re.compile(r"\b[a-z][a-z0-9-]*mab(?:-[a-z0-9]+)?\b", re.IGNORECASE)
MAB_FDA_SUFFIX_PATTERN = re.compile(r"^(?P<base>[a-z][a-z0-9-]*mab)-[a-z]{4}$", re.IGNORECASE)


def read_base_names(input_csv: str) -> list[str]:
    """Read unique base mAb names from a CSV file with a `base_name` column."""
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "base_name" not in (reader.fieldnames or []):
            raise ValueError(f"Input CSV must contain a 'base_name' column: {input_csv}")

        names = {row["base_name"].strip().lower() for row in reader if row.get("base_name", "").strip()}

    return sorted(names)


def fetch_ndc_page(search: str, limit: int, skip: int, retries: int = 3) -> dict:
    """Fetch one page from openFDA NDC endpoint with small retry handling."""
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
            # openFDA occasionally returns transient 500/502/503 responses.
            if err.code in {500, 502, 503, 504} and attempt < retries:
                time.sleep(0.5 * attempt)
                continue
            raise
        except urllib.error.URLError:
            if attempt < retries:
                time.sleep(0.5 * attempt)
                continue
            raise


def listify(value: object) -> list[str]:
    """Convert a scalar/list/None to a clean list of strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    text = str(value).strip()
    return [text] if text else []


def extract_rows_for_product(base_name: str, product: dict) -> list[dict[str, str]]:
    """Flatten one NDC product into output rows."""
    generic_name = str(product.get("generic_name", "") or "")
    brand_name = str(product.get("brand_name", "") or "")
    labeler_name = str(product.get("labeler_name", "") or "")
    dosage_form = str(product.get("dosage_form", "") or "")
    route = "|".join(listify(product.get("route")))
    marketing_category = str(product.get("marketing_category", "") or "")
    product_type = str(product.get("product_type", "") or "")
    finished = str(product.get("finished", "") or "")
    product_ndc = str(product.get("product_ndc", "") or "")
    listing_expiration_date = str(product.get("listing_expiration_date", "") or "")

    active_ingredients = product.get("active_ingredients")
    if not isinstance(active_ingredients, list) or not active_ingredients:
        active_ingredients = [{"name": "", "strength": ""}]

    packaging = product.get("packaging")
    if not isinstance(packaging, list) or not packaging:
        packaging = [{"package_ndc": "", "description": ""}]

    rows: list[dict[str, str]] = []
    for ingredient in active_ingredients:
        ing_name = str((ingredient or {}).get("name", "") or "")
        ing_strength = str((ingredient or {}).get("strength", "") or "")

        for pack in packaging:
            package_ndc = str((pack or {}).get("package_ndc", "") or "")
            package_description = str((pack or {}).get("description", "") or "")
            rows.append(
                {
                    "base_name": base_name,
                    "generic_name": generic_name,
                    "brand_name": brand_name,
                    "labeler_name": labeler_name,
                    "dosage_form": dosage_form,
                    "route": route,
                    "marketing_category": marketing_category,
                    "product_type": product_type,
                    "finished": finished,
                    "product_ndc": product_ndc,
                    "package_ndc": package_ndc,
                    "package_description": package_description,
                    "active_ingredient_name": ing_name,
                    "active_ingredient_strength": ing_strength,
                    "listing_expiration_date": listing_expiration_date,
                }
            )

    return rows


def query_ndc_details(base_names: list[str], page_size: int, max_records_per_mab: int, sleep_seconds: float) -> list[dict[str, str]]:
    """Query NDC for each base mAb name and flatten results."""
    all_rows: list[dict[str, str]] = []

    for idx, base_name in enumerate(base_names, start=1):
        # Phrase match keeps this fairly specific but still captures suffixed names.
        search = f'generic_name:"{base_name}"'
        skip = 0
        fetched = 0

        while fetched < max_records_per_mab:
            current_limit = min(page_size, max_records_per_mab - fetched)

            try:
                payload = fetch_ndc_page(search=search, limit=current_limit, skip=skip)
            except urllib.error.HTTPError as err:
                if err.code == 404:
                    break
                raise

            results = payload.get("results", [])
            if not results:
                break

            for product in results:
                all_rows.extend(extract_rows_for_product(base_name, product))

            batch_count = len(results)
            fetched += batch_count
            skip += batch_count

            if batch_count < current_limit:
                break

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        print(f"Processed {idx}/{len(base_names)}: {base_name}", file=sys.stderr)

    return all_rows


def write_output_csv(rows: list[dict[str, str]], output_csv: str) -> None:
    """Write flattened NDC rows to CSV."""
    fieldnames = [
        "base_name",
        "generic_name",
        "brand_name",
        "labeler_name",
        "dosage_form",
        "route",
        "marketing_category",
        "product_type",
        "finished",
        "product_ndc",
        "package_ndc",
        "package_description",
        "active_ingredient_name",
        "active_ingredient_strength",
        "listing_expiration_date",
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def normalize_strength_to_mg_per_ml(strength_text: str) -> float | None:
    """Parse common strength strings and normalize to mg/mL when possible."""
    text = (strength_text or "").strip().lower()
    match = STRENGTH_PER_VOLUME_PATTERN.search(text)
    if match:
        amount = float(match.group("amount"))
        amount_unit = match.group("amount_unit").lower()
        volume = float(match.group("volume"))
        volume_unit = match.group("volume_unit").lower()
    else:
        # Handle common implicit denominator forms like "150 mg/mL" or "50 g/L".
        unit_match = STRENGTH_PER_UNIT_VOLUME_PATTERN.search(text)
        if not unit_match:
            return None

        amount = float(unit_match.group("amount"))
        amount_unit = unit_match.group("amount_unit").lower()
        volume = 1.0
        volume_unit = unit_match.group("volume_unit").lower()

    if amount_unit == "g":
        amount_mg = amount * 1000.0
    elif amount_unit in {"mcg", "ug"}:
        amount_mg = amount * 0.001
    else:
        amount_mg = amount

    if volume_unit == "l":
        volume_ml = volume * 1000.0
    else:
        volume_ml = volume

    if volume_ml <= 0:
        return None

    return amount_mg / volume_ml


def amount_to_mg(amount: float, unit: str) -> float:
    """Convert mass value to mg."""
    unit = unit.lower()
    if unit == "g":
        return amount * 1000.0
    if unit in {"mcg", "ug"}:
        return amount * 0.001
    return amount


def extract_single_dose_mg(strength_text: str) -> float | None:
    """Extract total mg per single unit dose for strengths like 210 mg/1."""
    text = (strength_text or "").strip().lower()
    match = STRENGTH_PER_ONE_UNIT_PATTERN.search(text)
    if not match:
        return None

    den_unit = (match.group("den_unit") or "").strip().lower()
    if den_unit and den_unit in {"ml", "l", "mg", "g", "mcg", "ug", "kg"}:
        return None

    amount = float(match.group("amount"))
    amount_unit = match.group("amount_unit")
    return amount_to_mg(amount, amount_unit)


def format_float(value: float) -> str:
    """Format float without unnecessary trailing zeros."""
    return f"{value:.6f}".rstrip("0").rstrip(".")


def canonical_mab_base_from_ingredient_name(name: str) -> str | None:
    """Extract canonical base mAb name from active ingredient text."""
    text = (name or "").strip().lower()
    if not text:
        return None

    match = MAB_NAME_TOKEN_PATTERN.search(text)
    if not match:
        return None

    token = match.group(0).lower()
    suffix_match = MAB_FDA_SUFFIX_PATTERN.match(token)
    if suffix_match:
        return suffix_match.group("base")
    return token


def write_summary_csv(rows: list[dict[str, str]], summary_csv: str) -> None:
    """Write one row per base mAb with normalized strength summaries."""
    grouped: dict[str, dict[str, set[str] | set[float]]] = defaultdict(
        lambda: {
            "product_ndc": set(),
            "package_ndc": set(),
            "labeler_name": set(),
            "route": set(),
            "dosage_form": set(),
            "active_ingredient_name": set(),
            "active_ingredient_strength": set(),
            "mg_per_ml": set(),
            "single_dose_mg": set(),
        }
    )

    for row in rows:
        base_name = row.get("base_name", "").strip().lower()
        if not base_name:
            continue

        bucket = grouped[base_name]
        for key in (
            "product_ndc",
            "package_ndc",
            "labeler_name",
            "route",
            "dosage_form",
        ):
            val = row.get(key, "").strip()
            if val:
                if key == "route" and "|" in val:
                    for route_part in (part.strip() for part in val.split("|")):
                        if route_part:
                            bucket[key].add(route_part)
                else:
                    bucket[key].add(val)

        ingredient_name = row.get("active_ingredient_name", "").strip()
        ingredient_strength = row.get("active_ingredient_strength", "").strip()
        ingredient_base = canonical_mab_base_from_ingredient_name(ingredient_name)

        # Strict filter: only include strengths/ingredient names matching the queried base mAb.
        if ingredient_base != base_name:
            continue

        if ingredient_name:
            bucket["active_ingredient_name"].add(ingredient_name)
        if ingredient_strength:
            bucket["active_ingredient_strength"].add(ingredient_strength)

        normalized = normalize_strength_to_mg_per_ml(ingredient_strength)
        if normalized is not None:
            bucket["mg_per_ml"].add(round(normalized, 8))

        single_dose_mg = extract_single_dose_mg(ingredient_strength)
        if single_dose_mg is not None:
            bucket["single_dose_mg"].add(round(single_dose_mg, 8))

    fieldnames = [
        "base_name",
        "product_count",
        "package_count",
        "labeler_count",
        "route_values",
        "dosage_form_values",
        "active_ingredient_values",
        "raw_strength_values",
        "normalized_strength_mg_per_ml_values",
        "min_mg_per_ml",
        "max_mg_per_ml",
        "single_dose_mg_values",
        "min_single_dose_mg",
        "max_single_dose_mg",
    ]

    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for base_name in sorted(grouped):
            bucket = grouped[base_name]
            mg_values = sorted(bucket["mg_per_ml"])
            single_dose_values = sorted(bucket["single_dose_mg"])
            writer.writerow(
                {
                    "base_name": base_name,
                    "product_count": len(bucket["product_ndc"]),
                    "package_count": len(bucket["package_ndc"]),
                    "labeler_count": len(bucket["labeler_name"]),
                    "route_values": "|".join(sorted(bucket["route"])),
                    "dosage_form_values": "|".join(sorted(bucket["dosage_form"])),
                    "active_ingredient_values": "|".join(sorted(bucket["active_ingredient_name"])),
                    "raw_strength_values": "|".join(sorted(bucket["active_ingredient_strength"])),
                    "normalized_strength_mg_per_ml_values": "|".join(format_float(v) for v in mg_values),
                    "min_mg_per_ml": format_float(mg_values[0]) if mg_values else "",
                    "max_mg_per_ml": format_float(mg_values[-1]) if mg_values else "",
                    "single_dose_mg_values": "|".join(format_float(v) for v in single_dose_values),
                    "min_single_dose_mg": format_float(single_dose_values[0]) if single_dose_values else "",
                    "max_single_dose_mg": format_float(single_dose_values[-1]) if single_dose_values else "",
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch mAb dosing and packaging details from openFDA NDC")
    parser.add_argument("--input-csv", default="mabs_base.csv", help="input CSV with base_name column")
    parser.add_argument("--output-csv", default="mabs_ndc_details.csv", help="output CSV path")
    parser.add_argument("--summary-csv", default="mabs_ndc_summary.csv", help="per-base summarized output CSV path")
    parser.add_argument("--page-size", type=int, default=100, help="records per API call (default: 100)")
    parser.add_argument(
        "--max-records-per-mab",
        type=int,
        default=400,
        help="maximum NDC product records to scan per base name (default: 400)",
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

        rows = query_ndc_details(
            base_names=base_names,
            page_size=args.page_size,
            max_records_per_mab=args.max_records_per_mab,
            sleep_seconds=args.sleep,
        )
        write_output_csv(rows, args.output_csv)
        write_summary_csv(rows, args.summary_csv)
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
    print(f"Wrote per-base summary to {args.summary_csv}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
