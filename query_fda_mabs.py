#!/usr/bin/env python3
"""Query openFDA for monoclonal antibodies and print a deduplicated list.

Examples:
  /workspaces/FDA-dosing/.venv/bin/python query_fda_mabs.py
  /workspaces/FDA-dosing/.venv/bin/python query_fda_mabs.py --max-records 2000 --json
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
from typing import Iterable

OPENFDA_LABEL_URL = "https://api.fda.gov/drug/label.json"
MAB_TOKEN_PATTERN = re.compile(r"\b[a-z][a-z0-9-]*mab(?:-[a-z0-9]+)?\b", re.IGNORECASE)
BIOLOGIC_SUFFIX_PATTERN = re.compile(r"^(?P<base>[a-z][a-z0-9-]*mab)-[a-z]{4}$", re.IGNORECASE)


def fetch_page(search: str, limit: int, skip: int) -> dict:
    """Fetch one page from openFDA drug labeling endpoint."""
    params = {"search": search, "limit": limit, "skip": skip}
    url = OPENFDA_LABEL_URL + "?" + urllib.parse.urlencode(params)

    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def extract_candidate_names(result: dict) -> Iterable[str]:
    """Extract potential drug names from one labeling record."""
    openfda = result.get("openfda", {})
    for field in ("generic_name", "brand_name", "substance_name"):
        for name in openfda.get(field, []):
            if isinstance(name, str):
                yield name.strip()


def extract_mab_tokens(text: str) -> Iterable[str]:
    """Extract mAb-like tokens from a free-text drug name field."""
    for match in MAB_TOKEN_PATTERN.findall(text):
        token = match.lower()
        suffix_match = BIOLOGIC_SUFFIX_PATTERN.match(token)
        if suffix_match:
            yield suffix_match.group("base")
        else:
            yield token


def query_monoclonal_antibodies(max_records: int = 5000, page_size: int = 100) -> list[str]:
    """Query openFDA and return a sorted list of unique mAb names."""
    search = "openfda.generic_name:*mab OR openfda.substance_name:*mab"
    seen: set[str] = set()

    skip = 0
    fetched = 0
    while fetched < max_records:
        current_limit = min(page_size, max_records - fetched)
        payload = fetch_page(search=search, limit=current_limit, skip=skip)
        results = payload.get("results", [])
        if not results:
            break

        for item in results:
            for name in extract_candidate_names(item):
                for token in extract_mab_tokens(name):
                    seen.add(token)

        batch_count = len(results)
        fetched += batch_count
        skip += batch_count

        if batch_count < current_limit:
            break

        # Keep request pace friendly for openFDA.
        time.sleep(0.15)

    return sorted(seen, key=str.lower)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch a list of monoclonal antibodies from openFDA")
    parser.add_argument("--max-records", type=int, default=5000, help="maximum records to scan (default: 5000)")
    parser.add_argument("--page-size", type=int, default=100, help="records per API call (default: 100)")
    parser.add_argument("--json", action="store_true", help="print JSON array instead of plain text")
    parser.add_argument(
        "--csv",
        nargs="?",
        const="monoclonal_antibodies.csv",
        help="write CSV output to optional path (default: monoclonal_antibodies.csv)",
    )
    return parser.parse_args()


def write_csv(names: list[str], output_path: str) -> None:
    """Write one-column CSV output with monoclonal antibody base names."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["base_name"])
        for name in names:
            writer.writerow([name])


def main() -> int:
    args = parse_args()

    try:
        names = query_monoclonal_antibodies(max_records=args.max_records, page_size=args.page_size)
    except urllib.error.HTTPError as err:
        print(f"HTTP error {err.code}: {err.reason}", file=sys.stderr)
        return 2
    except urllib.error.URLError as err:
        print(f"Network error: {err.reason}", file=sys.stderr)
        return 2
    except TimeoutError:
        print("Request timed out", file=sys.stderr)
        return 2

    if args.csv:
        write_csv(names, args.csv)
        print(f"Wrote CSV: {args.csv}", file=sys.stderr)

    if args.json:
        print(json.dumps(names, indent=2))
    else:
        for name in names:
            print(name)

    print(f"\nTotal monoclonal antibody names: {len(names)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
