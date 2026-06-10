#!/usr/bin/env python3
"""Re-check V3 EphMRA assignments using an indication keyword dictionary.

Purpose:
- Find rows currently classified as V3 (All Other Therapeutic Products).
- Use indication text to optionally reclassify those rows.
- Keep an audit trail of what changed and why.

Input requirements:
- CSV with an EphMRA code column and an indication column.

Behavior:
- Only rows currently in V3 are reviewed.
- Matching supports literal keywords and regex via the `re:` prefix.
- If multiple indication classes match, the default policy keeps V3.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
import re
from typing import Dict, List


# Fill this dictionary with your indication-based backup mapping.
# Key: full EphMRA label to assign.
# Value: list of indication keywords (literal or regex with re: prefix).
indication_keywords: Dict[str, List[str]] = {
    # Example entries (replace with your own):
    # "L4C (Interleukin Inhibitors)": [
    #     "psoriasis",
    #     "rheumatoid arthritis",
    #     "re:ulcerative\\s+colitis",
    # ],
    # "N2C2 (Antimigraine CGRP Antagonists)": [
    #     "migraine",
    # ],
}


def keyword_matches(keyword: str, text_lower: str) -> bool:
    """Match a keyword as literal text or regex using `re:` prefix."""
    if keyword.startswith("re:"):
        pattern = keyword[3:]
        return re.search(pattern, text_lower, flags=re.IGNORECASE) is not None
    return keyword.lower() in text_lower


def find_matches(indication: str, mapping: Dict[str, List[str]]) -> List[str]:
    """Return matching EphMRA codes in dictionary insertion order."""
    if indication is None:
        return []

    indication_text = str(indication).strip()
    if not indication_text or indication_text == "-":
        return []

    text_lower = indication_text.lower()
    matches: List[str] = []

    for ephmra_code, keywords in mapping.items():
        for keyword in keywords:
            if keyword_matches(str(keyword), text_lower):
                matches.append(ephmra_code)
                break

    return matches


def is_v3_code(value: str) -> bool:
    """Treat both Therapeutic and common misspelling variants as V3."""
    if value is None:
        return False
    text = str(value).strip().lower()
    return text.startswith("v3 (all other therapeutic") or text.startswith("v3 (all other theraputic")


def default_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"{input_path.stem}_indication_checked{input_path.suffix}")


def review_v3_rows(
    rows: List[dict[str, str]],
    code_column: str,
    indication_column: str,
    multi_match_policy: str,
) -> List[dict[str, str]]:
    """Apply indication-based review to V3 rows and return updated rows."""
    for row in rows:
        original_code = row.get(code_column, "")
        row["EphMRA Code (Before Indication Check)"] = original_code
        row["Indication Check Matches"] = ""
        row["Indication Check Status"] = "not_v3"

        if not is_v3_code(original_code):
            continue

        matches = find_matches(row.get(indication_column, ""), indication_keywords)
        row["Indication Check Matches"] = " | ".join(matches)

        if not matches:
            row["Indication Check Status"] = "kept_v3_no_indication_match"
            continue

        if len(matches) == 1:
            row[code_column] = matches[0]
            row["Indication Check Status"] = "reclassified_single_match"
            continue

        if multi_match_policy == "first":
            row[code_column] = matches[0]
            row["Indication Check Status"] = "reclassified_multiple_matches_first_used"
        else:
            row["Indication Check Status"] = "kept_v3_multiple_matches"

    return rows


def summarize(rows: List[dict[str, str]]) -> None:
    total = len(rows)
    v3_reviewed = sum(1 for r in rows if is_v3_code(r.get("EphMRA Code (Before Indication Check)", "")))
    changed = sum(
        1
        for r in rows
        if r.get("EphMRA Code", "") != r.get("EphMRA Code (Before Indication Check)", "")
    )

    status_counts: dict[str, int] = {}
    for row in rows:
        status = row.get("Indication Check Status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    print(f"Total rows: {total}")
    print(f"V3 rows reviewed: {v3_reviewed}")
    print(f"Rows reclassified by indication: {changed}")
    print("Status breakdown:")
    for status in sorted(status_counts):
        print(f"  - {status}: {status_counts[status]}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input CSV containing EphMRA Code and indication columns.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path for output CSV (default: <input>_indication_checked.csv).",
    )
    parser.add_argument(
        "--code-column",
        default="EphMRA Code",
        help="Column name holding the current EphMRA classification.",
    )
    parser.add_argument(
        "--indication-column",
        default="Indication",
        help="Column name holding indication text for backup classification.",
    )
    parser.add_argument(
        "--multi-match-policy",
        choices=["keep_v3", "first"],
        default="keep_v3",
        help="How to handle indication rows matching more than one class.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    output_path = Path(args.output) if args.output else default_output_path(input_path)

    with input_path.open("r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    if args.code_column not in fieldnames:
        raise ValueError(f"Missing code column: {args.code_column}")
    if args.indication_column not in fieldnames:
        raise ValueError(f"Missing indication column: {args.indication_column}")

    rows = review_v3_rows(
        rows=rows,
        code_column=args.code_column,
        indication_column=args.indication_column,
        multi_match_policy=args.multi_match_policy,
    )

    audit_columns = [
        "EphMRA Code (Before Indication Check)",
        "Indication Check Matches",
        "Indication Check Status",
    ]
    for col in audit_columns:
        if col not in fieldnames:
            fieldnames.append(col)

    with output_path.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote reviewed CSV: {output_path}")
    summarize(rows)


if __name__ == "__main__":
    main()
