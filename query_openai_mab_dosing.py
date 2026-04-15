#!/usr/bin/env python3
"""Send FDA mAb label-section CSV groups to OpenAI, one base_name at a time.

Input:
- mabs_label_sections.csv (or compatible CSV)

Behavior:
- Groups rows by base_name
- Sends exactly one base_name group per OpenAI API request
- Appends the model's strict CSV-like lines to an output file

Environment:
- OPENAI_API_KEY must be set (or pass --api-key)

Example:
  /workspaces/FDA-dosing/.venv/bin/python query_openai_mab_dosing.py \
    --input-csv mabs_label_sections.csv \
    --output-csv mabs_dosing_from_openai.csv \
    --max-groups 1
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
from datetime import datetime, timezone
import sys
import time
import urllib.error
import urllib.request
from collections import OrderedDict

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

PROMPT_TEXT = """You are analysing drug labelling data in CSV format. Each row has at least these columns:
base_name, generic_name, brand_name, indications_and_usage, dosage_and_administration, dosage_forms_and_strengths

Multiple rows may share the same base_name but differ in brand_name, manufacturer_name, etc.

Your task is, for each drug base_name, to determine the typical adult dosing per indication and summarise it in a strict CSV-like format, with no explanations.
Follow these rules carefully:
1. Group by base_name
    * Treat all rows with the same base_name as one drug base_name group.
    * Within each group, the drug may be referred to by different brand_name values (e.g., "Humira", "Idacio", "Yuflyma" for adalimumab).
    * Consider all mentions of the drug under its base_name and any of its brand_name values when interpreting indications and doses.
2. Adult indications only
    * Use only dosing information clearly intended for adult patients.
    * Ignore pediatric-only regimens (weight-based pediatric dosing, age-limited pediatric dosing).
    * If a section mixes adult and pediatric dosing, extract only the adult regimen.
3. Median dose per indication (mg/dose)
    * For each adult indication of the drug, identify the typical maintenance dose per administration for an average adult patient.
    * Ignore induction or loading doses (e.g., high initial doses on Day 1, Day 15).
        * If both induction and maintenance regimens are described, collapse to the maintenance regimen only (e.g., for Crohn's: use the "40 mg every other week starting on Day 29" maintenance dose, not the 160/80 mg induction steps).
    * If multiple maintenance regimens are specified for the same adult indication (e.g., "40 mg every week OR 80 mg every other week"), treat them as separate regimens only if they are clearly distinct long-term options; otherwise choose the regimen that is presented as the standard or default.
    * Convert the dose to mg per administration (mg/dose):
        * If dosing is in mg/kg, assume an average adult weight of 70 kg.
        * If dosing is in mg/m^2 or mg/m2, assume an average adult body surface area of 1.7 m^2.
    * If the text provides a single fixed adult maintenance dose (e.g., "Adults: 40 mg every other week"), that dose is the value of mg/dose.
4. Dosing period
    * Identify the maintenance dosing period for each adult indication and regimen, such as:
        * once daily
        * twice daily
        * weekly
        * every other week
        * monthly
        * or a similar phrase.
    * Do not describe induction schedules; only capture the ongoing maintenance interval.
5. Designated daily dose (mg/day)
    * For each adult indication and maintenance regimen, calculate the designated daily dose in mg/day for a single patient, averaged over one year, based only on the maintenance regimen.
    * Steps:
        * From the dosing period, determine the number of maintenance doses per year:
            * Once daily ~= 365 doses/year
            * Every other week ~= 26 doses/year
            * Weekly ~= 52 doses/year
            * Twice daily ~= 730 doses/year
            * Monthly ~= 12 doses/year
            * Etc.
        * Compute:
total_annual_maintenance_dose_mg = mg_per_dose * number_of_maintenance_doses_per_year
        * Then:
designated_daily_dose_mg_per_day = total_annual_maintenance_dose_mg / 365
    * Example: if the adult maintenance dose is 40 mg every other week, then there are about 26 doses per year, so:
designated daily dose = (40 mg * 26) / 365.
6. Drug strengths (drug_strengths_mg)
    * Using dosage_forms_and_strengths, extract all distinct strengths for this base_name in mg, ignoring:
        * the mL volume, and
        * the form type (auto injector,pen, syringe, vial, etc.).
    * For example, from strings like:
        * 40 mg/0.8 mL pen
        * 40 mg/0.4 mL pen
        * 80 mg/0.8 mL syringe
        * 20 mg/0.2 mL syringe
        * 10 mg/0.1 mL syringe you should extract the mg/mL and form type: 40/0.8 pen, 80/0.8 syringe, 20/0.2 syringe, 10/0.1 syringe.
    * Collect all unique mg strengths for the entire base_name group (across all brand_names and rows with that base_name).
    * Output them as a single field with values separated by |, sorted or unsorted, for example:
10/0.1 syringe|20/0.2 syringe|40/0.4 pen|40/0.8 pen|80/0.8 syringe
    * This drug_strengths_mg field is shared across all indications for that base_name (you do not need to tie specific strengths to specific indications).
7. Use only the drug of interest
    * Only extract and use dosing information that clearly corresponds to this drug's base_name (and its associated brand_names).
    * Ignore dosing for other drugs that are:
        * co-administered,
        * pre-treatments,
        * concomitant medications,
        * or comparators in clinical studies.
8. Indication definition and deduplication
    * The indication is the clinical condition or use case (e.g., "rheumatoid arthritis", "psoriatic arthritis", "ankylosing spondylitis", "Crohn's disease", "ulcerative colitis", "plaque psoriasis", "hidradenitis suppurativa", "uveitis").
    * Normalize minor wording differences for the same condition into a single indication label, if they clearly refer to the same adult disease state.
    * For each (base_name, indication, maintenance regimen) combination, you should output one line.
9. Numeric handling and rounding
    * Perform all unit conversions exactly.
    * For output:
        * You may round numeric doses to a sensible precision (e.g., 2 decimal places) and drop trailing zeros where unnecessary (e.g., output 40 instead of 40.00).
10. Output format (strict)
* Do not include any headers.
* Do not include explanations, comments, or extra text.
* Output one line per adult indication and maintenance regimen in this exact comma-separated format:
base_name, indication, mg/dose, dosing period, designated daily dose, drug per dosage form
* Example (illustrative only):
adalimumab, rheumatoid arthritis, 40, every other week, 2.85, 10/0.1 syringe|20/0.2 syringe|40/0.4 pen|40/0.8 pen|80/0.8 syringe
11. Safety and ambiguity rule
* If you cannot confidently determine a required field (e.g., the adult maintenance dose or dosing period) from the text, skip that indication rather than guessing.
Important behavior:
* Analyze each base_name group independently (all rows with the same base_name).
* From indications_and_usage and dosage_and_administration, identify adult indications and adult maintenance regimens.
* From dosage_forms_and_strengths, construct the drug per dosage form field for the whole base_name.
* Then produce only the list of CSV lines in the required format, with no additional text."""


class RateLimitError(RuntimeError):
    """Raised when OpenAI returns HTTP 429 after retries."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send grouped mAb label sections to OpenAI")
    parser.add_argument("--input-csv", default="mabs_label_sections.csv", help="input CSV path")
    parser.add_argument("--output-csv", default="mabs_dosing_from_openai.csv", help="output CSV-like text file")
    parser.add_argument("--model", default="gpt-5.4-mini", help="OpenAI model name")
    parser.add_argument("--api-key", default="", help="OpenAI API key (overrides OPENAI_API_KEY)")
    parser.add_argument("--max-groups", type=int, default=1, help="max base_name groups to process this run")
    parser.add_argument("--base-name", default="", help="process only one specific base_name")
    parser.add_argument(
        "--start-after-base-name",
        default="",
        help="start processing after this base_name (alphabetical order)",
    )
    parser.add_argument("--overwrite", action="store_true", help="overwrite output file instead of appending")
    parser.add_argument("--timeout", type=int, default=120, help="per-request timeout seconds")
    parser.add_argument("--max-retries", type=int, default=4, help="max retries for API errors")
    parser.add_argument("--sleep", type=float, default=0.4, help="sleep seconds between groups")
    parser.add_argument("--dry-run", action="store_true", help="show selected groups without API calls")
    parser.add_argument(
        "--resume-file",
        default="mabs_openai_resume.json",
        help="resume tracker file path",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="disable automatic resume behavior for this run",
    )
    parser.add_argument(
        "--reset-resume",
        action="store_true",
        help="delete resume tracker file before processing",
    )
    parser.add_argument(
        "--max-consecutive-rate-limits",
        type=int,
        default=2,
        help="stop run after this many consecutive 429 rate-limit failures",
    )
    return parser.parse_args()


def load_resume_state(path: str) -> dict[str, str]:
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in data.items() if isinstance(v, (str, int, float, bool))}


def save_resume_state(path: str, base_name: str, output_csv: str) -> None:
    state = {
        "last_completed_base_name": base_name,
        "output_csv": output_csv,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=True, indent=2)


def retry_after_seconds(err: urllib.error.HTTPError, fallback_seconds: float) -> float:
    header = err.headers.get("Retry-After")
    if not header:
        return fallback_seconds
    try:
        value = float(header)
        if value > 0:
            return value
    except (TypeError, ValueError):
        pass
    return fallback_seconds


def read_rows(input_csv: str) -> list[dict[str, str]]:
    with open(input_csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return rows

    required_columns = {
        "base_name",
        "generic_name",
        "brand_name",
        "indications_and_usage",
        "dosage_and_administration",
        "dosage_forms_and_strengths",
    }
    missing = sorted(required_columns - set(rows[0].keys()))
    if missing:
        raise ValueError(f"Input CSV is missing required columns: {', '.join(missing)}")

    return rows


def group_rows_by_base_name(rows: list[dict[str, str]]) -> OrderedDict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        base_name = (row.get("base_name") or "").strip().lower()
        if not base_name:
            continue
        grouped.setdefault(base_name, []).append(row)
    return OrderedDict((k, grouped[k]) for k in sorted(grouped))


def select_groups(
    grouped: OrderedDict[str, list[dict[str, str]]],
    base_name: str,
    start_after_base_name: str,
    max_groups: int,
) -> list[tuple[str, list[dict[str, str]]]]:
    items = list(grouped.items())

    if base_name:
        key = base_name.strip().lower()
        if key not in grouped:
            raise ValueError(f"base_name not found: {base_name}")
        return [(key, grouped[key])]

    if start_after_base_name:
        cursor = start_after_base_name.strip().lower()
        items = [(k, v) for k, v in items if k > cursor]

    if max_groups > 0:
        items = items[:max_groups]

    return items


def rows_to_group_csv(rows: list[dict[str, str]]) -> str:
    if not rows:
        return ""
    fieldnames = list(rows[0].keys())
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


def extract_output_text(payload: dict) -> str:
    direct = payload.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct

    parts: list[str] = []
    for item in payload.get("output", []):
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []):
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text)
    return "\n".join(parts).strip()


def call_openai(
    api_key: str,
    model: str,
    prompt_text: str,
    timeout_seconds: int,
    max_retries: int,
) -> str:
    body = {
        "model": model,
        "input": prompt_text,
    }
    data = json.dumps(body).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    for attempt in range(1, max_retries + 1):
        req = urllib.request.Request(OPENAI_RESPONSES_URL, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
                text = extract_output_text(payload)
                if text.strip():
                    return text
                raise RuntimeError("OpenAI response had no text output")
        except urllib.error.HTTPError as err:
            retriable = err.code in {408, 409, 429, 500, 502, 503, 504}
            if retriable and attempt < max_retries:
                sleep_for = min(15.0, 1.2 * attempt)
                if err.code == 429:
                    sleep_for = min(30.0, retry_after_seconds(err, sleep_for))
                time.sleep(sleep_for)
                continue
            detail = err.read().decode("utf-8", errors="replace")
            if err.code == 429:
                raise RateLimitError(f"OpenAI HTTP error {err.code}: {detail}") from err
            raise RuntimeError(f"OpenAI HTTP error {err.code}: {detail}") from err
        except urllib.error.URLError as err:
            if attempt < max_retries:
                time.sleep(min(8.0, 0.8 * attempt))
                continue
            raise RuntimeError(f"Network error calling OpenAI: {err.reason}") from err

    raise RuntimeError("OpenAI call failed after retries")


def normalize_model_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("```"):
            continue
        if line.lower().startswith("base_name,"):
            continue
        lines.append(line)

    # Keep only likely CSV rows with at least 5 commas.
    return [line for line in lines if line.count(",") >= 5]


def main() -> int:
    args = parse_args()

    api_key = args.api_key.strip() or os.environ.get("OPENAI_API_KEY", "").strip()
    if not args.dry_run and not api_key:
        print("Missing API key. Set OPENAI_API_KEY or pass --api-key.", file=sys.stderr)
        return 2

    if args.reset_resume and os.path.exists(args.resume_file):
        os.remove(args.resume_file)
        print(f"Reset resume tracker: {args.resume_file}", file=sys.stderr)

    resume_start_after = ""
    resume_enabled = not args.no_resume and not args.base_name and not args.start_after_base_name
    if resume_enabled:
        resume_state = load_resume_state(args.resume_file)
        resume_start_after = (resume_state.get("last_completed_base_name") or "").strip().lower()
        if resume_start_after:
            print(f"Resuming after base_name={resume_start_after}", file=sys.stderr)

    try:
        rows = read_rows(args.input_csv)
        grouped = group_rows_by_base_name(rows)
        selected = select_groups(
            grouped=grouped,
            base_name=args.base_name,
            start_after_base_name=args.start_after_base_name or resume_start_after,
            max_groups=args.max_groups,
        )
    except FileNotFoundError as err:
        print(f"File not found: {err}", file=sys.stderr)
        return 2
    except ValueError as err:
        print(str(err), file=sys.stderr)
        return 2

    if not selected:
        print("No base_name groups selected.", file=sys.stderr)
        return 0

    mode = "w" if args.overwrite else "a"
    if args.overwrite:
        with open(args.output_csv, "w", encoding="utf-8"):
            pass

    total_lines_written = 0
    consecutive_rate_limits = 0
    for index, (base_name, group_rows) in enumerate(selected, start=1):
        print(
            f"[{index}/{len(selected)}] Processing base_name={base_name} rows={len(group_rows)}",
            file=sys.stderr,
        )

        if args.dry_run:
            continue

        group_csv = rows_to_group_csv(group_rows)
        request_prompt = (
            f"{PROMPT_TEXT}\n\n"
            f"Drug base_name group to analyze: {base_name}\n"
            "Input CSV rows (single base_name group):\n"
            f"{group_csv}"
        )

        try:
            model_text = call_openai(
                api_key=api_key,
                model=args.model,
                prompt_text=request_prompt,
                timeout_seconds=args.timeout,
                max_retries=args.max_retries,
            )
        except RateLimitError as err:
            consecutive_rate_limits += 1
            print(
                (
                    f"Rate limit for {base_name}: {err} "
                    f"(consecutive={consecutive_rate_limits}/{args.max_consecutive_rate_limits})"
                ),
                file=sys.stderr,
            )
            if consecutive_rate_limits >= args.max_consecutive_rate_limits:
                print(
                    (
                        "Stopping early due to repeated 429 rate limits. "
                        "Resume file has been preserved so you can continue later."
                    ),
                    file=sys.stderr,
                )
                break
            continue
        except RuntimeError as err:
            print(f"OpenAI call failed for {base_name}: {err}", file=sys.stderr)
            continue

        lines = normalize_model_lines(model_text)
        if not lines:
            print(f"No usable CSV lines returned for {base_name}", file=sys.stderr)
            continue

        with open(args.output_csv, mode, encoding="utf-8") as f:
            f.write("\n".join(lines))
            f.write("\n")
        mode = "a"

        total_lines_written += len(lines)
        consecutive_rate_limits = 0
        print(f"Wrote {len(lines)} lines for {base_name}", file=sys.stderr)
        if not args.no_resume:
            save_resume_state(args.resume_file, base_name=base_name, output_csv=args.output_csv)

        if args.sleep > 0:
            time.sleep(args.sleep)

    print(f"Done. Total lines written: {total_lines_written}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
