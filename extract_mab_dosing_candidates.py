#!/usr/bin/env python3
"""Convert variable FDA label prose into structured mAb dosing candidates.

Input:
- CSV from query_fda_mab_label_sections.py (default: mabs_label_sections.csv)

Outputs:
- Candidate-level rows (one row per dose mention)
- Per-mAb summary with review flags

This is intentionally conservative: it extracts explicit numeric dosing mentions and
marks ambiguous cases for review instead of guessing.
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict

DOSE_PATTERN = re.compile(
    r"(?P<value>\d*\.?\d+)\s*(?P<unit>mg|mcg|ug|g)(?:\s*/\s*(?P<per>kg|m2))?",
    re.IGNORECASE,
)

FREQ_PATTERNS = [
    re.compile(r"every\s+\d+\s+(?:day|days|week|weeks|month|months)", re.IGNORECASE),
    re.compile(r"every\s+other\s+week", re.IGNORECASE),
    re.compile(r"once\s+(?:daily|weekly|monthly)", re.IGNORECASE),
    re.compile(r"twice\s+(?:daily|weekly|monthly)", re.IGNORECASE),
    re.compile(r"q\d+\s*(?:d|w|m)", re.IGNORECASE),
    re.compile(r"(?:daily|weekly|monthly)", re.IGNORECASE),
]

ROUTE_PATTERNS = {
    "intravenous": re.compile(r"\bintravenous\b|\biv\b", re.IGNORECASE),
    "subcutaneous": re.compile(r"\bsubcutaneous\b|\bsc\b", re.IGNORECASE),
    "intramuscular": re.compile(r"\bintramuscular\b|\bim\b", re.IGNORECASE),
    "oral": re.compile(r"\boral\b", re.IGNORECASE),
}
COMMON_INDICATIONS = [
    "Rheumatoid Arthritis",
    "Psoriasis",
    "Psoriatic Arthritis",
    "Ankylosing Spondylitis",
    "Ulcerative Colitis",
    "Crohn's Disease",
    "Juvenile Idiopathic Arthritis",
    "Cancer",
    "Melanoma",
    "Non-small Cell Lung Cancer",
    "Colorectal Cancer",
    "Asthma",
    "COPD",
    "Multiple Sclerosis",
    "Migraine",
    "Atopic Dermatitis",
]

PATIENT_POP_PATTERNS = {
    "adult": re.compile(r"\badult\b|\badults\b", re.IGNORECASE),
    "pediatric": re.compile(r"\bpediatric\b|\bchildren\b|\bchild\b", re.IGNORECASE),
    "juvenile": re.compile(r"\bjuvenile\b", re.IGNORECASE),
    "geriatric": re.compile(r"\bgeriatric\b|\belderly\b", re.IGNORECASE),
}
SENTENCE_SPLIT = re.compile(r"(?<=[\.;])\s+|\n+")
WHITESPACE_PATTERN = re.compile(r"\s+")


def read_rows(path: str) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows


def normalize_mass_to_mg(value: float, unit: str) -> float:
    unit = unit.lower()
    if unit == "g":
        return value * 1000.0
    if unit in {"mcg", "ug"}:
        return value * 0.001
    return value


def find_frequency(text: str) -> str:
    found: list[str] = []
    for pattern in FREQ_PATTERNS:
        found.extend(match.group(0).strip() for match in pattern.finditer(text))
    unique = sorted(set(found), key=str.lower)
    return "|".join(unique)


def find_routes(text: str) -> str:
    routes = [name for name, pattern in ROUTE_PATTERNS.items() if pattern.search(text)]
    return "|".join(sorted(routes))


def normalize_frequency(freq_text: str) -> str:
    """Normalize frequency phrases to canonical forms like q2w, q4w, daily, bd."""
    if not freq_text:
        return ""
    text = freq_text.lower()
    # Map patterns to canonical forms
    replacements = [
        (r"every\s+other\s+week", "q2w"),
        (r"every\s+1\s+week", "q1w"),
        (r"every\s+2\s+week", "q2w"),
        (r"every\s+3\s+week", "q3w"),
        (r"every\s+4\s+week", "q4w"),
        (r"every\s+(\d+)\s+week", r"q\1w"),
        (r"every\s+1\s+day", "daily"),
        (r"every\s+2\s+day", "q2d"),
        (r"every\s+(\d+)\s+day", r"q\1d"),
        (r"once\s+daily", "daily"),
        (r"twice\s+daily", "bd"),
        (r"three\s+times\s+daily", "tid"),
        (r"once\s+weekly", "q1w"),
        (r"twice\s+weekly", "bw"),
        (r"once\s+monthly", "q1m"),
    ]
    result = text
    for pattern, replacement in replacements:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    return result.strip()


def extract_label_indications(indications_and_usage: str) -> list[str]:
    """Extract all indications mentioned in the indications_and_usage section."""
    text = (indications_and_usage or "").lower()
    found = [ind for ind in COMMON_INDICATIONS if ind.lower() in text]
    return sorted(set(found))


def find_indications(text: str, label_indications: list[str] | None = None) -> str:
    """Extract mentioned indications from context, with label-level fallback."""
    text_lower = text.lower()
    found = [ind for ind in COMMON_INDICATIONS if ind.lower() in text_lower]
    if not found and label_indications:
        found = label_indications
    return "|".join(sorted(set(found)))


def find_patient_population(text: str) -> str:
    """Detect patient population from context (adult, pediatric, juvenile, etc.)."""
    pops = [name for name, pattern in PATIENT_POP_PATTERNS.items() if pattern.search(text)]
    if not pops:
        return "unknown"
    return "|".join(sorted(set(pops)))


def normalize_text(text: str) -> str:
    """Normalize whitespace for stable matching and deduping."""
    return WHITESPACE_PATTERN.sub(" ", text or "").strip()


def split_text_chunks(section_text: str) -> list[str]:
    """Split verbose label text into smaller chunks to reduce extraction noise."""
    text = (section_text or "").replace("•", "\n")
    chunks: list[str] = []
    for block in SENTENCE_SPLIT.split(text):
        block = normalize_text(block)
        if not block:
            continue

        # Extra split for dense table-like strings that often create repeated hits.
        for piece in re.split(r"\s{2,}|\s(?=\d+\s*(?:mg|mcg|ug|g)\b)", block):
            piece = normalize_text(piece)
            if piece:
                chunks.append(piece)
    return chunks


def candidate_signature(row: dict[str, str]) -> tuple[str, ...]:
    """Stable signature for deduping candidates from repetitive label prose."""
    return (
        row["base_name"],
        row["set_id"],
        row["id"],
        row["section"],
        row["dose_mg"],
        row["dose_per"],
        row["frequency_canonical"],
        row["route_mentions"],
        row["phase_mentions"],
        row["indication"],
        row["patient_population"],
        normalize_text(row["context_sentence"]),
    )


def build_candidates(label_row: dict[str, str]) -> list[dict[str, str]]:
    base_name = label_row.get("base_name", "").strip().lower()
    set_id = label_row.get("set_id", "")
    record_id = label_row.get("id", "")

    # Extract label-level indications from indications_and_usage section
    indications_section = label_row.get("indications_and_usage", "") or ""
    label_indications = extract_label_indications(indications_section)

    sections = [
        ("dosage_and_administration", label_row.get("dosage_and_administration", "") or ""),
        ("dosage_forms_and_strengths", label_row.get("dosage_forms_and_strengths", "") or ""),
    ]

    candidates: list[dict[str, str]] = []
    seen: set[tuple[str, ...]] = set()

    for section_name, section_text in sections:
        if not section_text.strip():
            continue

        for sentence in split_text_chunks(section_text):
            if not sentence:
                continue

            freq = find_frequency(sentence)
            freq_canonical = normalize_frequency(freq)
            routes = find_routes(sentence)
            indications = find_indications(sentence, label_indications)
            patient_pop = find_patient_population(sentence)
            lowered = sentence.lower()
            phase = ""
            if any(x in lowered for x in ("loading", "initial", "week 0")):
                phase = "loading"
            if any(x in lowered for x in ("maintenance", "thereafter", "subsequent")):
                phase = "maintenance" if not phase else f"{phase}|maintenance"

            for match in DOSE_PATTERN.finditer(sentence):
                value = float(match.group("value"))
                unit = match.group("unit").lower()
                per = (match.group("per") or "").lower()
                dose_mg = normalize_mass_to_mg(value, unit)

                row = {
                    "base_name": base_name,
                    "set_id": set_id,
                    "id": record_id,
                    "section": section_name,
                    "dose_value_raw": str(value),
                    "dose_unit_raw": unit,
                    "dose_per": per,
                    "dose_mg": f"{dose_mg:.6f}".rstrip("0").rstrip("."),
                    "is_weight_or_bsa_based": "true" if per in {"kg", "m2"} else "false",
                    "frequency_mentions": freq,
                    "frequency_canonical": freq_canonical,
                    "route_mentions": routes,
                    "phase_mentions": phase,
                    "indication": indications,
                    "patient_population": patient_pop,
                    "context_sentence": sentence,
                }
                sig = candidate_signature(row)
                if sig in seen:
                    continue
                seen.add(sig)
                candidates.append(row)

    return candidates


def split_by_dimensions(candidates: list[dict[str, str]]) -> list[dict[str, str]]:
    """Split candidates by indication and patient population so each combo gets its own row."""
    split_rows: list[dict[str, str]] = []
    for row in candidates:
        indications = [x.strip() for x in row["indication"].split("|") if x.strip()]
        pops = [x.strip() for x in row["patient_population"].split("|") if x.strip()]
        if not indications:
            indications = [""]
        if not pops:
            pops = [""]
        for ind in indications:
            for pop in pops:
                new_row = row.copy()
                new_row["indication"] = ind
                new_row["patient_population"] = pop
                split_rows.append(new_row)
    return split_rows


def write_csv(path: str, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def summarize(candidates: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in candidates:
        grouped[row["base_name"]].append(row)

    summary: list[dict[str, str]] = []
    for base_name in sorted(grouped):
        rows = grouped[base_name]
        mg_fixed = sorted({r["dose_mg"] for r in rows if r["is_weight_or_bsa_based"] == "false"})
        mg_per_kg = sorted({r["dose_mg"] for r in rows if r["dose_per"] == "kg"})
        mg_per_m2 = sorted({r["dose_mg"] for r in rows if r["dose_per"] == "m2"})

        freqs = Counter()
        routes = Counter()
        phases = Counter()
        for r in rows:
            for f in (x for x in r["frequency_mentions"].split("|") if x):
                freqs[f] += 1
            for rt in (x for x in r["route_mentions"].split("|") if x):
                routes[rt] += 1
            for ph in (x for x in r["phase_mentions"].split("|") if x):
                phases[ph] += 1

        top_freqs = "|".join([k for k, _ in freqs.most_common(5)])
        top_routes = "|".join([k for k, _ in routes.most_common(3)])
        top_phases = "|".join([k for k, _ in phases.most_common(3)])

        regimen_counter: Counter[str] = Counter()
        for r in rows:
            freq_canonical = (r.get("frequency_canonical") or "").strip()
            if not freq_canonical:
                continue

            phase_values = {x for x in r["phase_mentions"].split("|") if x}
            if "loading" in phase_values and "maintenance" not in phase_values:
                continue

            dose_label = f"{r['dose_mg']} mg"
            if r["dose_per"]:
                dose_label += f"/{r['dose_per']}"

            route = next((x for x in r["route_mentions"].split("|") if x), "")
            regimen = f"{dose_label}; {freq_canonical}"
            if route:
                regimen += f"; {route}"
            regimen_counter[regimen] += 1

        maintenance_candidates = "|".join([k for k, _ in regimen_counter.most_common(5)])
        intended_maintenance = regimen_counter.most_common(1)[0][0] if regimen_counter else ""

        needs_review = (
            len(mg_fixed) > 4
            or (not mg_fixed and not mg_per_kg and not mg_per_m2)
            or (not top_freqs)
            or ("loading" in top_phases and "maintenance" not in top_phases)
        )

        summary.append(
            {
                "base_name": base_name,
                "candidate_count": str(len(rows)),
                "fixed_dose_mg_values": "|".join(mg_fixed),
                "dose_mg_per_kg_values": "|".join(mg_per_kg),
                "dose_mg_per_m2_values": "|".join(mg_per_m2),
                "top_frequency_mentions": top_freqs,
                "top_route_mentions": top_routes,
                "top_phase_mentions": top_phases,
                "maintenance_regimen_candidates": maintenance_candidates,
                "intended_maintenance_regimen": intended_maintenance,
                "needs_manual_review": "true" if needs_review else "false",
            }
        )

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract structured dosing candidates from FDA mAb label sections")
    parser.add_argument("--input-csv", default="mabs_label_sections.csv", help="label-sections CSV input")
    parser.add_argument("--candidates-csv", default="mabs_dosing_candidates.csv", help="candidate-level output CSV")
    parser.add_argument("--summary-csv", default="mabs_dosing_summary.csv", help="per-mAb summary output CSV")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    rows = read_rows(args.input_csv)
    all_candidates: list[dict[str, str]] = []
    for row in rows:
        all_candidates.extend(build_candidates(row))

    split_candidates = split_by_dimensions(all_candidates)
    candidate_fields = [
        "base_name",
        "set_id",
        "id",
        "section",
        "dose_value_raw",
        "dose_unit_raw",
        "dose_per",
        "dose_mg",
        "is_weight_or_bsa_based",
        "frequency_mentions",
        "frequency_canonical",
        "route_mentions",
        "phase_mentions",
        "indication",
        "patient_population",
        "context_sentence",
    ]
    write_csv(args.candidates_csv, split_candidates, candidate_fields)

    summary_rows = summarize(all_candidates)
    summary_fields = [
        "base_name",
        "candidate_count",
        "fixed_dose_mg_values",
        "dose_mg_per_kg_values",
        "dose_mg_per_m2_values",
        "top_frequency_mentions",
        "top_route_mentions",
        "top_phase_mentions",
        "maintenance_regimen_candidates",
        "intended_maintenance_regimen",
        "needs_manual_review",
    ]
    write_csv(args.summary_csv, summary_rows, summary_fields)

    print(f"Wrote {len(all_candidates)} candidate rows to {args.candidates_csv}")
    print(f"Wrote {len(summary_rows)} summary rows to {args.summary_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
