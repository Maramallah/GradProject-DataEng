#!/usr/bin/env python3
"""
Merges each week's raw SonarQube output -- data/measures_week_XX.csv
(from pull_metrics.py) and data/issues_week_XX.json (from pull_issues.py)
-- into ONE per-file, per-week CSV: snapshot_features/week_XX.csv.

This is the same "metrics + issues -> one row per file" logic
compute_features.py already uses for T0 (ncloc, complexity, finding
counts, severity/type breakdown, density), just generalized to run for
EVERY scanned week instead of only the latest one. compute_features.py
still owns the actual T0 feature table (it also adds the historical
slope/volatility columns for the single T0 you pick); this script's job
is to make sure every week -- any of which might end up chosen as a T0
or checked as a T90 later -- has its metrics and issues sitting in one
file instead of two.

Columns written per file (artifact_id):
  ncloc, complexity, cognitive_complexity,
  duplicated_lines_density, comment_lines_density
  finding_count, finding_density                 (per 1000 ncloc)
  technical_debt_minutes, technical_debt_density  (per 1000 ncloc)
  blocker_finding_count, critical_finding_count, major_finding_count,
  minor_finding_count, info_finding_count
  code_smell_count, bug_count, vulnerability_count

Only ACTIVE issues (OPEN/CONFIRMED/REOPENED) are counted -- "findings
at week t" means currently-open findings at that snapshot, matching
compute_features.py's and compute_sonar_trend_features.py's convention.
RESOLVED/CLOSED issues still exist in issues_week_XX.json for the
scripts that need them (compute_finding_lifecycle.py,
compute_issue_persistence.py, compute_label.py) -- this script just
doesn't count them here.

Reads data/measures_week_XX.csv + data/issues_week_XX.json for every
week_XX scanned by scan_weekly_snapshots.py. Writes one CSV per week to
snapshot_features/, one row per file (artifact_id) -- same layout
convention as sonar_trends/, ownership_entropy/, etc.

Usage:
    python3 merge_measures_issues.py
"""
import csv
import json
from pathlib import Path

DATA_DIR = Path("data")
OUTPUT_DIR = Path("snapshot_features")

ACTIVE_STATUSES = {"OPEN", "CONFIRMED", "REOPENED"}

SEVERITY_COLS = {
    "BLOCKER": "blocker_finding_count",
    "CRITICAL": "critical_finding_count",
    "MAJOR": "major_finding_count",
    "MINOR": "minor_finding_count",
    "INFO": "info_finding_count",
}
TYPE_COLS = {
    "CODE_SMELL": "code_smell_count",
    "BUG": "bug_count",
    "VULNERABILITY": "vulnerability_count",
}


def parse_minutes(effort):
    """SonarQube returns effort/debt like '5min' or '1h30min'."""
    if not effort:
        return 0
    total, num = 0, ""
    for ch in effort:
        if ch.isdigit():
            num += ch
        elif ch == "h":
            total += int(num or 0) * 60
            num = ""
        elif ch == "m":
            total += int(num or 0)
            num = ""
    return total


def load_measures(label):
    with open(DATA_DIR / f"measures_{label}.csv") as f:
        return {r["file_path"]: r for r in csv.DictReader(f)}


def load_active_issues(label):
    with open(DATA_DIR / f"issues_{label}.json") as f:
        issues = json.load(f)
    return [i for i in issues if i["status"] in ACTIVE_STATUSES]


def merge_one_week(label):
    measures = load_measures(label)
    issues = load_active_issues(label)

    finding_count, debt_minutes = {}, {}
    severity_by_file, type_by_file = {}, {}
    for issue in issues:
        fpath = issue["component"].split(":", 1)[-1]
        finding_count[fpath] = finding_count.get(fpath, 0) + 1
        debt_minutes[fpath] = debt_minutes.get(fpath, 0) + parse_minutes(issue["effort_minutes"])

        sev = issue.get("severity")
        if sev in SEVERITY_COLS:
            severity_by_file.setdefault(fpath, {})
            severity_by_file[fpath][sev] = severity_by_file[fpath].get(sev, 0) + 1

        typ = issue.get("type")
        if typ in TYPE_COLS:
            type_by_file.setdefault(fpath, {})
            type_by_file[fpath][typ] = type_by_file[fpath].get(typ, 0) + 1

    rows = []
    for fpath, m in measures.items():
        ncloc = int(m.get("ncloc") or 0)
        fcount = finding_count.get(fpath, 0)
        debt = debt_minutes.get(fpath, 0)

        row = {
            "artifact_id": fpath,
            "ncloc": ncloc,
            "complexity": m.get("complexity", ""),
            "cognitive_complexity": m.get("cognitive_complexity", ""),
            "duplicated_lines_density": m.get("duplicated_lines_density", ""),
            "comment_lines_density": m.get("comment_lines_density", ""),
            "finding_count": fcount,
            "finding_density": round(1000 * fcount / ncloc, 4) if ncloc else "",
            "technical_debt_minutes": debt,
            "technical_debt_density": round(1000 * debt / ncloc, 4) if ncloc else "",
        }

        file_sev = severity_by_file.get(fpath, {})
        for sev, col in SEVERITY_COLS.items():
            row[col] = file_sev.get(sev, 0)

        file_typ = type_by_file.get(fpath, {})
        for typ, col in TYPE_COLS.items():
            row[col] = file_typ.get(typ, 0)

        rows.append(row)

    return rows


def main():
    week_labels = sorted(
        p.stem.replace("measures_", "") for p in DATA_DIR.glob("measures_week_*.csv")
    )
    if not week_labels:
        print("No data/measures_week_*.csv files found -- run scan_weekly_snapshots.py first.")
        return

    OUTPUT_DIR.mkdir(exist_ok=True)

    for label in week_labels:
        rows = merge_one_week(label)
        if not rows:
            print(f"{label}: no files found, skipping.")
            continue
        with open(OUTPUT_DIR / f"{label}.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"{label}.csv done ({len(rows)} files)")


if __name__ == "__main__":
    main()
