#!/usr/bin/env python3
"""
Computes BACKWARD-looking finding arrivals/closures per file per week --
the contract's B3 columns (section 1.6):

  finding_arrivals_30d / _90d / _180d   findings first observed in the
                                         trailing N-day window before
                                         this week's snapshot
  finding_closures_30d / _90d / _180d   findings that were active at the
                                         start of that window and are no
                                         longer active by this week

This is the mirror image of compute_issue_persistence.py, which looks
FORWARD from week t (t -> t+h) and is a label-only file per the
contract's "Forbidden prediction inputs" list. This script only ever
looks backward from t, so it's safe to use as a B3 feature.

Naming: your snapshots are weekly, so there's no way to get an exact
calendar-day window finer than a week. This script computes at your
existing weekly windows (3w/1m/3m/6m, same convention as
compute_sonar_trend_features.py) AND writes contract-named aliases:
  30d  = 1m (4 weeks, ~28d)
  90d  = 3m (12 weeks, ~84d)
  180d = 6m (26 weeks, ~182d)
The alias columns are literal copies of the nearest weekly window --
documented here so nobody mistakes them for exact-day figures.

Reads data/issues_week_XX.json for every week scanned by
scan_weekly_snapshots.py. Writes one CSV per week to
finding_lifecycle/, one row per file (artifact_id).

Usage:
    python3 compute_finding_lifecycle.py
"""
import json
from pathlib import Path
import csv

DATA_DIR = Path("data")
OUTPUT_DIR = Path("finding_lifecycle")
WINDOWS = {"3w": 3, "1m": 4, "3m": 12, "6m": 26}
DAY_ALIAS = {"1m": "30d", "3m": "90d", "6m": "180d"}
ACTIVE_STATUSES = {"OPEN", "CONFIRMED", "REOPENED"}


def load_active_keys_by_file(label):
    """{file_path: set(active issue keys)} at this week."""
    with open(DATA_DIR / f"issues_{label}.json") as f:
        issues = json.load(f)
    by_file = {}
    for issue in issues:
        if issue["status"] not in ACTIVE_STATUSES:
            continue
        fpath = issue["component"].split(":", 1)[-1]
        by_file.setdefault(fpath, set()).add(issue["key"])
    return by_file


def main():
    week_labels = sorted(
        p.stem.replace("issues_", "") for p in DATA_DIR.glob("issues_week_*.json")
    )
    if not week_labels:
        print("No data/issues_week_*.json files found -- run scan_weekly_snapshots.py first.")
        return

    n_weeks = len(week_labels)
    active_by_week = {label: load_active_keys_by_file(label) for label in week_labels}

    OUTPUT_DIR.mkdir(exist_ok=True)
    for i, label in enumerate(week_labels):
        all_files = set()
        for w in range(max(0, i - max(WINDOWS.values())), i + 1):
            all_files |= set(active_by_week[week_labels[w]].keys())
        all_files |= set(active_by_week[label].keys())

        rows = []
        now_by_file = active_by_week[label]
        for fpath in sorted(all_files):
            now_keys = now_by_file.get(fpath, set())
            row = {"artifact_id": fpath}

            for suffix, w in WINDOWS.items():
                j = i - w
                if j < 0:
                    arrivals = closures = ""
                else:
                    past_keys = active_by_week[week_labels[j]].get(fpath, set())
                    arrivals = len(now_keys - past_keys)  # active now, weren't active w weeks ago
                    closures = len(past_keys - now_keys)  # active then, no longer active now
                row[f"finding_arrivals_{suffix}"] = arrivals
                row[f"finding_closures_{suffix}"] = closures

            for suffix, alias in DAY_ALIAS.items():
                row[f"finding_arrivals_{alias}"] = row[f"finding_arrivals_{suffix}"]
                row[f"finding_closures_{alias}"] = row[f"finding_closures_{suffix}"]

            rows.append(row)

        if rows:
            with open(OUTPUT_DIR / f"{label}.csv", "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)
        print(f"{label}.csv done ({len(rows)} files)")


if __name__ == "__main__":
    main()
