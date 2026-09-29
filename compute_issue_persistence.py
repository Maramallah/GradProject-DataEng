#!/usr/bin/env python3
"""
For each week and each look-ahead horizon (3 weeks, 1 month/4 weeks,
3 months/12 weeks, 180 days/26 weeks), checks whether each issue open at
that week is STILL open at week+horizon -- matched by SonarQube's issue
`key`, which persists across analyses of the same project (the same
identity assumption compute_label.py already relies on for the T0->T90
label).

Only computable where week+horizon falls within your scanned range. With
26 weeks total, the 180d/26-week horizon has NO valid comparison point at
all (it would need a week 27+ that doesn't exist) -- that column will be
entirely blank unless you scan beyond 26 weeks. The other horizons taper
off similarly toward the end of your data: blank means "can't be computed
yet", not "no issues survived".

Per file, per week, per horizon:
  open_issues_t          issues open at week t on this file
  still_open_<h>         of those, how many (by key) are still open at week t+h
  closed_<h>              open_issues_t - still_open_<h>
  survival_rate_<h>       still_open_<h> / open_issues_t

Usage:
    python3 compute_issue_persistence.py
"""
import csv
import json
from pathlib import Path

DATA_DIR = Path("data")
OUTPUT_DIR = Path("issue_persistence")
HORIZONS = {"3w": 3, "1m": 4, "3m": 12, "180d": 26}


ACTIVE_STATUSES = {"OPEN", "CONFIRMED", "REOPENED"}


def load_issue_keys(label):
    """Returns {file_path: set(issue keys open at this week)}.
    pull_issues.py now saves every status; "open at this week" means
    active, so filter here rather than change the meaning of open_issues_t."""
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
    weekly_keys = {label: load_issue_keys(label) for label in week_labels}

    OUTPUT_DIR.mkdir(exist_ok=True)
    for i, label in enumerate(week_labels):
        files = sorted(weekly_keys[label].keys())
        rows = []
        for fpath in files:
            open_now = weekly_keys[label].get(fpath, set())
            row = {"artifact_id": fpath, "open_issues_t": len(open_now)}

            for suffix, h in HORIZONS.items():
                j = i + h
                if j >= n_weeks or not open_now:
                    row[f"still_open_{suffix}"] = ""
                    row[f"closed_{suffix}"] = ""
                    row[f"survival_rate_{suffix}"] = ""
                    continue
                future_keys = weekly_keys[week_labels[j]].get(fpath, set())
                still_open = len(open_now & future_keys)
                row[f"still_open_{suffix}"] = still_open
                row[f"closed_{suffix}"] = len(open_now) - still_open
                row[f"survival_rate_{suffix}"] = round(still_open / len(open_now), 4)

            rows.append(row)

        if rows:
            with open(OUTPUT_DIR / f"{label}.csv", "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)
        print(f"{label}.csv done ({len(rows)} files)")


if __name__ == "__main__":
    main()
