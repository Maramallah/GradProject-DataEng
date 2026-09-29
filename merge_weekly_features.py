#!/usr/bin/env python3
"""
Joins, per week, your existing weekly feature CSVs with every other
computed source:
  <existing_dir>/week_XX.csv        your current data -- churn_rate, commits,
                                     acceleration, burstiness, total_lines, etc.
                                     (pipeline_g3.py / pipeline_total_line.py)
  snapshot_features/week_XX.csv     raw SonarQube metrics + issue counts, one
                                     file merged (merge_measures_issues.py)
  git_window_features/week_XX.csv   exact-calendar-day B1 git features --
                                     30d/90d/180d commits/churn/authors, plus
                                     acceleration/burstiness/file_age_days
                                     (compute_git_window_features.py)
  sonar_trends/week_XX.csv          SonarQube B3 trend features (3w/1m/3m/6m windows)
  finding_lifecycle/week_XX.csv     backward-looking finding arrivals/closures
                                     (3w/1m/3m/6m + 30d/90d/180d aliases)
  ownership_entropy/week_XX.csv     ownership_entropy_180d
  issue_persistence/week_XX.csv     issue survival across 3w/1m/3m/180d horizons

Writes all_features/week_XX.csv, one row per artifact_id, for every week
present in <existing_dir>/ (left join -- a file missing from any of the
other sources for a given week is left blank there, not dropped).

Usage:
    python3 merge_weekly_features.py <existing_dir>

Example:
    python3 merge_weekly_features.py weekly_with_total_lines
"""
import csv
import sys
from pathlib import Path

SNAPSHOT_DIR = Path("snapshot_features")
GIT_WINDOW_DIR = Path("git_window_features")
SONAR_DIR = Path("sonar_trends")
LIFECYCLE_DIR = Path("finding_lifecycle")
OWNERSHIP_DIR = Path("ownership_entropy")
PERSISTENCE_DIR = Path("issue_persistence")
OUTPUT_DIR = Path("all_features")

# Order here is the column order in the merged output, after artifact_id.
SOURCES = [
    SNAPSHOT_DIR,
    GIT_WINDOW_DIR,
    SONAR_DIR,
    LIFECYCLE_DIR,
    OWNERSHIP_DIR,
    PERSISTENCE_DIR,
]


def load(path):
    if not path.exists():
        return {}
    with open(path) as f:
        return {r["artifact_id"]: r for r in csv.DictReader(f)}


def cols_of(table):
    return [c for c in next(iter(table.values())).keys() if c != "artifact_id"] if table else []


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    existing_dir = Path(sys.argv[1])

    OUTPUT_DIR.mkdir(exist_ok=True)
    week_files = sorted(existing_dir.glob("week_*.csv"))
    if not week_files:
        print(f"No week_*.csv files found in {existing_dir}/.")
        return

    for existing_file in week_files:
        label = existing_file.stem
        g3 = load(existing_file)
        g3_cols = cols_of(g3)

        tables = [load(src_dir / f"{label}.csv") for src_dir in SOURCES]
        table_cols = [cols_of(t) for t in tables]

        fieldnames = ["artifact_id"] + g3_cols + [c for cols in table_cols for c in cols]

        rows = []
        for artifact_id, g3_row in g3.items():
            row = {"artifact_id": artifact_id}
            row.update({c: g3_row.get(c, "") for c in g3_cols})
            for table, cols in zip(tables, table_cols):
                other_row = table.get(artifact_id, {})
                row.update({c: other_row.get(c, "") for c in cols})
            rows.append(row)

        with open(OUTPUT_DIR / f"{label}.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"{label}.csv done ({len(rows)} files, {len(fieldnames)} columns)")


if __name__ == "__main__":
    main()
