#!/usr/bin/env python3
"""
Scans one SonarQube snapshot per week, from start_date to the latest commit
on `branch`, using the SAME weekly commit-resolution logic as
pipeline_total_line.py -- so week_XX here lines up with week_XX in your
weekly_with_total_lines / author_weekly / g3 folders.

No manual commit hashes or dates: this walks the calendar, resolves each
week's commit via git, and runs scan_snapshot.py + pull_metrics.py +
pull_issues.py for it, labeled week_XX.

IMPORTANT: pass the exact same start_date, branch, and week count you used
for your total_lines / churn extraction, or week_XX won't line up.

IDEMPOTENT / RESUMABLE: before scanning a week, checks whether that
label is already in data/snapshots.csv. If so, it's skipped -- this is
what makes it safe to re-run with a BIGGER --weeks value later (e.g. you
scanned 39 weeks already and now want 60): only the new weeks (40-60)
actually get scanned. This matters because scan_snapshot.py appends to
data/snapshots.csv / data/analysis_metadata.csv with no de-duplication
of its own -- re-scanning an already-recorded label would otherwise
silently double that row and waste a full Sonar scan for nothing. If
you ever need to force a genuine re-scan of an already-recorded week
(e.g. you fixed a bad quality profile), pass --force, or delete that
label's row from data/snapshots.csv and data/analysis_metadata.csv
yourself first.

Usage:
    python3 scan_weekly_snapshots.py <start_date> [branch] [weeks] [--force]

Example (matches an existing 26-week dataset):
    python3 scan_weekly_snapshots.py 2011-08-01 trunk 26

Example (extending a 39-week dataset to 60 weeks -- only scans 40-60):
    python3 scan_weekly_snapshots.py 2011-08-01 trunk 60
"""
import csv
import os
import subprocess
import sys
from pathlib import Path

import config
from weekly_commits import resolve_weekly_commits

PYTHON = sys.executable
SNAPSHOTS_FILE = "data/snapshots.csv"

# Absolute path to the directory THIS script lives in. Needed because this
# script is invoked (by run_all_repos.py) with its CURRENT WORKING DIRECTORY
# set to a per-repo working dir (e.g. runs\OFBiz), not the directory the
# scripts themselves live in (e.g. E:\kafka). Without this, "scan_snapshot.py"
# below resolves relative to the WRONG (cwd) directory and fails with
# "No such file or directory" even though the file exists right next to
# this script. Same fix pattern run_all_repos.py already uses via here().
SCRIPT_DIR = Path(__file__).resolve().parent


def here(script_name):
    return str(SCRIPT_DIR / script_name)


def run(cmd):
    print(f">>> {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def already_scanned_labels():
    """{label: commit_hash} for every label already recorded in data/snapshots.csv."""
    if not os.path.exists(SNAPSHOTS_FILE):
        return {}
    with open(SNAPSHOTS_FILE) as f:
        return {r["label"]: r["commit_hash"] for r in csv.DictReader(f)}


def main():
    args = [a for a in sys.argv[1:] if a != "--force"]
    force = "--force" in sys.argv[1:]

    if len(args) not in (1, 2, 3):
        print(__doc__)
        sys.exit(1)
    start_date = args[0]
    branch = args[1] if len(args) >= 2 else "trunk"
    max_weeks = int(args[2]) if len(args) == 3 else 26

    weeks = resolve_weekly_commits(config.KAFKA_REPO_PATH, start_date, branch, max_weeks=max_weeks)
    print(f">>> Resolved {len(weeks)} week(s) from {start_date} to latest on '{branch}'")

    done = already_scanned_labels()
    if done:
        print(f">>> {len(done)} week(s) already recorded in {SNAPSHOTS_FILE} -- will skip those.")

    last_commit = None
    n_scanned, n_skipped_done, n_skipped_dup = 0, 0, 0
    for week, bounds in weeks.items():
        commit = bounds["commit"]
        label = f"week_{week:02d}"
        if commit is None:
            print(f">>> {label}: no commit found before {bounds['end'].date()}, skipping.")
            continue

        if label in done and not force:
            if done[label] != commit:
                print(f">>> {label}: already recorded with a DIFFERENT commit "
                      f"({done[label][:8]} vs resolved {commit[:8]}) -- skipping anyway. "
                      f"Investigate before trusting this label (branch history rewritten?), "
                      f"or pass --force to re-scan.")
            else:
                print(f">>> {label}: already scanned (commit {commit[:8]}), skipping.")
            last_commit = commit
            n_skipped_done += 1
            continue

        if commit == last_commit:
            print(f">>> {label}: same commit as previous week ({commit[:8]}), skipping scan.")
            n_skipped_dup += 1
            continue
        last_commit = commit

        print(f">>> {label}: commit {commit[:8]} ({bounds['start'].date()} -> {bounds['end'].date()})")
        run([PYTHON, here("scan_snapshot.py"), commit, label])
        run([PYTHON, here("pull_metrics.py"), label])
        run([PYTHON, here("pull_issues.py"), label])
        run([PYTHON, here("pull_analysis_metadata.py"), label])
        n_scanned += 1

    print(f">>> Weekly scanning complete: {n_scanned} newly scanned, "
          f"{n_skipped_done} already-done, {n_skipped_dup} duplicate-commit weeks skipped.")
    print(">>> Next: python3 compute_sonar_trend_features.py")


if __name__ == "__main__":
    main()
