#!/usr/bin/env python3
"""
Computes the B1 Git/process columns (contract section 1.4) per file per
week, using EXACT trailing calendar-day windows ending at each week's
snapshot date -- not the weekly-bucket approximations pipeline_g3.py
uses for churn_acceleration/commit_burstiness. Both are kept: this is
an additional, more precise feature set alongside your existing
pipeline_g3.py output, not a replacement for it.

  commits_30d / _90d / _180d
  lines_added_30d / _90d / _180d, lines_deleted_30d / _90d / _180d
  churn_30d / _90d / _180d                 (added + deleted)
  authors_30d / _90d / _180d
  file_age_days                            days since earliest commit
                                            touching this file THAT'S
                                            VISIBLE in the collected git
                                            history window (see caveat
                                            below)
  days_since_last_change
  commit_acceleration, churn_acceleration  recent 30d vs the prior,
                                            comparable 30d before that
                                            (formula frozen here: v2)
  commit_burstiness_180d, churn_burstiness_180d
                                            same variance/mean and
                                            (std-mean)/(std+mean)
                                            formulas pipeline_g3.py
                                            uses for its 3-week windows,
                                            applied to 26 weekly buckets
                                            covering the trailing 180d
  git_history_missing                      True if the repo's commit
                                            history doesn't reach back
                                            180 days before this week
  last_change_missing                      True if no commit touching
                                            this file was found at all
                                            in the collected window

CAVEAT on file_age_days: it's computed from the earliest commit visible
in the date range this script actually walks (which starts 180 days
before your earliest week). If the file is OLDER than that, file_age_days
undercounts its true age -- pass an earlier <start_date> below if you
need true first-ever-commit age; this script does one
--since=<start_date - 180d> git log pass, same trick
compute_ownership_entropy.py uses, so widening it is cheap.

Uses the SAME weekly commit calendar as scan_weekly_snapshots.py (pass
the identical start_date/branch) so week numbers line up.

Usage:
    python3 compute_git_window_features.py <start_date> [branch] [weeks]

Example (matches an existing 26-week dataset):
    python3 compute_git_window_features.py 2011-08-01 trunk 26
"""
import csv
import subprocess
import sys
from pathlib import Path

import pandas as pd

import config
from weekly_commits import resolve_weekly_commits

OUTPUT_DIR = Path("git_window_features")
WINDOWS_DAYS = {"30d": 30, "90d": 90, "180d": 180}
BURSTINESS_WINDOW_DAYS = 180
BURSTINESS_BUCKETS = 26  # weekly buckets across the 180-day window


def run_git(repo_path, args):
    result = subprocess.run(
        ["git", "-C", str(repo_path), *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Git command failed:\n{' '.join(args)}\n\n{result.stderr}")
    return result.stdout


def collect_history(repo_path, end_commit, since, until):
    """One git log --numstat pass. Returns {file_path: [(date, author, added, deleted), ...]}."""
    out = run_git(repo_path, [
        "log", end_commit,
        f"--since={since}", f"--until={until}",
        "--no-merges", "--format=COMMIT|%aI|%an", "--numstat",
    ])
    history = {}
    commit_date, commit_author = None, None
    for line in out.splitlines():
        if line.startswith("COMMIT|"):
            _, iso_date, author = line.split("|", 2)
            commit_date = pd.Timestamp(iso_date).tz_localize(None)
            commit_author = author
            continue
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        added, deleted, path = parts
        if "=>" in path:
            path = path.split("=>")[-1].strip().strip("{}")
        added = int(added) if added.isdigit() else 0
        deleted = int(deleted) if deleted.isdigit() else 0
        history.setdefault(path, []).append((commit_date, commit_author, added, deleted))
    return history


def events_in(events, start, end):
    """(start, end] inclusive of end, matching the rest of this pipeline's convention."""
    return [e for e in events if start < e[0] <= end]


def burstiness(daily_counts):
    """pipeline_g3.py's formulas, generalized from 3 weekly buckets to N."""
    if not daily_counts:
        return None, None
    import statistics
    mean = sum(daily_counts) / len(daily_counts)
    var = statistics.pvariance(daily_counts) if len(daily_counts) > 1 else 0.0
    std = var ** 0.5
    commit_b = (var / mean) if mean > 0 else 0.0
    churn_b = ((std - mean) / (std + mean)) if (std + mean) > 0 else 0.0
    return commit_b, churn_b


def main():
    if len(sys.argv) not in (2, 3, 4):
        print(__doc__)
        sys.exit(1)
    start_date = sys.argv[1]
    branch = sys.argv[2] if len(sys.argv) >= 3 else "trunk"
    max_weeks = int(sys.argv[3]) if len(sys.argv) == 4 else 26

    weeks = resolve_weekly_commits(config.KAFKA_REPO_PATH, start_date, branch, max_weeks=max_weeks)
    weeks = {w: b for w, b in weeks.items() if b["commit"]}
    if not weeks:
        print("No weeks resolved -- check start_date/branch.")
        return

    latest_commit = list(weeks.values())[-1]["commit"]
    earliest_start = list(weeks.values())[0]["start"] - pd.Timedelta(days=BURSTINESS_WINDOW_DAYS)
    latest_end = list(weeks.values())[-1]["end"]

    print(f">>> Walking git history once: {earliest_start.date()} -> {latest_end.date()}")
    history = collect_history(
        config.KAFKA_REPO_PATH, latest_commit,
        earliest_start.strftime("%Y-%m-%dT00:00:00"),
        latest_end.strftime("%Y-%m-%dT23:59:59"),
    )
    repo_history_floor = earliest_start  # earliest date this script actually walked back to

    OUTPUT_DIR.mkdir(exist_ok=True)
    for week, bounds in weeks.items():
        label = f"week_{week:02d}"
        t0 = bounds["end"]

        rows = []
        for fpath, events in history.items():
            row = {"artifact_id": fpath}

            for suffix, days in WINDOWS_DAYS.items():
                window_start = t0 - pd.Timedelta(days=days)
                evs = events_in(events, window_start, t0)
                added = sum(e[2] for e in evs)
                deleted = sum(e[3] for e in evs)
                row[f"commits_{suffix}"] = len(evs)
                row[f"lines_added_{suffix}"] = added
                row[f"lines_deleted_{suffix}"] = deleted
                row[f"churn_{suffix}"] = added + deleted
                row[f"authors_{suffix}"] = len({e[1] for e in evs})

            # acceleration: recent 30d vs the prior, comparable 30d before that
            recent_start = t0 - pd.Timedelta(days=30)
            prior_start = t0 - pd.Timedelta(days=60)
            recent = events_in(events, recent_start, t0)
            prior = events_in(events, prior_start, recent_start)
            recent_churn = sum(e[2] + e[3] for e in recent)
            prior_churn = sum(e[2] + e[3] for e in prior)
            row["commit_acceleration"] = len(recent) - len(prior)
            row["churn_acceleration"] = recent_churn - prior_churn

            # burstiness across 26 weekly buckets spanning the trailing 180d
            bucket_start = t0 - pd.Timedelta(days=BURSTINESS_WINDOW_DAYS)
            commit_buckets, churn_buckets = [], []
            for b in range(BURSTINESS_BUCKETS):
                b_start = bucket_start + pd.Timedelta(days=7 * b)
                b_end = b_start + pd.Timedelta(days=7)
                b_events = events_in(events, b_start, b_end)
                commit_buckets.append(len(b_events))
                churn_buckets.append(sum(e[2] + e[3] for e in b_events))
            commit_b, _ = burstiness(commit_buckets)
            _, churn_b = burstiness(churn_buckets)
            row["commit_burstiness_180d"] = commit_b
            row["churn_burstiness_180d"] = churn_b

            all_dates = [e[0] for e in events if e[0] <= t0]
            if all_dates:
                last_change = max(all_dates)
                first_change = min(all_dates)
                row["file_age_days"] = (t0 - first_change).days
                row["days_since_last_change"] = (t0 - last_change).days
                row["last_change_missing"] = False
            else:
                row["file_age_days"] = ""
                row["days_since_last_change"] = ""
                row["last_change_missing"] = True

            row["git_history_missing"] = (t0 - pd.Timedelta(days=BURSTINESS_WINDOW_DAYS)) < repo_history_floor

            rows.append(row)

        with open(OUTPUT_DIR / f"{label}.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["artifact_id"])
            writer.writeheader()
            writer.writerows(rows)
        print(f"{label}.csv done ({len(rows)} files)")


if __name__ == "__main__":
    main()
