#!/usr/bin/env python3
"""
Project-level (repo-wide) Git dev-history features, one row per week -- no
per-file identity tracking needed, which is what makes this simpler than
the file-level version.

One repo-wide `git log --shortstat` pass over the whole date range covered
by your scanned weeks, then bucketed/windowed per week:

  commits_7d/30d/90d/180d       commit count in the trailing window
  lines_added_7d                inserted lines in the trailing 7 days
  lines_deleted_7d              deleted lines in the trailing 7 days
  churn_7d/30d/90d/180d         added+deleted lines in the trailing window
  contributors_30d/90d/180d     distinct authors in the trailing window
  commit_acceleration           commits_7d(this week) - commits_7d(last week)
  churn_acceleration            churn_7d(this week) - churn_7d(last week)
  commit_burstiness_180d        Goh-Barabasi burstiness of weekly commit counts, 26 buckets
  churn_burstiness_180d         same, over weekly churn sums
  ownership_entropy_180d        Shannon entropy (base 2) of author-commit shares, trailing 180d

Note: commit_burstiness_180d and churn_burstiness_180d both use the
Goh-Barabasi formula here for consistency (your file-level pipeline_g3.py
uses two different formulas for these two columns -- var/mean for commits,
Goh-Barabasi for churn). If you want strict consistency with that codebase
instead, swap commit_burstiness_180d's formula to variance/mean.

Uses the SAME weekly commit calendar as scan_weekly_snapshots.py (pass the
identical start_date/branch/weeks) so week numbers line up with your Sonar
project features.

Usage:
    python3 compute_project_git_features.py <start_date> [branch] [weeks]

Example:
    python3 compute_project_git_features.py 2011-08-01 trunk 26
"""
import csv
import math
import statistics
import subprocess
import sys
from pathlib import Path

import pandas as pd

import config
from weekly_commits import resolve_weekly_commits

OUTPUT_FILE = Path("project_git_features.csv")


def run_git(repo_path, args):
    result = subprocess.run(
        ["git", "-C", str(repo_path), *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Git command failed:\n{' '.join(args)}\n\n{result.stderr}")
    return result.stdout


def collect_commits(repo_path, end_commit, since, until):
    """One git log --shortstat pass. Returns [(date, author, added, deleted), ...]."""
    out = run_git(repo_path, [
        "log", end_commit,
        f"--since={since}", f"--until={until}",
        "--no-merges", "--format=COMMIT|%aI|%an", "--shortstat",
    ])
    commits = []
    pending = None  # (date, author)

    def flush(added=0, deleted=0):
        nonlocal pending
        if pending is not None:
            commits.append((pending[0], pending[1], added, deleted))
            pending = None

    for line in out.splitlines():
        line = line.strip()
        if line.startswith("COMMIT|"):
            flush()  # previous commit had no stat line -> empty commit, added=0/deleted=0
            _, iso_date, name = line.split("|", 2)
            pending = (pd.Timestamp(iso_date).tz_localize(None), name)
            continue
        if "changed" in line and ("insertion" in line or "deletion" in line):
            added, deleted = 0, 0
            for part in line.split(","):
                part = part.strip()
                if "insertion" in part:
                    added = int(part.split()[0])
                elif "deletion" in part:
                    deleted = int(part.split()[0])
            flush(added, deleted)

    flush()  # last commit in the log
    return commits


def window_commits(commits, window_end, days):
    window_start = window_end - pd.Timedelta(days=days)
    return [c for c in commits if window_start < c[0] <= window_end]


def burstiness(counts):
    """Goh-Barabasi burstiness parameter over a list of per-bucket counts."""
    if len(counts) < 2 or sum(counts) == 0:
        return None
    mean = statistics.mean(counts)
    stdev = statistics.pstdev(counts)
    if stdev + mean == 0:
        return None
    return round((stdev - mean) / (stdev + mean), 6)


def shannon_entropy(author_counts):
    total = sum(author_counts.values())
    if total == 0:
        return None
    ent = 0.0
    for c in author_counts.values():
        p = c / total
        if p > 0:
            ent -= p * math.log2(p)
    return round(ent, 6)


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
    earliest_start = list(weeks.values())[0]["start"] - pd.Timedelta(days=180)
    latest_end = list(weeks.values())[-1]["end"]

    print(f">>> Walking git history once: {earliest_start.date()} -> {latest_end.date()}")
    commits = collect_commits(
        config.KAFKA_REPO_PATH, latest_commit,
        earliest_start.strftime("%Y-%m-%dT00:00:00"),
        latest_end.strftime("%Y-%m-%dT23:59:59"),
    )

    rows = []
    prev_commits_7d, prev_churn_7d = None, None
    for week, bounds in weeks.items():
        label = f"week_{week:02d}"
        week_end = bounds["end"]

        c7 = window_commits(commits, week_end, 7)
        c30 = window_commits(commits, week_end, 30)
        c90 = window_commits(commits, week_end, 90)
        c180 = window_commits(commits, week_end, 180)

        commits_7d, commits_30d, commits_90d, commits_180d = len(c7), len(c30), len(c90), len(c180)
        lines_added_7d = sum(c[2] for c in c7)
        lines_deleted_7d = sum(c[3] for c in c7)
        churn_7d = lines_added_7d + lines_deleted_7d
        churn_30d = sum(c[2] + c[3] for c in c30)
        churn_90d = sum(c[2] + c[3] for c in c90)
        churn_180d = sum(c[2] + c[3] for c in c180)
        contributors_30d = len({c[1] for c in c30})
        contributors_90d = len({c[1] for c in c90})
        contributors_180d = len({c[1] for c in c180})

        commit_acceleration = (commits_7d - prev_commits_7d) if prev_commits_7d is not None else None
        churn_acceleration = (churn_7d - prev_churn_7d) if prev_churn_7d is not None else None
        prev_commits_7d, prev_churn_7d = commits_7d, churn_7d

        # 26 weekly buckets over the trailing 180 days, for burstiness
        commit_buckets, churn_buckets = [], []
        window_start_180 = week_end - pd.Timedelta(days=180)
        for b in range(26):
            b_start = window_start_180 + pd.Timedelta(days=b * 7)
            b_end = b_start + pd.Timedelta(days=7)
            bucket_commits = [c for c in commits if b_start < c[0] <= b_end]
            commit_buckets.append(len(bucket_commits))
            churn_buckets.append(sum(c[2] + c[3] for c in bucket_commits))

        author_counts_180d = {}
        for c in c180:
            author_counts_180d[c[1]] = author_counts_180d.get(c[1], 0) + 1

        rows.append({
            "week": label,
            "snapshot_date": week_end.isoformat(),
            "commits_7d": commits_7d, "commits_30d": commits_30d,
            "commits_90d": commits_90d, "commits_180d": commits_180d,
            "lines_added_7d": lines_added_7d, "lines_deleted_7d": lines_deleted_7d,
            "churn_7d": churn_7d, "churn_30d": churn_30d,
            "churn_90d": churn_90d, "churn_180d": churn_180d,
            "contributors_30d": contributors_30d, "contributors_90d": contributors_90d,
            "contributors_180d": contributors_180d,
            "commit_acceleration": commit_acceleration, "churn_acceleration": churn_acceleration,
            "commit_burstiness_180d": burstiness(commit_buckets),
            "churn_burstiness_180d": burstiness(churn_buckets),
            "ownership_entropy_180d": shannon_entropy(author_counts_180d),
        })

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f">>> Wrote {len(rows)} row(s) to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
