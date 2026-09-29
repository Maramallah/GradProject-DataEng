#!/usr/bin/env python3
"""
Computes ownership_entropy_180d per file per week: Shannon entropy (base 2)
of the per-author share of commits touching the file in the 180 days
trailing each week's commit. This is the one Git-only B1 feature
pipeline_g3.py doesn't already cover (it handles churn/commit acceleration
+ burstiness).

Uses the SAME weekly commit calendar as scan_weekly_snapshots.py and
pipeline_total_line.py (pass the identical start_date/branch) so week
numbers line up. Does ONE repo-wide `git log` pass covering the whole date
range instead of one call per week, then slices it per week in memory.

Usage:
    python3 compute_ownership_entropy.py <start_date> [branch] [weeks]

Example (matches an existing 26-week dataset):
    python3 compute_ownership_entropy.py 2011-08-01 trunk 26
"""
import csv
import math
import subprocess
import sys
from pathlib import Path

import pandas as pd

import config
from weekly_commits import resolve_weekly_commits

WINDOW_DAYS = 180
OUTPUT_DIR = Path("ownership_entropy")


def run_git(repo_path, args):
    result = subprocess.run(
        ["git", "-C", str(repo_path), *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Git command failed:\n{' '.join(args)}\n\n{result.stderr}")
    return result.stdout


def collect_history(repo_path, end_commit, since, until):
    """One git log --numstat pass. Returns {file_path: [(commit_date, author), ...]}."""
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
        _added, _deleted, path = parts
        if "=>" in path:
            path = path.split("=>")[-1].strip().strip("{}")
        history.setdefault(path, []).append((commit_date, commit_author))
    return history


def shannon_entropy(events, window_start, window_end):
    author_counts = {}
    for date, author in events:
        if window_start < date <= window_end:
            author_counts[author] = author_counts.get(author, 0) + 1
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
    earliest_start = list(weeks.values())[0]["start"] - pd.Timedelta(days=WINDOW_DAYS)
    latest_end = list(weeks.values())[-1]["end"]

    print(f">>> Walking git history once: {earliest_start.date()} -> {latest_end.date()}")
    history = collect_history(
        config.KAFKA_REPO_PATH, latest_commit,
        earliest_start.strftime("%Y-%m-%dT00:00:00"),
        latest_end.strftime("%Y-%m-%dT23:59:59"),
    )

    OUTPUT_DIR.mkdir(exist_ok=True)
    for week, bounds in weeks.items():
        label = f"week_{week:02d}"
        window_end = bounds["end"]
        window_start = window_end - pd.Timedelta(days=WINDOW_DAYS)

        rows = []
        for fpath, events in history.items():
            ent = shannon_entropy(events, window_start, window_end)
            if ent is not None:
                rows.append({"artifact_id": fpath, "ownership_entropy_180d": ent})

        with open(OUTPUT_DIR / f"{label}.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["artifact_id", "ownership_entropy_180d"])
            writer.writeheader()
            writer.writerows(rows)
        print(f"{label}.csv done ({len(rows)} files)")


if __name__ == "__main__":
    main()
