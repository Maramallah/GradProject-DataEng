"""
Shared weekly commit resolver -- the same week-boundary/commit-resolution
logic pipeline_total_line.py uses internally, extracted so every other
script (Sonar scanning, ownership entropy) can produce the SAME week_XX
numbering without duplicating the date math.

IMPORTANT: always call this with the same start_date and branch you used
for your total_lines / churn extraction, or week_XX won't line up across
your data sources.
"""
import subprocess
from pathlib import Path

import pandas as pd


def resolve_weekly_commits(repo_path, start_date, branch="trunk", max_weeks=None):
    """
    Returns {week_number: {"start": Timestamp, "end": Timestamp, "commit": str|None}}
    for every 7-day window from start_date, either through the latest commit on
    branch (max_weeks=None) or capped at max_weeks windows -- pass max_weeks to
    match a fixed-size dataset (e.g. 26 weeks) instead of walking to "now".
    """
    repo_path = Path(repo_path)
    start_date = pd.Timestamp(start_date)

    def run_git(args):
        result = subprocess.run(
            ["git", "-C", str(repo_path), *args],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            raise RuntimeError(f"Git command failed:\n{' '.join(args)}\n\n{result.stderr}")
        return result.stdout

    latest_date = run_git(["log", "-1", "--format=%cI", branch]).strip()
    latest_date = pd.Timestamp(latest_date).tz_localize(None)

    weeks = {}
    current_start = start_date
    week_number = 1
    while current_start <= latest_date:
        if max_weeks is not None and week_number > max_weeks:
            break
        current_end = current_start + pd.Timedelta(days=6)
        weeks[week_number] = {"start": current_start, "end": current_end}
        current_start += pd.Timedelta(days=7)
        week_number += 1

    for week, bounds in weeks.items():
        before_date = (bounds["end"] + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        commit = run_git(["rev-list", "-1", f"--before={before_date}", branch]).strip()
        bounds["commit"] = commit or None

    return weeks
