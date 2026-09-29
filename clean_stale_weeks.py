#!/usr/bin/env python3
"""
Deletes stale week_XX.csv files above a given week number.

Useful after re-running pipeline_total_line.py with a smaller/corrected
--weeks value: its OLD, uncapped run may have left week_61.csv,
week_100.csv, etc. sitting in the output directory, and those never get
overwritten by a smaller run -- they just sit there and break the next
script that reads "every week_*.csv in this folder" (e.g. pipeline_g3.py).

Usage:
    python3 clean_stale_weeks.py <max_week> <dir> [<dir> ...]

Example:
    python3 clean_stale_weeks.py 60 weekly_total_lines weekly_g3_features
"""
import re
import sys
from pathlib import Path

WEEK_RE = re.compile(r"week_(\d+)\.csv$")


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    max_week = int(sys.argv[1])
    dirs = [Path(d) for d in sys.argv[2:]]

    total_removed = 0
    for d in dirs:
        if not d.exists():
            print(f">>> {d}/ does not exist, skipping.")
            continue
        removed_here = 0
        for f in sorted(d.glob("week_*.csv")):
            m = WEEK_RE.search(f.name)
            if not m:
                continue
            week_num = int(m.group(1))
            if week_num > max_week:
                f.unlink()
                print(f"removed {f}")
                removed_here += 1
        print(f">>> {d}/: removed {removed_here} file(s) above week_{max_week:02d}.")
        total_removed += removed_here

    print(f">>> Done. {total_removed} file(s) removed total.")


if __name__ == "__main__":
    main()
