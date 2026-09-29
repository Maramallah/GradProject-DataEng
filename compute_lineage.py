#!/usr/bin/env python3
"""
Tracks file identity (lineage_id) across weekly commits, so a rename
doesn't look like "the file at T0 vanished and an unrelated new file
appeared at T90" -- the contract is explicit that "a path is not an
identity" (section 1.1).

APPROACH (heuristic, matches this project's "MVP / research contract,
not production authorization" status on page 1 of the PDF):
  - Week 1: every file gets a fresh lineage_id (sha1 of its path) and
    confidence HIGH.
  - Each subsequent week: `git diff -M -C --name-status` between last
    week's commit and this week's commit.
      * unchanged path                -> lineage_id carries over, HIGH
      * clean 1:1 rename (R###)       -> lineage_id carries over, HIGH
      * copy (C###), or a source that
        rename-detection maps to
        MULTIPLE targets (a split)    -> ALL paths sharing that source
                                          get confidence AMBIGUOUS
      * plain delete (D) with no
        matching rename/copy record   -> lineage ends here (file is
                                          genuinely gone, not tracked
                                          forward)
      * plain add (A) with no
        matching rename/copy record   -> brand-new lineage_id, HIGH
        (it really is a new file, not a continuation of anything)
  - "MERGE"-type ambiguity (two old paths collapsing onto one new path)
    isn't distinguishable from an ordinary modify with today's --numstat
    based diff, so it isn't specifically detected -- flag as a known gap
    if you need to certify it away.

Writes lineage/week_XX.csv (artifact_id, lineage_id, lineage_confidence)
for every week, and lineage/lineage_events.csv logging every detected
rename/copy for audit.

Uses the SAME weekly commit calendar as scan_weekly_snapshots.py.

Usage:
    python3 compute_lineage.py <start_date> [branch] [weeks]
"""
import csv
import hashlib
import subprocess
import sys
from pathlib import Path

import config
from weekly_commits import resolve_weekly_commits

OUTPUT_DIR = Path("lineage")


def run_git(repo_path, args):
    result = subprocess.run(
        ["git", "-C", str(repo_path), *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Git command failed:\n{' '.join(args)}\n\n{result.stderr}")
    return result.stdout


def list_files_at(repo_path, commit):
    out = run_git(repo_path, ["ls-tree", "-r", "--name-only", commit])
    return [f for f in out.splitlines() if f.endswith(".java")]


def diff_name_status(repo_path, commit_from, commit_to):
    """Returns list of (status, old_path, new_path). new_path is None for D/A."""
    out = run_git(repo_path, ["diff", "-M", "-C", "--name-status", commit_from, commit_to])
    events = []
    for line in out.splitlines():
        parts = line.split("\t")
        if not parts or not parts[0]:
            continue
        status = parts[0]
        if status.startswith(("R", "C")) and len(parts) == 3:
            events.append((status[0], parts[1], parts[2]))
        elif status in ("A", "D") and len(parts) == 2:
            events.append((status, parts[1] if status == "D" else None, parts[1] if status == "A" else None))
    return events


def new_lineage_id(path, salt):
    return hashlib.sha1(f"{salt}:{path}".encode("utf-8")).hexdigest()[:16]


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

    OUTPUT_DIR.mkdir(exist_ok=True)
    week_nums = sorted(weeks.keys())
    events_log = []

    # Week 1: everyone gets a fresh lineage.
    first_commit = weeks[week_nums[0]]["commit"]
    current = {}  # path -> (lineage_id, confidence)
    for path in list_files_at(config.KAFKA_REPO_PATH, first_commit):
        current[path] = (new_lineage_id(path, config.PROJECT_KEY), "HIGH")
    _write_week(week_nums[0], current)

    last_commit = first_commit
    for week in week_nums[1:]:
        commit = weeks[week]["commit"]
        if commit == last_commit:
            _write_week(week, current)
            continue

        events = diff_name_status(config.KAFKA_REPO_PATH, last_commit, commit)
        events = [e for e in events if (e[1] or "").endswith(".java") or (e[2] or "").endswith(".java")]

        # find sources that map to >1 target (splits/copies) -> ambiguous
        source_targets = {}
        for status, old, new in events:
            if status in ("R", "C") and old:
                source_targets.setdefault(old, []).append(new)

        nxt = {}
        for status, old, new in events:
            if status == "D":
                events_log.append((week, "D", old, "", ""))
                # lineage ends; nothing written forward for `old`
                continue
            if status == "A":
                nxt[new] = (new_lineage_id(new, config.PROJECT_KEY), "HIGH")
                events_log.append((week, "A", "", new, "HIGH"))
                continue
            if status in ("R", "C"):
                lin_id, conf = current.get(old, (new_lineage_id(old, config.PROJECT_KEY), "UNSUPPORTED"))
                ambiguous = status == "C" or len(source_targets.get(old, [])) > 1
                new_conf = "AMBIGUOUS" if ambiguous else conf
                nxt[new] = (lin_id, new_conf)
                events_log.append((week, status, old, new, new_conf))

        # anything not touched by the diff carries its identity forward unchanged
        touched_old = {old for _, old, _ in events if old}
        touched_new = {new for _, _, new in events if new}
        for path, (lin_id, conf) in current.items():
            if path not in touched_old and path not in touched_new and path.endswith(".java"):
                nxt[path] = (lin_id, conf)

        current = nxt
        _write_week(week, current)
        last_commit = commit

    with open(OUTPUT_DIR / "lineage_events.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["week", "event_type", "old_path", "new_path", "resulting_confidence"])
        writer.writerows(events_log)

    print(f">>> Lineage tracked across {len(week_nums)} weeks -> {OUTPUT_DIR}/")


def _write_week(week, current):
    label = f"week_{week:02d}"
    with open(OUTPUT_DIR / f"{label}.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["artifact_id", "lineage_id", "lineage_confidence"])
        for path, (lin_id, conf) in sorted(current.items()):
            writer.writerow([path, lin_id, conf])
    print(f"{label}.csv done ({len(current)} files)")


if __name__ == "__main__":
    main()
