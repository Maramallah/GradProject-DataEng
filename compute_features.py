#!/usr/bin/env python3
"""
Reads everything saved under data/ and builds a feature table:
data/npf90_features.csv

Assumes data/snapshots.csv contains your historical snapshots plus
your T0 snapshot, and that T0 is the one with the LATEST analysis_date.

Usage:
    python3 compute_features.py
"""
import csv
import json
import statistics
from datetime import datetime


def load_snapshots():
    with open("data/snapshots.csv") as f:
        rows = list(csv.DictReader(f))
    rows.sort(key=lambda r: r["analysis_date"])
    return rows


def load_measures(label):
    with open(f"data/measures_{label}.csv") as f:
        return {r["file_path"]: r for r in csv.DictReader(f)}


ACTIVE_STATUSES = {"OPEN", "CONFIRMED", "REOPENED"}


def load_issues(label, active_only=True):
    """pull_issues.py now saves every status (needed for lifecycle/closures
    elsewhere) -- default to active-only here since "findings at T0" means
    currently-open findings, not ones already resolved by T0."""
    with open(f"data/issues_{label}.json") as f:
        issues = json.load(f)
    if active_only:
        issues = [i for i in issues if i["status"] in ACTIVE_STATUSES]
    return issues


def parse_minutes(effort):
    """SonarQube returns effort like '5min' or '1h30min'. Simple parser, good enough for an MVP."""
    if not effort:
        return 0
    total, num = 0, ""
    for ch in effort:
        if ch.isdigit():
            num += ch
        elif ch == "h":
            total += int(num or 0) * 60
            num = ""
        elif ch == "m":
            total += int(num or 0)
            num = ""
    return total


def linreg_slope(points):
    """points: list of (x, y). Returns the slope of the best-fit line, or 0 if fewer than 2 points."""
    if len(points) < 2:
        return 0.0
    n = len(points)
    xs, ys = [p[0] for p in points], [p[1] for p in points]
    mean_x, mean_y = sum(xs) / n, sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in points)
    den = sum((x - mean_x) ** 2 for x in xs)
    return num / den if den else 0.0


def main():
    snapshots = load_snapshots()
    t0 = snapshots[-1]
    history = snapshots[:-1]  # everything before T0, oldest first

    t0_measures = load_measures(t0["label"])
    t0_issues = load_issues(t0["label"])  # active only

    SEVERITY_COLS = {
        "BLOCKER": "blocker_finding_count_t0",
        "CRITICAL": "critical_finding_count_t0",
        "MAJOR": "major_finding_count_t0",
        "MINOR": "minor_finding_count_t0",
        "INFO": "info_finding_count_t0",
    }
    TYPE_COLS = {
        "CODE_SMELL": "code_smell_count_t0",
        "BUG": "bug_count_t0",
        "VULNERABILITY": "vulnerability_count_t0",
    }

    findings_by_file, debt_by_file = {}, {}
    severity_by_file = {}  # fpath -> {BLOCKER: n, ...}
    type_by_file = {}  # fpath -> {CODE_SMELL: n, ...}
    for issue in t0_issues:
        fpath = issue["component"].split(":", 1)[-1]
        findings_by_file[fpath] = findings_by_file.get(fpath, 0) + 1
        debt_by_file[fpath] = debt_by_file.get(fpath, 0) + parse_minutes(issue["effort_minutes"])

        sev = issue.get("severity")
        if sev in SEVERITY_COLS:
            severity_by_file.setdefault(fpath, {})
            severity_by_file[fpath][sev] = severity_by_file[fpath].get(sev, 0) + 1

        typ = issue.get("type")
        if typ in TYPE_COLS:
            type_by_file.setdefault(fpath, {})
            type_by_file[fpath][typ] = type_by_file[fpath].get(typ, 0) + 1

    # Load (date, measures, issue-counts) for every historical snapshot before T0
    history_data = []
    for snap in history:
        m = load_measures(snap["label"])
        iss = load_issues(snap["label"])
        counts = {}
        for issue in iss:
            fpath = issue["component"].split(":", 1)[-1]
            counts[fpath] = counts.get(fpath, 0) + 1
        date = datetime.fromisoformat(snap["analysis_date"].replace("Z", "+00:00"))
        history_data.append((date, m, counts))

    out_rows = []
    for fpath, m in t0_measures.items():
        ncloc = int(m.get("ncloc") or 0)
        finding_count_t0 = findings_by_file.get(fpath, 0)
        debt_minutes_t0 = debt_by_file.get(fpath, 0)

        row = {
            "file_path": fpath,
            "ncloc_t0": ncloc,
            "complexity_t0": m.get("complexity", ""),
            "cognitive_complexity_t0": m.get("cognitive_complexity", ""),
            "duplicated_lines_density_t0": m.get("duplicated_lines_density", ""),
            "comment_lines_density_t0": m.get("comment_lines_density", ""),
            "finding_count_t0": finding_count_t0,
            "finding_density_t0": round(1000 * finding_count_t0 / ncloc, 4) if ncloc else "",
            "technical_debt_minutes_t0": debt_minutes_t0,
            "technical_debt_density_t0": round(1000 * debt_minutes_t0 / ncloc, 4) if ncloc else "",
        }

        # B2 severity/type breakdown (contract section 1.5). Zero-fill --
        # a file with findings but none of a given severity/type is a real
        # 0, not a missing value.
        file_sev = severity_by_file.get(fpath, {})
        for sev, col in SEVERITY_COLS.items():
            row[col] = file_sev.get(sev, 0)
        file_typ = type_by_file.get(fpath, {})
        for typ, col in TYPE_COLS.items():
            row[col] = file_typ.get(typ, 0)

        # B3: finding-density trend across history, for this file
        points = []
        for date, m_hist, counts_hist in history_data:
            hist_ncloc = int(m_hist.get(fpath, {}).get("ncloc") or 0)
            hist_count = counts_hist.get(fpath, 0)
            if hist_ncloc:
                points.append((date.timestamp(), 1000 * hist_count / hist_ncloc))

        row["finding_density_slope_hist"] = round(linreg_slope(points), 8)
        row["finding_density_volatility_hist"] = (
            round(statistics.pstdev([p[1] for p in points]), 4) if len(points) > 1 else ""
        )

        out_rows.append(row)

    if not out_rows:
        print("No rows to write -- did you run pull_metrics.py / pull_issues.py for t0?")
        return

    fieldnames = list(out_rows[0].keys())
    with open("data/npf90_features.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    print(f">>> Wrote {len(out_rows)} rows to data/npf90_features.csv")


if __name__ == "__main__":
    main()
