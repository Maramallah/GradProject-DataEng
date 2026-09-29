#!/usr/bin/env python3
"""
Computes SonarQube-based trend ("ratio") features -- OLS slope and, for the
density metrics, population-stdev volatility -- over four rolling weekly
windows: 3 weeks, 1 month (4 weeks), 3 months (12 weeks), and 180 days
(26 weeks, same as 6 months). The 3m/6m column names and formulas match
sonar_git_metrics_formulas.pdf exactly; 3w/1m are the identical formulas at
shorter windows.

  finding_density_slope_{3w,1m,3m,6m}
  finding_density_volatility_{3w,1m,3m,6m}
  technical_debt_density_slope_{3w,1m,3m,6m}
  technical_debt_density_volatility_{3w,1m,3m,6m}
  complexity_slope_{3w,1m,3m,6m}
  cognitive_complexity_slope_{3w,1m,3m,6m}
  duplication_slope_{3w,1m,3m,6m}

t is a plain 1..n week index (weeks are evenly spaced by construction), not
a timestamp -- matches the PDF's t = 1, 2, ..., n. KLOC_t = ncloc_t / 1000,
using SonarQube's own ncloc measure.

Debt is converted from SonarQube's minutes to HOURS to match the PDF's
TDD_t definition -- a different unit than technical_debt_density_t0 in
compute_features.py (minutes-based, per the NPF-90 contract's B2 column).

A week only gets a value for a given window once it has that window's full
trailing history (e.g. 26 weeks of history before the 6m/180d columns fill
in); earlier weeks are left blank -- same convention pipeline_g3.py uses
for its own burstiness columns.

Reads data/measures_week_XX.csv + data/issues_week_XX.json for every
week_XX scanned by scan_weekly_snapshots.py. Writes one CSV per week to
sonar_trends/, one row per file (artifact_id).

Usage:
    python3 compute_sonar_trend_features.py
"""
import csv
import json
import statistics
from pathlib import Path

DATA_DIR = Path("data")
OUTPUT_DIR = Path("sonar_trends")
WINDOWS = {"3w": 3, "1m": 4, "3m": 12, "6m": 26}  # 6m key doubles as "180d" per the contract's naming


def parse_minutes(effort):
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


ACTIVE_STATUSES = {"OPEN", "CONFIRMED", "REOPENED"}


def load_week(label):
    with open(DATA_DIR / f"measures_{label}.csv") as f:
        measures = {r["file_path"]: r for r in csv.DictReader(f)}
    with open(DATA_DIR / f"issues_{label}.json") as f:
        # pull_issues.py saves every status now; a week's finding-density
        # snapshot is about issues open AT that week, so filter to active.
        issues = [i for i in json.load(f) if i["status"] in ACTIVE_STATUSES]

    finding_count, debt_minutes = {}, {}
    for issue in issues:
        fpath = issue["component"].split(":", 1)[-1]
        finding_count[fpath] = finding_count.get(fpath, 0) + 1
        debt_minutes[fpath] = debt_minutes.get(fpath, 0) + parse_minutes(issue["effort_minutes"])

    return measures, finding_count, debt_minutes


def per_file_densities(measures, finding_count, debt_minutes):
    """One week's DD_t / TDD_t / CC_t / CogC_t / DP_t for every file."""
    out = {}
    for fpath, m in measures.items():
        ncloc = int(m.get("ncloc") or 0)
        kloc = ncloc / 1000 if ncloc else None
        if not kloc:
            continue
        out[fpath] = {
            "DD": finding_count.get(fpath, 0) / kloc,
            "TDD": (debt_minutes.get(fpath, 0) / 60) / kloc,
            "CC": (float(m["complexity"]) / kloc) if m.get("complexity") not in (None, "") else None,
            "CogC": (float(m["cognitive_complexity"]) / kloc) if m.get("cognitive_complexity") not in (None, "") else None,
            "DP": float(m["duplicated_lines_density"]) if m.get("duplicated_lines_density") not in (None, "") else None,
        }
    return out


def ols_slope(values):
    """values: list of y's at t = 1..n. None if any point missing or fewer than 2 points."""
    if values is None or any(v is None for v in values) or len(values) < 2:
        return None
    n = len(values)
    ts = list(range(1, n + 1))
    mean_t, mean_y = sum(ts) / n, sum(values) / n
    num = sum((t - mean_t) * (y - mean_y) for t, y in zip(ts, values))
    den = sum((t - mean_t) ** 2 for t in ts)
    return round(num / den, 8) if den else 0.0


def population_stdev(values):
    if values is None or any(v is None for v in values) or len(values) < 2:
        return None
    return round(statistics.pstdev(values), 8)


def window_series(weekly, week_labels, i, w, fpath, metric):
    """The last w weeks' metric values ending at index i, or None if that history doesn't exist yet."""
    if i - w + 1 < 0:
        return None
    window = week_labels[i - w + 1: i + 1]
    return [weekly[label].get(fpath, {}).get(metric) for label in window]


def main():
    week_labels = sorted(
        p.stem.replace("measures_", "") for p in DATA_DIR.glob("measures_week_*.csv")
    )
    if not week_labels:
        print("No data/measures_week_*.csv files found -- run scan_weekly_snapshots.py first.")
        return

    OUTPUT_DIR.mkdir(exist_ok=True)

    weekly = {}  # label -> {file_path: {"DD":.., "TDD":.., "CC":.., "CogC":.., "DP":..}}
    for label in week_labels:
        measures, finding_count, debt_minutes = load_week(label)
        weekly[label] = per_file_densities(measures, finding_count, debt_minutes)

    for i, label in enumerate(week_labels):
        files = sorted(weekly[label].keys())
        rows = []
        for fpath in files:
            row = {"artifact_id": fpath}

            for metric, prefix in [("DD", "finding_density"), ("TDD", "technical_debt_density")]:
                for suffix, w in WINDOWS.items():
                    series = window_series(weekly, week_labels, i, w, fpath, metric)
                    row[f"{prefix}_slope_{suffix}"] = ols_slope(series)
                    row[f"{prefix}_volatility_{suffix}"] = population_stdev(series)

            for metric, prefix in [
                ("CC", "complexity"),
                ("CogC", "cognitive_complexity"),
                ("DP", "duplication"),
            ]:
                for suffix, w in WINDOWS.items():
                    series = window_series(weekly, week_labels, i, w, fpath, metric)
                    row[f"{prefix}_slope_{suffix}"] = ols_slope(series)

            # td_history_points / td_history_missing (contract 1.6): how
            # much valid trailing history actually fed the slopes above,
            # per window. A slope of 0.0 and a slope computed from a full
            # window look identical unless you also carry this -- missing
            # is not the same as flat.
            for suffix, w in WINDOWS.items():
                series = window_series(weekly, week_labels, i, w, fpath, "DD")
                n_points = 0 if series is None else sum(1 for v in series if v is not None)
                row[f"td_history_points_{suffix}"] = n_points
                row[f"td_history_missing_{suffix}"] = series is None or n_points < w

            rows.append(row)

        if rows:
            with open(OUTPUT_DIR / f"{label}.csv", "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)

        print(f"{label}.csv done")


if __name__ == "__main__":
    main()
