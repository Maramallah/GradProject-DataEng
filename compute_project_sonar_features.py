#!/usr/bin/env python3
"""
Project-level (repo-wide) SonarQube features, one row per week, pulled
directly from SonarQube's historical measures endpoint
(api/measures/search_history) instead of re-scanning anything -- every week
you already scanned with scan_weekly_snapshots.py created a real analysis,
so Sonar already has this whole time series stored server-side. This is the
main reason project-level is EASIER than file-level: file-level trend
features needed a full re-scan per week because Sonar's per-file measures
endpoint has no history; this project-level endpoint does.

Metrics pulled: ncloc, complexity, cognitive_complexity,
duplicated_lines_density, comment_lines, code_smells, bugs, vulnerabilities,
duplicated_blocks, uncovered_lines, sqale_index,
reliability_remediation_effort, security_remediation_effort.
(uncovered_lines will likely be 0/blank -- it needs test-coverage data,
which this pipeline never ran.)

  total_principal = sqale_index + reliability_remediation_effort +
                     security_remediation_effort   (MINUTES -- matches the
                     paper's target unit exactly, per
                     Explain_Technical_Debt_Forecasting.pdf)

Trend features (same OLS-slope / population-stdev formulas as the
file-level script, just on one project-wide series instead of one per
file):
  finding_density_slope_3m/6m         (code_smells+bugs+vulnerabilities)/KLOC
  technical_debt_density_slope_3m/6m  total_principal in HOURS / KLOC --
                                       hours here, a DIFFERENT unit than the
                                       minutes-based total_principal target.
                                       Don't mix the two.
  finding_density_volatility_6m
  technical_debt_density_volatility_6m
  complexity_slope_6m                 complexity / KLOC
  cognitive_complexity_slope_6m
  duplication_slope_6m                duplicated_lines_density (already a %)

Reads week labels + analysis dates from data/snapshots.csv (written by
scan_snapshot.py) to match each week to its exact historical measure entry.

Usage:
    python3 compute_project_sonar_features.py
"""
import csv
import statistics
from datetime import datetime
from pathlib import Path

import requests

import config

OUTPUT_FILE = Path("project_sonar_features.csv")
WINDOW_3M = 12
WINDOW_6M = 26

METRICS = [
    "ncloc", "complexity", "cognitive_complexity", "duplicated_lines_density",
    "comment_lines", "code_smells", "bugs", "vulnerabilities",
    "duplicated_blocks", "uncovered_lines", "sqale_index",
    "reliability_remediation_effort", "security_remediation_effort",
]


def load_snapshots():
    with open("data/snapshots.csv") as f:
        return list(csv.DictReader(f))


def fetch_measure_history():
    """Returns {metric: [(date_str, float_value), ...]}, paginated."""
    history = {m: [] for m in METRICS}
    page = 1
    while True:
        resp = requests.get(
            f"{config.SONAR_URL}/api/measures/search_history",
            params={
                "component": config.PROJECT_KEY,
                "metrics": ",".join(METRICS),
                "ps": 1000,
                "p": page,
            },
            auth=(config.SONAR_TOKEN, ""),
        )
        resp.raise_for_status()
        data = resp.json()
        for m in data.get("measures", []):
            for point in m.get("history", []):
                if "value" in point:
                    history[m["metric"]].append((point["date"], float(point["value"])))
        paging = data.get("paging", {})
        if page * paging.get("pageSize", 1000) >= paging.get("total", 0):
            break
        page += 1
    return history


def nearest_value(series, target_date):
    """series: [(date_str, value)]. Finds the entry closest to target_date (ISO string)."""
    if not series:
        return None
    target = datetime.fromisoformat(target_date.replace("Z", "+00:00"))
    best = min(
        series,
        key=lambda p: abs((datetime.fromisoformat(p[0].replace("Z", "+00:00")) - target).total_seconds()),
    )
    return best[1]


def ols_slope(values):
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


def main():
    snapshots = sorted(load_snapshots(), key=lambda r: r["analysis_date"])
    if not snapshots:
        print("No data/snapshots.csv rows found -- run scan_weekly_snapshots.py first.")
        return

    print(">>> Fetching project measure history from SonarQube")
    history = fetch_measure_history()

    weekly = []
    for snap in snapshots:
        label, analysis_date = snap["label"], snap["analysis_date"]
        m = {metric: nearest_value(history[metric], analysis_date) for metric in METRICS}

        ncloc = m["ncloc"] or 0
        kloc = ncloc / 1000 if ncloc else None
        findings = (m["code_smells"] or 0) + (m["bugs"] or 0) + (m["vulnerabilities"] or 0)
        total_principal = (
            (m["sqale_index"] or 0) + (m["reliability_remediation_effort"] or 0)
            + (m["security_remediation_effort"] or 0)
        )

        weekly.append({
            "label": label,
            "analysis_date": analysis_date,
            "measures": m,
            "total_principal": total_principal,  # minutes -- matches the paper's target unit
            "DD": (findings / kloc) if kloc else None,
            "TDD": ((total_principal / 60) / kloc) if kloc else None,  # hours-based trend feature
            "CC": (m["complexity"] / kloc) if (kloc and m["complexity"] is not None) else None,
            "CogC": (m["cognitive_complexity"] / kloc) if (kloc and m["cognitive_complexity"] is not None) else None,
            "DP": m["duplicated_lines_density"],
        })

    rows = []
    for i, wk in enumerate(weekly):
        row = {
            "week": wk["label"],
            "snapshot_date": wk["analysis_date"],
            "ncloc": wk["measures"]["ncloc"],
            "complexity": wk["measures"]["complexity"],
            "cognitive_complexity": wk["measures"]["cognitive_complexity"],
            "duplicated_lines_density": wk["measures"]["duplicated_lines_density"],
            "comment_lines": wk["measures"]["comment_lines"],
            "code_smells": wk["measures"]["code_smells"],
            "bugs": wk["measures"]["bugs"],
            "vulnerabilities": wk["measures"]["vulnerabilities"],
            "duplicated_blocks": wk["measures"]["duplicated_blocks"],
            "uncovered_lines": wk["measures"]["uncovered_lines"],
            "sqale_index": wk["measures"]["sqale_index"],
            "reliability_remediation_effort": wk["measures"]["reliability_remediation_effort"],
            "security_remediation_effort": wk["measures"]["security_remediation_effort"],
            "total_principal": wk["total_principal"],
        }

        for metric, prefix in [("DD", "finding_density"), ("TDD", "technical_debt_density")]:
            s3 = [weekly[j][metric] for j in range(max(0, i - WINDOW_3M + 1), i + 1)]
            s6 = [weekly[j][metric] for j in range(max(0, i - WINDOW_6M + 1), i + 1)]
            s3 = s3 if len(s3) == WINDOW_3M else None
            s6 = s6 if len(s6) == WINDOW_6M else None
            row[f"{prefix}_slope_3m"] = ols_slope(s3)
            row[f"{prefix}_slope_6m"] = ols_slope(s6)
            row[f"{prefix}_volatility_6m"] = population_stdev(s6)

        for metric, col in [
            ("CC", "complexity_slope_6m"),
            ("CogC", "cognitive_complexity_slope_6m"),
            ("DP", "duplication_slope_6m"),
        ]:
            s6 = [weekly[j][metric] for j in range(max(0, i - WINDOW_6M + 1), i + 1)]
            s6 = s6 if len(s6) == WINDOW_6M else None
            row[col] = ols_slope(s6)

        rows.append(row)

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f">>> Wrote {len(rows)} row(s) to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
