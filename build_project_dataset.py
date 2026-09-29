#!/usr/bin/env python3
"""
Builds the final project-level dataset: joins project_git_features.csv +
project_sonar_features.csv on week, adds lag/rolling features for
total_principal (its own history, safe to use as a predictor), and builds
future target columns target_<h>w = total_principal shifted h weeks ahead,
for each horizon in HORIZONS.

Per Explain_Technical_Debt_Forecasting.pdf section 7: total_principal
itself must NOT be used as an input feature at the same/future row -- only
its lagged/rolling history is safe. The target_<h>w columns are the FUTURE
values; never feed them back in as features for the same row.

Usage:
    python3 build_project_dataset.py
"""
import csv

HORIZONS = [1, 2, 4, 12]  # weeks ahead -- the set recommended for this project
LAG_WEEKS = 1
ROLLING_WEEKS = 4


def load(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def main():
    git = load("project_git_features.csv")
    sonar = load("project_sonar_features.csv")

    git_by_week = {r["week"]: r for r in git}
    sonar_by_week = {r["week"]: r for r in sonar}
    weeks = sorted(set(git_by_week) & set(sonar_by_week), key=lambda w: int(w.split("_")[1]))

    missing_git = set(sonar_by_week) - set(git_by_week)
    missing_sonar = set(git_by_week) - set(sonar_by_week)
    if missing_git:
        print(f">>> Warning: weeks in sonar features but not git features (dropped): {sorted(missing_git)}")
    if missing_sonar:
        print(f">>> Warning: weeks in git features but not sonar features (dropped): {sorted(missing_sonar)}")

    total_principal_series = [float(sonar_by_week[w]["total_principal"]) for w in weeks]

    rows = []
    for i, week in enumerate(weeks):
        row = {"week": week}
        row.update(git_by_week[week])
        row.update({k: v for k, v in sonar_by_week[week].items() if k not in row})

        # Lag / rolling features -- total_principal's own history, safe as predictors
        row["total_principal_lag_1w"] = (
            total_principal_series[i - LAG_WEEKS] if i - LAG_WEEKS >= 0 else ""
        )
        window = total_principal_series[max(0, i - ROLLING_WEEKS + 1): i + 1]
        row["total_principal_4w_mean"] = (
            round(sum(window) / len(window), 4) if len(window) == ROLLING_WEEKS else ""
        )

        # Future targets -- NEVER use these as features for the same row
        for h in HORIZONS:
            j = i + h
            row[f"target_{h}w"] = total_principal_series[j] if j < len(weeks) else ""

        rows.append(row)

    fieldnames = list(rows[0].keys())
    with open("project_dataset.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f">>> Wrote {len(rows)} row(s) to project_dataset.csv")
    for h in HORIZONS:
        usable = sum(1 for r in rows if r[f"target_{h}w"] != "")
        print(f">>>   usable rows for {h}-week horizon: {usable} / {len(rows)}")


if __name__ == "__main__":
    main()
