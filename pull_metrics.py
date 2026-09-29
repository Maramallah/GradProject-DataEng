#!/usr/bin/env python3
"""
Pulls per-file measures for the CURRENTLY analyzed snapshot and saves
them to data/measures_<label>.csv

Run this right after scan_snapshot.py, for the SAME label, while that
commit's analysis is still the latest one in SonarQube.

Pulls every metric in METRICS below in one paginated pass over
/api/measures/component_tree. Not every metric applies to every project
(e.g. coverage/test metrics are blank on a repo with no test execution
data loaded, security_hotspots-related metrics depend on edition) --
those columns just come back empty rather than erroring, so this list
is intentionally broad: better to have a blank column than to have
under-pulled data you have to re-scan for later.

Usage:
    python3 pull_metrics.py <snapshot_label>
"""
import csv
import os
import sys

import requests

import config

METRICS = ",".join([
    # size / structure
    "ncloc",
    "lines",
    "statements",
    "functions",
    "classes",

    # complexity
    "complexity",
    "cognitive_complexity",

    # duplication
    "duplicated_lines",
    "duplicated_lines_density",
    "duplicated_blocks",
    "duplicated_files",

    # documentation
    "comment_lines",
    "comment_lines_density",

    # issues
    "violations",
    "code_smells",
    "bugs",
    "vulnerabilities",

    # ratings
    "reliability_rating",
    "security_rating",
    "sqale_rating",

    # technical debt / maintainability
    "sqale_index",
    "sqale_debt_ratio",

    # coverage
    "coverage",
    "line_coverage",
    "branch_coverage",
    "uncovered_lines",
    "uncovered_conditions",
    "lines_to_cover",
    "conditions_to_cover",

    # tests
    "tests",
    "test_errors",
    "test_failures",

    # SCM
    "last_commit_date",
])


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    label = sys.argv[1]

    os.makedirs("data", exist_ok=True)
    out_path = f"data/measures_{label}.csv"

    rows = []
    page = 1
    while True:
        resp = requests.get(
            f"{config.SONAR_URL}/api/measures/component_tree",
            params={
                "component": config.PROJECT_KEY,
                "metricKeys": METRICS,
                "qualifiers": "FIL",
                "ps": 500,
                "p": page,
            },
            auth=(config.SONAR_TOKEN, ""),
        )
        resp.raise_for_status()
        data = resp.json()
        components = data.get("components", [])
        if not components:
            break

        for c in components:
            row = {"file_path": c["path"], "language": c.get("language", "")}
            for m in c.get("measures", []):
                row[m["metric"]] = m.get("value", "")
            rows.append(row)

        if len(components) < 500:
            break
        page += 1

    fieldnames = ["file_path", "language"] + METRICS.split(",")
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    print(f">>> Saved {len(rows)} files' measures ({len(fieldnames)} columns) to {out_path}")


if __name__ == "__main__":
    main()
