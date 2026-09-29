#!/usr/bin/env python3
"""
Pulls issues (findings) for the CURRENTLY analyzed snapshot and saves
them to data/issues_<label>.json

Run this right after scan_snapshot.py, for the SAME label, while that
commit's analysis is still the latest one in SonarQube.

Pulls ALL statuses, not just active ones (OPEN/CONFIRMED/REOPENED) --
RESOLVED and CLOSED issues are needed by compute_finding_lifecycle.py
to count closures, and by compute_label.py's T0->T90 comparison. Filter
down to active-only at read time (status in OPEN/CONFIRMED/REOPENED) if
a script specifically needs "currently active findings" -- most of the
existing scripts (compute_features.py, compute_sonar_trend_features.py)
already do that filtering internally where it matters.

Usage:
    python3 pull_issues.py <snapshot_label>
"""
import json
import os
import sys

import requests

import config

STATUSES = "OPEN,CONFIRMED,REOPENED,RESOLVED,CLOSED"
SEVERITIES = ["BLOCKER", "CRITICAL", "MAJOR", "MINOR", "INFO"]
TYPES = ["BUG", "VULNERABILITY", "CODE_SMELL"]

# SonarQube's api/issues/search hard-caps at 10,000 results per query
# (p * ps <= 10000), regardless of how you page through it. Large/old repos
# (e.g. OFBiz) can have more than 10,000 issues total across all statuses
# in a single snapshot, which trips this cap even with correct pagination.
MAX_RESULTS_PER_QUERY = 10000


def fetch_page(extra_params, page):
    params = {
        "componentKeys": config.PROJECT_KEY,
        "ps": 500,
        "p": page,
        "statuses": STATUSES,
    }
    params.update(extra_params)
    resp = requests.get(
        f"{config.SONAR_URL}/api/issues/search",
        params=params,
        auth=(config.SONAR_TOKEN, ""),
    )
    resp.raise_for_status()
    return resp.json()


def fetch_all(extra_params, remaining_dims):
    """Fetches every issue matching extra_params, paginating normally.
    If the total exceeds SonarQube's 10,000-result cap, splits further by
    the next available dimension (severities, then types) and recurses,
    so each individual query stays under the cap."""
    page = 1
    collected = []
    total = None
    while True:
        data = fetch_page(extra_params, page)
        total = data["total"]
        if total > MAX_RESULTS_PER_QUERY:
            if not remaining_dims:
                raise RuntimeError(
                    f"Still over {MAX_RESULTS_PER_QUERY} issues ({total}) for "
                    f"{extra_params} even after splitting by severity AND type "
                    f"-- needs a finer split (e.g. by date range or directory)."
                )
            dim_name, dim_values = remaining_dims[0]
            rest = remaining_dims[1:]
            print(f">>> {extra_params or '(all)'}: {total} issues, over the "
                  f"{MAX_RESULTS_PER_QUERY} cap -- splitting by {dim_name}")
            out = []
            for v in dim_values:
                sub_params = dict(extra_params)
                sub_params[dim_name] = v
                out.extend(fetch_all(sub_params, rest))
            return out

        issues = data.get("issues", [])
        if not issues:
            break
        collected.extend(issues)
        page += 1
        if len(collected) >= total:
            break
    return collected


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    label = sys.argv[1]

    os.makedirs("data", exist_ok=True)
    out_path = f"data/issues_{label}.json"

    raw_issues = fetch_all({}, [("severities", SEVERITIES), ("types", TYPES)])

    # De-dupe by issue key in case any overlap slipped through a split.
    seen = set()
    all_issues = []
    for i in raw_issues:
        if i["key"] in seen:
            continue
        seen.add(i["key"])
        impacts = i.get("impacts", [])  # SonarQube 10.x severity taxonomy
        all_issues.append(
            {
                "key": i["key"],
                "rule": i["rule"],
                "type": i.get("type"),  # BUG / VULNERABILITY / CODE_SMELL
                "severity": i.get("severity"),  # BLOCKER..INFO (legacy taxonomy)
                "impacts": [
                    {"softwareQuality": imp.get("softwareQuality"), "severity": imp.get("severity")}
                    for imp in impacts
                ],
                "component": i["component"],
                "line": i.get("line"),
                "effort_minutes": i.get("debt") or i.get("effort"),
                "creationDate": i["creationDate"],
                "updateDate": i.get("updateDate"),
                "closeDate": i.get("closeDate"),
                "status": i["status"],
                "resolution": i.get("resolution"),
                "tags": i.get("tags", []),
                "author": i.get("author"),
            }
        )

    with open(out_path, "w") as f:
        json.dump(all_issues, f, indent=2)

    print(f">>> Saved {len(all_issues)} issues (all statuses) to {out_path}")


if __name__ == "__main__":
    main()
