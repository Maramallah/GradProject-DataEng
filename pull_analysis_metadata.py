#!/usr/bin/env python3
"""
Pulls the "frozen analyzer-contract" columns (data contract section 1.2)
for the CURRENTLY analyzed snapshot and appends a row to
data/analysis_metadata.csv:

  label, sonarqube_version, java_analyzer_version, quality_profile_id,
  quality_profile_hash, analysis_mode, compatible_rule_domain_version

NPF-90 is only meaningful when T0 and T90 used a compatible analyzer and
rule set -- this is what build_origin_mart.py's analyzer-compatibility
gate checks before it will build a labeled row.

quality_profile_hash is NOT a Sonar-provided value: SonarQube doesn't
expose one directly, so this computes a stable sha256 over the sorted
list of (rule key, severity, params) for every ACTIVE rule in the
project's Java quality profile. Two snapshots hash equal only if their
active rule set is byte-for-byte identical -- exactly what the contract
needs to gate on, even though the number itself isn't a Sonar concept.

Run this right after scan_snapshot.py, for the SAME label.

Usage:
    python3 pull_analysis_metadata.py <snapshot_label>
"""
import csv
import hashlib
import json
import os
import sys

import requests

import config


def get_server_version():
    resp = requests.get(f"{config.SONAR_URL}/api/server/version", auth=(config.SONAR_TOKEN, ""))
    resp.raise_for_status()
    return resp.text.strip()


def get_java_analyzer_version():
    resp = requests.get(
        f"{config.SONAR_URL}/api/plugins/installed", auth=(config.SONAR_TOKEN, "")
    )
    resp.raise_for_status()
    for plugin in resp.json().get("plugins", []):
        if plugin.get("key", "").lower() == "java":
            return f"{plugin.get('version', '')} build {plugin.get('build', '')}".strip()
    return ""


def get_java_quality_profile(project_key):
    """Returns (profile_key, profile_name) for this project's active Java profile."""
    resp = requests.get(
        f"{config.SONAR_URL}/api/qualityprofiles/search",
        params={"project": project_key},
        auth=(config.SONAR_TOKEN, ""),
    )
    resp.raise_for_status()
    for profile in resp.json().get("profiles", []):
        if profile.get("language") == "java":
            return profile["key"], profile["name"]
    return "", ""


def compute_profile_hash(profile_key):
    """sha256 over every active rule's (key, severity, params), sorted --
    deterministic regardless of API pagination order."""
    if not profile_key:
        return ""
    rules = []
    page = 1
    while True:
        resp = requests.get(
            f"{config.SONAR_URL}/api/rules/search",
            params={"activation": "true", "qprofile": profile_key, "ps": 500, "p": page},
            auth=(config.SONAR_TOKEN, ""),
        )
        resp.raise_for_status()
        data = resp.json()
        page_rules = data.get("rules", [])
        if not page_rules:
            break
        for r in page_rules:
            actives = data.get("actives", {}).get(r["key"], [])
            severity = actives[0].get("severity", "") if actives else ""
            params = sorted(
                (p.get("key", ""), p.get("value", ""))
                for a in actives
                for p in a.get("params", [])
            )
            rules.append((r["key"], severity, params))
        if len(page_rules) < 500:
            break
        page += 1

    rules.sort(key=lambda t: t[0])
    payload = json.dumps(rules, sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def get_analysis_mode(label):
    """Reads back what scan_snapshot.py stamped into data/snapshots.csv."""
    with open("data/snapshots.csv") as f:
        for row in csv.DictReader(f):
            if row["label"] == label:
                return row.get("analysis_mode", "")
    return ""


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    label = sys.argv[1]

    os.makedirs("data", exist_ok=True)
    out_path = "data/analysis_metadata.csv"

    print(">>> Fetching server/analyzer/quality-profile metadata...")
    sonarqube_version = get_server_version()
    java_analyzer_version = get_java_analyzer_version()
    profile_key, profile_name = get_java_quality_profile(config.PROJECT_KEY)
    profile_hash = compute_profile_hash(profile_key)
    analysis_mode = get_analysis_mode(label)

    row = {
        "label": label,
        "sonarqube_version": sonarqube_version,
        "java_analyzer_version": java_analyzer_version,
        "quality_profile_id": profile_name or profile_key,
        "quality_profile_hash": profile_hash,
        "analysis_mode": analysis_mode,
        "compatible_rule_domain_version": config.COMPATIBLE_RULE_DOMAIN_VERSION,
    }

    is_new = not os.path.exists(out_path)
    with open(out_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if is_new:
            writer.writeheader()
        writer.writerow(row)

    print(f">>> {label}: sonar={sonarqube_version} java_analyzer={java_analyzer_version} "
          f"profile={row['quality_profile_id']} mode={analysis_mode}")
    print(f">>> Appended to {out_path}")


if __name__ == "__main__":
    main()
