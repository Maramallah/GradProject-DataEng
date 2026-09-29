#!/usr/bin/env python3
"""
Checks out one Kafka commit, runs a SonarQube scan against it, and
records the resulting analysis in data/snapshots.csv.

Usage:
    python3 scan_snapshot.py <commit_hash> <snapshot_label>

Example:
    python3 scan_snapshot.py 3f2a1b9 t0

Run this once per snapshot, OLDEST COMMIT FIRST, per README.md step 3.
"""
import csv
import os
import subprocess
import sys
import time

import requests

import config


def run(cmd, cwd=None):
    print(f">>> {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, check=True)


def write_sonar_properties(repo_path, project_key, version_label):
    """
    Writes a sonar-project.properties scoped to Kafka's production Java
    code: excludes tests, generated sources, build output, and Scala
    files (the NPF-90 contract's language scope is JAVA only).

    If config.JAVA_BINARIES_PATH is set, adds sonar.java.binaries so the
    analyzer runs in COMPILED mode instead of SOURCE_ONLY -- some Java
    rules only fire with bytecode available, so mixing modes across
    snapshots would make T0/T90 findings incomparable. analysis_mode is
    recorded in data/snapshots.csv below so that can be checked later.
    """
    content = f"""sonar.projectKey={project_key}
sonar.projectVersion={version_label}
sonar.sources=.
sonar.exclusions=**/test/**,**/generated/**,**/*.scala,**/build/**,**/vendor/**
"""
    if config.JAVA_BINARIES_PATH:
        content += f"sonar.java.binaries={config.JAVA_BINARIES_PATH}\n"
    with open(os.path.join(repo_path, "sonar-project.properties"), "w") as f:
        f.write(content)


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)

    commit_hash, label = sys.argv[1], sys.argv[2]
    repo = config.KAFKA_REPO_PATH

    print(f">>> Checking out Kafka at {commit_hash}")
    # Every commit this script ever checks out was already resolved from a
    # SINGLE full clone/fetch done once up front (by weekly_commits.py,
    # before any weekly scan starts) -- so the commit is already sitting
    # locally, and re-fetching from GitHub here on every single week (60x
    # per repo) is redundant network traffic that only adds 60 extra
    # chances to hit a dropped connection for zero benefit. Try the
    # checkout directly first; only fall back to a fetch (and retry once)
    # in the rare case the commit genuinely isn't available locally yet.
    try:
        run(["git", "checkout", commit_hash], cwd=repo)
    except subprocess.CalledProcessError:
        print(">>> Checkout failed -- commit not found locally, fetching and retrying once")
        run(["git", "fetch", "--all"], cwd=repo)
        run(["git", "checkout", commit_hash], cwd=repo)

    print(">>> Writing sonar-project.properties")
    write_sonar_properties(repo, config.PROJECT_KEY, label)

    print(">>> Running sonar-scanner via Docker (this can take a while on Kafka)")
    docker_cmd = ["docker", "run", "--rm"]
    if config.DOCKER_NETWORK:
        docker_cmd += ["--network", config.DOCKER_NETWORK]
    docker_cmd += [
        "-e", f"SONAR_HOST_URL={config.SONAR_URL_FROM_SCANNER}",
        "-e", f"SONAR_TOKEN={config.SONAR_TOKEN}",
        "-v", f"{repo}:/usr/src",
        "sonarsource/sonar-scanner-cli",
    ]
    run(docker_cmd)

    print(">>> Scan finished. Waiting for SonarQube to finish processing the report...")
    # Uploading the report and SonarQube actually processing it are two
    # separate steps -- large projects like Kafka can take a while to
    # process in the background, so we poll instead of guessing a fixed
    # delay.
    analysis = None
    for attempt in range(60):  # up to ~5 minutes (60 x 5s)
        resp = requests.get(
            f"{config.SONAR_URL}/api/project_analyses/search",
            params={"project": config.PROJECT_KEY, "ps": 1},
            auth=(config.SONAR_TOKEN, ""),
        )
        resp.raise_for_status()
        analyses = resp.json().get("analyses", [])
        if analyses:
            analysis = analyses[0]
            break
        print(f"    ...not ready yet, waiting (attempt {attempt + 1}/60)")
        time.sleep(5)

    if analysis is None:
        print(">>> ERROR: SonarQube never showed a completed analysis after 5 minutes.")
        print(">>> Check the SonarQube UI's background tasks page for errors:")
        print(f">>>   {config.SONAR_URL}/admin/background_tasks")
        sys.exit(1)

    analysis_mode = "COMPILED" if config.JAVA_BINARIES_PATH else "SOURCE_ONLY"

    os.makedirs("data", exist_ok=True)
    snapshot_file = "data/snapshots.csv"
    is_new = not os.path.exists(snapshot_file)
    with open(snapshot_file, "a", newline="") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow([
                "label", "commit_hash", "analysis_key", "analysis_date", "analysis_mode",
            ])
        writer.writerow([label, commit_hash, analysis["key"], analysis["date"], analysis_mode])

    print(f">>> Saved snapshot '{label}' -> {analysis['key']} ({analysis['date']}, {analysis_mode})")
    print(f">>> Next: python3 pull_metrics.py {label}")
    print(f">>> Then: python3 pull_issues.py {label}")
    print(f">>> Then: python3 pull_analysis_metadata.py {label}")


if __name__ == "__main__":
    main()