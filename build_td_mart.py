#!/usr/bin/env python3
"""
Assembles the TD-regression mart: one row per (repository_id,
artifact_id, t0), with THREE regression targets instead of the old
NPF-90 binary label:

  TD_3w   TD_Principal at T0 + 3 weeks
  TD_1m   TD_Principal at T0 + 4 weeks  (closest weekly-cadence approx. of "1 month")
  TD_3m   TD_Principal at T0 + 13 weeks (closest weekly-cadence approx. of "3 months")

TD_Principal = bug_remediation_effort + vulnerability_remediation_effort
             + code_smell_remediation_effort (minutes)
             == the sum of remediation effort across every ACTIVE
                SonarQube issue on the file at that snapshot -- every
                issue is exactly one of those 3 types, so this is the
                same quantity as "total technical debt" at that point
                in time. TD_Principal_t0 (the T0-side value) is a
                FEATURE (already in the B2 section, as
                technical_debt_minutes_t0); TD_3w/TD_1m/TD_3m are the
                three LABELS.

Run this after scan_weekly_snapshots.py, compute_sonar_trend_features.py,
compute_finding_lifecycle.py, compute_git_window_features.py, and
compute_lineage.py have all been run for the same start_date/branch/weeks.

Everything in the B0/B1/B2/B3 feature sections below is UNCHANGED from
build_origin_mart.py -- git-derived and Sonar-derived FEATURES don't
care what the target is. Only section 1.7 (label construction) and the
per-horizon eligibility gates in 1.3 are new.

Each horizon is independently gated -- a row can have a real TD_3w
while TD_3m is still UNKNOWN_OBSERVATION (you haven't scanned that far
yet), especially for T0 weeks near the end of your scanned range.
Never impute a missing target: an UNKNOWN status means "don't use this
row for that target," not "assume 0 debt."

KNOWN SIMPLIFICATIONS (same MVP caveats as build_origin_mart.py):
  - identity_ambiguous_count / identity_excluded_count are always 0.
  - quality_profile_hash/version compatibility is checked exactly
    between T0 and each horizon; it does NOT check against any
    contract PDF's frozen pins.
  - split_family is fixed to CHRONOLOGICAL_POOLED per repo. If you're
    running this across multiple repos via run_all_repos.py, repo-level
    holdout (REPOSITORY_HELD_OUT) is applied afterward by
    combine_repo_marts.py, not here.
  - file_age_days inherits compute_git_window_features.py's history-depth
    caveat.

Writes td_mart.csv to the working directory.

Usage:
    python3 build_td_mart.py <start_date> [branch] [weeks]
"""
import csv
import hashlib
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import config
from weekly_commits import resolve_weekly_commits

DATA_DIR = Path("data")

_COMMIT_DATE_CACHE = {}


def get_commit_date(repo_path, commit_hash):
    """
    Returns the commit's actual authored/committed date (ISO 8601),
    fetched directly from git -- NOT from Sonar's analysis["date"].

    Sonar stamps every analysis with the wall-clock time the SCAN was
    RUN, not the date of the code being analyzed. Since every week's
    scan for a historical repo typically gets run on the same real-world
    day (or a few consecutive days), using snap_t0["analysis_date"] for
    the "t0" column collapses every row onto ~today's date regardless
    of which historical week it represents -- silently breaking any
    chronological ordering/analysis downstream. The actual commit date
    is available straight from git for any commit hash, independent of
    what's currently checked out, so this doesn't require re-scanning.
    """
    if commit_hash not in _COMMIT_DATE_CACHE:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "show", "-s", "--format=%cI", commit_hash],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Could not look up commit date for {commit_hash}:\n{result.stderr}"
            )
        _COMMIT_DATE_CACHE[commit_hash] = result.stdout.strip()
    return _COMMIT_DATE_CACHE[commit_hash]
ACTIVE_STATUSES = {"OPEN", "CONFIRMED", "REOPENED"}
SEVERITY_COLS = {
    "BLOCKER": "blocker_finding_count_t0", "CRITICAL": "critical_finding_count_t0",
    "MAJOR": "major_finding_count_t0", "MINOR": "minor_finding_count_t0",
    "INFO": "info_finding_count_t0",
}
TYPE_COLS = {
    "CODE_SMELL": "code_smell_count_t0", "BUG": "bug_count_t0",
    "VULNERABILITY": "vulnerability_count_t0",
}
# (horizon name, weeks offset from T0) -- closest weekly-cadence approximation
# of "3 weeks" / "1 month" / "3 months".
HORIZONS = [("3w", 3), ("1m", 4), ("3m", 13)]


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


def to_bool(s):
    return str(s).strip().lower() == "true"


def load_csv_dict(path, key_col):
    if not Path(path).exists():
        return {}
    with open(path) as f:
        return {r[key_col]: r for r in csv.DictReader(f)}


def load_issues(label, active_only=True):
    path = DATA_DIR / f"issues_{label}.json"
    if not path.exists():
        return []
    with open(path) as f:
        issues = json.load(f)
    if active_only:
        issues = [i for i in issues if i["status"] in ACTIVE_STATUSES]
    return issues


def debt_breakdown_by_file(issues):
    """{file_path: {"bug": min, "vulnerability": min, "code_smell": min, "total": min}}"""
    out = {}
    key_by_type = {"BUG": "bug", "VULNERABILITY": "vulnerability", "CODE_SMELL": "code_smell"}
    for issue in issues:
        fp = issue["component"].split(":", 1)[-1]
        minutes = parse_minutes(issue["effort_minutes"])
        typ_key = key_by_type.get(issue.get("type"))
        out.setdefault(fp, {"bug": 0, "vulnerability": 0, "code_smell": 0, "total": 0})
        out[fp]["total"] += minutes
        if typ_key:
            out[fp][typ_key] += minutes
    return out


def main():
    if len(sys.argv) not in (2, 3, 4, 5):
        print(__doc__)
        sys.exit(1)
    start_date = sys.argv[1]
    branch = sys.argv[2] if len(sys.argv) >= 3 else "trunk"
    max_weeks = int(sys.argv[3]) if len(sys.argv) >= 4 else 26
    g3_dir = Path(sys.argv[4]) if len(sys.argv) == 5 else Path("weekly_g3_features")

    weeks = resolve_weekly_commits(config.KAFKA_REPO_PATH, start_date, branch, max_weeks=max_weeks)
    weeks = {w: b for w, b in weeks.items() if b["commit"]}
    week_nums = sorted(weeks.keys())

    snapshots = load_csv_dict(DATA_DIR / "snapshots.csv", "label")
    analysis_meta = {}
    meta_path = DATA_DIR / "analysis_metadata.csv"
    if meta_path.exists():
        with open(meta_path) as f:
            for r in csv.DictReader(f):
                analysis_meta[r["label"]] = r  # last row per label wins

    max_offset = max(off for _, off in HORIZONS)
    valid_t0_weeks = [w for w in week_nums if any((w + off) in weeks for _, off in HORIZONS)]
    if not valid_t0_weeks:
        print(f"No T0 weeks have ANY horizon available yet (need at least "
              f"T0 + {HORIZONS[0][1]} weeks) -- scan more weeks.")
        return

    all_rows = []
    for t0_w in valid_t0_weeks:
        t0_label = f"week_{t0_w:02d}"
        snap_t0 = snapshots.get(t0_label)
        if not snap_t0:
            print(f">>> Skipping {t0_label}: missing data/snapshots.csv row.")
            continue
        meta_t0 = analysis_meta.get(t0_label, {})
        t0_date = get_commit_date(config.KAFKA_REPO_PATH, snap_t0["commit_hash"])

        measures_t0 = load_csv_dict(DATA_DIR / f"measures_{t0_label}.csv", "file_path")
        lineage_t0 = load_csv_dict(f"lineage/{t0_label}.csv", "artifact_id")
        git_win = load_csv_dict(f"git_window_features/{t0_label}.csv", "artifact_id")
        sonar_trend = load_csv_dict(f"sonar_trends/{t0_label}.csv", "artifact_id")
        lifecycle = load_csv_dict(f"finding_lifecycle/{t0_label}.csv", "artifact_id")
        g3 = load_csv_dict(g3_dir / f"{t0_label}.csv", "artifact_id")

        t0_issues = load_issues(t0_label, active_only=True)
        debt_t0 = debt_breakdown_by_file(t0_issues)
        findings_by_file, severity_by_file, type_by_file = {}, {}, {}
        for issue in t0_issues:
            fp = issue["component"].split(":", 1)[-1]
            findings_by_file[fp] = findings_by_file.get(fp, 0) + 1
            sev, typ = issue.get("severity"), issue.get("type")
            if sev in SEVERITY_COLS:
                severity_by_file.setdefault(fp, {})
                severity_by_file[fp][sev] = severity_by_file[fp].get(sev, 0) + 1
            if typ in TYPE_COLS:
                type_by_file.setdefault(fp, {})
                type_by_file[fp][typ] = type_by_file[fp].get(typ, 0) + 1

        # --- per-horizon data: snapshot, metadata, lineage, debt ------------
        horizon_data = {}
        for name, off in HORIZONS:
            hw = t0_w + off
            if hw not in weeks:
                horizon_data[name] = None
                continue
            h_label = f"week_{hw:02d}"
            snap_h = snapshots.get(h_label)
            if not snap_h:
                horizon_data[name] = None
                continue
            meta_h = analysis_meta.get(h_label, {})
            measures_h = load_csv_dict(DATA_DIR / f"measures_{h_label}.csv", "file_path")
            lineage_h_rows = load_csv_dict(f"lineage/{h_label}.csv", "artifact_id")
            lineage_h_by_id = {r["lineage_id"]: fp for fp, r in lineage_h_rows.items()}
            debt_h = debt_breakdown_by_file(load_issues(h_label, active_only=True))

            analyzer_compatible = (
                meta_t0.get("sonarqube_version") == meta_h.get("sonarqube_version")
                and meta_t0.get("java_analyzer_version") == meta_h.get("java_analyzer_version")
                and meta_t0.get("quality_profile_hash") == meta_h.get("quality_profile_hash")
                and snap_t0.get("analysis_mode") == snap_h.get("analysis_mode")
                and bool(meta_t0.get("sonarqube_version"))
            )
            horizon_data[name] = {
                "label": h_label, "snap": snap_h,
                "measures": measures_h, "lineage_by_id": lineage_h_by_id,
                "debt": debt_h, "analyzer_compatible": analyzer_compatible,
            }

        for fpath, m in measures_t0.items():
            row = {}
            ncloc = int(m.get("ncloc") or 0)

            lin = lineage_t0.get(fpath)
            lineage_id = lin["lineage_id"] if lin else hashlib.sha1(fpath.encode()).hexdigest()[:16]
            lineage_conf = lin["lineage_confidence"] if lin else "UNSUPPORTED"

            # --- 1.1 identity/snapshot ---------------------------------
            row["origin_id"] = hashlib.sha1(
                f"{config.REPOSITORY_ID}:{fpath}:{t0_date}".encode()
            ).hexdigest()[:20]
            row["repository_id"] = config.REPOSITORY_ID
            row["artifact_id"] = fpath
            row["lineage_id"] = lineage_id
            row["language"] = m.get("language") or config.LANGUAGE
            row["path_at_t0"] = fpath
            row["t0"] = t0_date
            row["commit_hash_t0"] = snap_t0["commit_hash"]
            row["sonar_analysis_key_t0"] = snap_t0["analysis_key"]
            row["lineage_confidence"] = lineage_conf

            # --- 1.2 frozen analyzer-contract ---------------------------
            row["sonarqube_version"] = meta_t0.get("sonarqube_version", "")
            row["java_analyzer_version"] = meta_t0.get("java_analyzer_version", "")
            row["quality_profile_id"] = meta_t0.get("quality_profile_id", "")
            row["quality_profile_hash"] = meta_t0.get("quality_profile_hash", "")
            row["analysis_mode"] = snap_t0.get("analysis_mode", "")
            row["compatible_rule_domain_version"] = meta_t0.get(
                "compatible_rule_domain_version", config.COMPATIBLE_RULE_DOMAIN_VERSION
            )

            # --- 1.3 T0-side eligibility (horizon-independent) -----------
            production_source_file = fpath.endswith(".java") and not any(
                pat in f"/{fpath}" for pat in config.NON_PRODUCTION_PATH_PATTERNS
            )
            gw = git_win.get(fpath, {})
            st = sonar_trend.get(fpath, {})
            git_history_missing = to_bool(gw.get("git_history_missing", "True"))
            td_history_missing_6m = to_bool(st.get("td_history_missing_6m", "True"))
            history_complete = (not git_history_missing) and (not td_history_missing_6m) and bool(gw)

            t0_reasons = []
            if not production_source_file:
                t0_reasons.append("not_production_source_file")
            if not history_complete:
                t0_reasons.append("history_incomplete")
            eligible_t0 = len(t0_reasons) == 0

            row["production_source_file"] = production_source_file
            row["sonar_available_t0"] = True
            row["history_complete"] = history_complete
            row["eligible_t0"] = eligible_t0
            row["exclusion_reason_t0"] = ";".join(t0_reasons)

            # --- B0 / B1 (git features, unchanged) --------------------------
            row["ncloc_t0"] = ncloc
            row["file_age_days"] = gw.get("file_age_days", "")
            for suf in ("30d", "90d", "180d"):
                row[f"commits_{suf}"] = gw.get(f"commits_{suf}", "")
                row[f"lines_added_{suf}"] = gw.get(f"lines_added_{suf}", "")
                row[f"lines_deleted_{suf}"] = gw.get(f"lines_deleted_{suf}", "")
                row[f"churn_{suf}"] = gw.get(f"churn_{suf}", "")
                row[f"authors_{suf}"] = gw.get(f"authors_{suf}", "")
            row["days_since_last_change"] = gw.get("days_since_last_change", "")
            row["commit_acceleration"] = gw.get("commit_acceleration", "")
            row["churn_acceleration"] = gw.get("churn_acceleration", "")
            row["commit_burstiness_180d"] = gw.get("commit_burstiness_180d", "")
            row["churn_burstiness_180d"] = gw.get("churn_burstiness_180d", "")
            row["git_history_missing"] = git_history_missing
            row["last_change_missing"] = to_bool(gw.get("last_change_missing", "True"))

            # --- g3 / total_line features (pipeline_total_line.py + pipeline_g3.py) --
            # Kept alongside B1 rather than replacing it -- same underlying
            # concepts (churn, commit acceleration, burstiness) but computed
            # with the older 3-week-bucket method instead of B1's exact
            # calendar-day windows, hence the _g3 suffix to avoid colliding
            # with commit_acceleration/churn_acceleration/*_burstiness_180d above.
            g3_row = g3.get(fpath, {})
            row["total_lines_g3"] = g3_row.get("total_lines", "")
            row["commits_g3"] = g3_row.get("commits", "")
            row["churn_g3"] = g3_row.get("churn", "")
            row["churn_rate_g3"] = g3_row.get("churn_rate", "")
            row["commit_acceleration_g3"] = g3_row.get("commit_acceleration", "")
            row["churn_acceleration_g3"] = g3_row.get("churn_acceleration", "")
            row["commit_burstiness_g3"] = g3_row.get("commit_burstiness", "")
            row["churn_burstiness_g3"] = g3_row.get("churn_burstiness", "")

            # --- B2 (Sonar T0 snapshot features, unchanged) ------------------
            debt_row = debt_t0.get(fpath, {"bug": 0, "vulnerability": 0, "code_smell": 0, "total": 0})
            finding_count_t0 = findings_by_file.get(fpath, 0)
            row["finding_count_t0"] = finding_count_t0
            row["finding_density_t0"] = round(1000 * finding_count_t0 / ncloc, 4) if ncloc else ""
            row["bug_remediation_effort_t0"] = debt_row["bug"]
            row["vulnerability_remediation_effort_t0"] = debt_row["vulnerability"]
            row["code_smell_remediation_effort_t0"] = debt_row["code_smell"]
            row["technical_debt_minutes_t0"] = debt_row["total"]  # == TD_Principal_t0
            row["technical_debt_density_t0"] = round(1000 * debt_row["total"] / ncloc, 4) if ncloc else ""
            for sev, col in SEVERITY_COLS.items():
                row[col] = severity_by_file.get(fpath, {}).get(sev, 0)
            for typ, col in TYPE_COLS.items():
                row[col] = type_by_file.get(fpath, {}).get(typ, 0)
            row["complexity_t0"] = m.get("complexity", "")
            row["cognitive_complexity_t0"] = m.get("cognitive_complexity", "")
            row["duplicated_lines_density_t0"] = m.get("duplicated_lines_density", "")
            row["comment_lines_density_t0"] = m.get("comment_lines_density", "")

            # --- B3 (Sonar trend + finding lifecycle, unchanged) --------------
            for suf in ("3w", "1m", "3m", "6m"):
                row[f"finding_density_slope_{suf}"] = st.get(f"finding_density_slope_{suf}", "")
                row[f"finding_density_volatility_{suf}"] = st.get(f"finding_density_volatility_{suf}", "")
                row[f"technical_debt_density_slope_{suf}"] = st.get(f"technical_debt_density_slope_{suf}", "")
                row[f"technical_debt_density_volatility_{suf}"] = st.get(f"technical_debt_density_volatility_{suf}", "")
                row[f"complexity_slope_{suf}"] = st.get(f"complexity_slope_{suf}", "")
                row[f"cognitive_complexity_slope_{suf}"] = st.get(f"cognitive_complexity_slope_{suf}", "")
                row[f"duplication_slope_{suf}"] = st.get(f"duplication_slope_{suf}", "")
                row[f"td_history_points_{suf}"] = st.get(f"td_history_points_{suf}", "")
                row[f"td_history_missing_{suf}"] = st.get(f"td_history_missing_{suf}", "")
            lc = lifecycle.get(fpath, {})
            for suf in ("3w", "1m", "3m", "6m", "30d", "90d", "180d"):
                row[f"finding_arrivals_{suf}"] = lc.get(f"finding_arrivals_{suf}", "")
                row[f"finding_closures_{suf}"] = lc.get(f"finding_closures_{suf}", "")

            # --- 1.7 TD-regression targets, one per horizon -------------------
            for name, off in HORIZONS:
                hd = horizon_data.get(name)
                path_at_h = hd["lineage_by_id"].get(lineage_id, "") if hd else ""
                sonar_available_h = bool(hd) and bool(path_at_h) and path_at_h in hd["measures"]
                lineage_ok = lineage_conf in ("HIGH", "MEDIUM")
                analyzer_ok = bool(hd) and hd["analyzer_compatible"]

                h_reasons = []
                if not eligible_t0:
                    h_reasons.append("t0_not_eligible")
                if hd is None:
                    h_reasons.append("horizon_not_scanned_yet")
                elif not sonar_available_h:
                    h_reasons.append("sonar_unavailable_at_horizon")
                if not lineage_ok:
                    h_reasons.append("lineage_unsupported_or_ambiguous")
                if hd is not None and not analyzer_ok:
                    h_reasons.append("analyzer_incompatible_t0_horizon")

                observed = len(h_reasons) == 0
                row[f"path_at_{name}"] = path_at_h
                if observed:
                    debt_h_row = hd["debt"].get(path_at_h, {"bug": 0, "vulnerability": 0, "code_smell": 0, "total": 0})
                    row[f"bug_remediation_effort_{name}"] = debt_h_row["bug"]
                    row[f"vulnerability_remediation_effort_{name}"] = debt_h_row["vulnerability"]
                    row[f"code_smell_remediation_effort_{name}"] = debt_h_row["code_smell"]
                    row[f"TD_{name}"] = debt_h_row["total"]
                    row[f"TD_{name}_status"] = "OBSERVED"
                else:
                    row[f"bug_remediation_effort_{name}"] = ""
                    row[f"vulnerability_remediation_effort_{name}"] = ""
                    row[f"code_smell_remediation_effort_{name}"] = ""
                    row[f"TD_{name}"] = "UNKNOWN"
                    if "horizon_not_scanned_yet" in h_reasons or "sonar_unavailable_at_horizon" in h_reasons:
                        row[f"TD_{name}_status"] = "UNKNOWN_OBSERVATION"
                    elif "lineage_unsupported_or_ambiguous" in h_reasons:
                        row[f"TD_{name}_status"] = "UNKNOWN_LINEAGE"
                    else:
                        row[f"TD_{name}_status"] = "EXCLUDED"
                row[f"TD_{name}_exclusion_reason"] = ";".join(h_reasons)

            row["identity_ambiguous_count"] = 0  # see module docstring
            row["identity_excluded_count"] = 0
            row["identity_policy_version"] = "trust-sonar-issue-identity-v1"

            # --- 1.8 leakage/split -----------------------------------------
            row["max_git_feature_ts"] = t0_date
            row["max_sonar_feature_ts"] = t0_date
            row["max_feature_ts"] = t0_date
            row["split_family"] = "CHRONOLOGICAL_POOLED"
            row["held_repository"] = ""
            row["fold_id"] = ""  # filled below
            row["partition"] = ""  # filled below
            row["_t0_week"] = t0_w  # internal, stripped before writing

            all_rows.append(row)

        print(f">>> {t0_label}: {len(measures_t0)} origin rows built")

    # --- chronological split assignment across ALL T0 weeks -----------------
    # Purge at least the LONGEST horizon so no train row's target window
    # overlaps a test row's T0.
    purge_weeks = max_offset
    n = len(valid_t0_weeks)
    train_end = valid_t0_weeks[int(n * 0.6) - 1] if n >= 5 else valid_t0_weeks[0]
    test_start_idx = int(n * 0.8)
    test_start = valid_t0_weeks[test_start_idx] if test_start_idx < n else valid_t0_weeks[-1]
    for row in all_rows:
        w = row.pop("_t0_week")
        if w <= train_end:
            partition = "TRAIN"
        elif w < train_end + purge_weeks:
            partition = "PURGED"
        elif w < test_start:
            partition = "VALIDATION"
        elif w < test_start + purge_weeks:
            partition = "PURGED"
        else:
            partition = "TEST"
        row["partition"] = partition
        row["fold_id"] = f"chrono_{partition.lower()}"

    if not all_rows:
        print("No origin rows produced.")
        return

    fieldnames = list(all_rows[0].keys())
    with open("td_mart.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f">>> Wrote {len(all_rows)} rows ({len(fieldnames)} columns) to td_mart.csv")
    for name, _ in HORIZONS:
        n_obs = sum(1 for r in all_rows if r[f"TD_{name}_status"] == "OBSERVED")
        print(f">>>   TD_{name}: {n_obs} observed / {len(all_rows)} total")
    print(">>> Remember: TD_3w/TD_1m/TD_3m/TD_*_status/path_at_* are label-only "
          "-- never put them in your feature matrix X. Filter each target's own "
          "TD_*_status == 'OBSERVED' independently -- don't drop a row just "
          "because ONE horizon is still UNKNOWN.")


if __name__ == "__main__":
    main()