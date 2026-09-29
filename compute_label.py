#!/usr/bin/env python3
"""
Builds the NPF-90 label by comparing the T0 snapshot's analysis date to
the T0+90 snapshot's findings. Because SonarQube tracks issue identity
across analyses of the SAME project automatically, this MVP doesn't
need custom line-hash matching: any issue at T+90 whose creationDate is
AFTER the T0 analysis date is a new finding.

Every file that existed at T0 gets a labeled row -- including files
with ZERO issues at T90 (label_npf90 = 0 instead of the file silently
disappearing from the output). Files that show up at T90 but were NOT
part of the T0 file list (new files) are still written, but flagged
existed_at_t0=False so you can filter them out before training -- they
have no T0 features to predict from, so they aren't valid NPF-90 rows.

NOTE: this script is the raw finding-count component -- it does NOT
apply the contract's eligibility/lineage/analyzer-compatibility gates,
so it can still coerce an ambiguous case to label_npf90=0 (see the
"Complete negatives" constraint in the data contract). build_origin_mart.py
is the authoritative place the final label_npf90 / label_status columns
get produced for training, by combining this file's raw counts with
lineage confidence and the eligibility flags. Use this script's output
for quick weekly monitoring, not as the training label on its own.

Usage:
    python3 compute_label.py <t0_label> <t90_label>
"""
import csv
import json
import sys
from datetime import datetime

ACTIVE_STATUSES = {"OPEN", "CONFIRMED", "REOPENED"}


def load_t0_files(t0_label):
    """All files SonarQube measured at T0 -- this is our file universe."""
    with open(f"data/measures_{t0_label}.csv") as f:
        return {r["file_path"] for r in csv.DictReader(f)}


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    t0_label, t90_label = sys.argv[1], sys.argv[2]

    with open("data/snapshots.csv") as f:
        snaps = {r["label"]: r for r in csv.DictReader(f)}
    t0_date = datetime.fromisoformat(snaps[t0_label]["analysis_date"].replace("Z", "+00:00"))

    with open(f"data/issues_{t90_label}.json") as f:
        # active only -- endpoint_finding_count means "findings still
        # active at T90", not "every finding ever seen through T90"
        t90_issues = [i for i in json.load(f) if i["status"] in ACTIVE_STATUSES]

    t0_files = load_t0_files(t0_label)

    new_by_file, total_by_file = {}, {}
    for issue in t90_issues:
        fpath = issue["component"].split(":", 1)[-1]
        total_by_file[fpath] = total_by_file.get(fpath, 0) + 1
        created = datetime.fromisoformat(issue["creationDate"].replace("Z", "+00:00"))
        if created > t0_date:
            new_by_file[fpath] = new_by_file.get(fpath, 0) + 1

    # Union: every file that existed at T0, plus any file that shows up
    # in the T90 issues but wasn't part of the T0 file list.
    all_files = t0_files | set(total_by_file.keys())

    with open("data/npf90_labels.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "file_path", "endpoint_finding_count", "new_finding_count",
            "label_npf90", "existed_at_t0",
        ])
        for fpath in sorted(all_files):
            total = total_by_file.get(fpath, 0)
            new = new_by_file.get(fpath, 0)
            writer.writerow([fpath, total, new, 1 if new > 0 else 0, fpath in t0_files])

    n_clean = sum(1 for fp in all_files if total_by_file.get(fp, 0) == 0)
    n_new_files = sum(1 for fp in all_files if fp not in t0_files)
    print(f">>> Wrote labels for {len(all_files)} files to data/npf90_labels.csv")
    print(f">>>   {n_clean} had zero T90 issues (label_npf90 = 0)")
    print(f">>>   {n_new_files} did not exist at T0 (existed_at_t0 = False -- exclude these before training)")


if __name__ == "__main__":
    main()