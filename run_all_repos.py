#!/usr/bin/env python3
"""
Runs the full TD-regression pipeline across MULTIPLE repos, each given
as a git URL, and combines the results into one dataset.

Give it a CSV of repos (one row per repo):

    repository_id,git_url,branch,start_date,weeks
    kafka,https://github.com/apache/kafka.git,trunk,2011-08-01,60
    guava,https://github.com/google/guava.git,master,2015-01-01,60
    junit4,https://github.com/junit-team/junit4.git,main,2015-01-01,60

branch/start_date/weeks columns are optional per-row -- omit them to
fall back to --branch/--start-date/--weeks given on the command line.

For each repo, this:
  1. clones it (or fetches latest if already cloned) into
     <repos-dir>/<repository_id>/
  2. writes <output-dir>/<repository_id>/repo_context.json so the SAME
     config.py picks up this repo's KAFKA_REPO_PATH/PROJECT_KEY/
     REPOSITORY_ID without editing config.py per repo
  3. runs the whole pipeline with <output-dir>/<repository_id>/ as the
     working directory (scan -> git features -> Sonar features -> td_mart.csv)
  4. moves on to the next repo -- one repo failing does NOT stop the
     others (logged and skipped; see the summary at the end)

At the end, every repo's <output-dir>/<repository_id>/td_mart.csv gets
combined into <output-dir>/combined_td_mart.csv. Pass --holdout-repos to
mark entire repos as a held-out test set (REPOSITORY_HELD_OUT split)
instead of pooling their rows into the per-repo chronological split.

Usage:
    python3 run_all_repos.py <repos_csv> [--branch trunk] [--start-date 2015-01-01]
                              [--weeks 60] [--holdout-repos repoA,repoB]
                              [--skip-scan] [--resume-from <repository_id>]
                              [--repos-dir C:/repo_clones] [--output-dir G:/MyDrive/td_runs]

--repos-dir is where repos actually get git-cloned. KEEP THIS ON LOCAL
DISK, not a Drive-synced folder -- a git clone is tens of thousands of
small .git object files; syncing that continuously is slow, burns
Drive quota, and git itself gets slower inside a sync-shadowed folder.

--output-dir is where every repo's working directory (all the CSVs
this pipeline writes, and the final td_mart.csv / combined_td_mart.csv)
gets written. THIS is the one safe to point at a Drive-synced folder,
e.g. "G:/My Drive/td_runs" -- it's a modest number of CSV files, and
having them land directly in Drive is exactly what you want for backup
or sharing. Defaults to ./runs if not given.

Requires: config.py + every pipeline script (scan_weekly_snapshots.py,
pipeline_total_line.py, add_weekly_commits_churn.py, pipeline_g3.py,
compute_git_window_features.py, compute_ownership_entropy.py,
compute_lineage.py, compute_sonar_trend_features.py,
compute_finding_lifecycle.py, build_td_mart.py) sitting in the SAME
directory as this script -- they get invoked by absolute path so Python
resolves `import config` against THIS directory regardless of which
repo's working directory a given call runs in.
"""
import argparse
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

PYTHON = sys.executable
SCRIPT_DIR = Path(__file__).resolve().parent

TOTAL_LINES_DIR = "weekly_total_lines"
G3_DIR = "weekly_g3_features"


def here(script_name):
    return str(SCRIPT_DIR / script_name)


def step(title):
    print()
    print("=" * 70)
    print(f">>> {title}")
    print("=" * 70)


def run(cmd, cwd, allow_fail=False):
    print(f"$ (cwd={cwd}) {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd)
    if result.returncode != 0 and not allow_fail:
        raise RuntimeError(f"Step failed (exit {result.returncode}): {' '.join(cmd)}")
    return result.returncode


def normalize_git_url(url):
    """Strips tracking query strings/fragments (e.g. ?utm_source=...) that
    break git's smart-HTTP handshake, and adds .git for GitHub URLs that
    are missing it. Leaves local filesystem paths (no ://) untouched."""
    url = url.strip()
    if "://" not in url:
        return url
    parts = urlsplit(url)
    url = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    if "github.com" in url and not url.endswith(".git"):
        url = url.rstrip("/") + ".git"
    return url


def clone_or_update(git_url, repo_path):
    # Resolve to an ABSOLUTE path first. Previously this stayed relative
    # (e.g. "repo_clones/OFBiz") while also being cloned with cwd set to
    # its own parent ("repo_clones") -- that applies the relative segment
    # TWICE, so the repo actually lands at repo_clones/repo_clones/OFBiz
    # while repo_context.json (and every later step) looks for it one
    # level up and fails with "No such file or directory". Resolving to
    # an absolute path here makes the cwd argument irrelevant and fixes
    # that double-nesting for good.
    repo_path = Path(repo_path).resolve()
    if (repo_path / ".git").exists():
        print(f">>> {repo_path} already cloned -- fetching latest.")
        run(["git", "fetch", "--all"], cwd=str(repo_path))
    else:
        repo_path.parent.mkdir(parents=True, exist_ok=True)
        print(f">>> Cloning {git_url} -> {repo_path}")
        run(["git", "clone", git_url, str(repo_path)], cwd=str(repo_path.parent))


def load_repos(path):
    path = Path(path)
    if path.suffix.lower() in (".xlsx", ".xls"):
        try:
            import openpyxl
        except ImportError:
            print("!!! Reading .xlsx requires openpyxl: pip install openpyxl")
            sys.exit(1)
        wb = openpyxl.load_workbook(path, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        header = [str(h).strip() for h in rows[0]]
        out = []
        for r in rows[1:]:
            if all(c is None for c in r):
                continue
            d = {k: ("" if v is None else v) for k, v in zip(header, r)}
            for k in ("branch", "start_date", "weeks"):
                if k in d and hasattr(d[k], "strftime"):
                    d[k] = d[k].strftime("%Y-%m-%d")
                elif k in d:
                    d[k] = str(d[k]).strip()
            d["git_url"] = str(d.get("git_url", "")).strip()
            out.append(d)
        return out
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def run_one_repo(repo_row, args):
    repository_id = repo_row["repository_id"].strip()
    git_url = normalize_git_url(repo_row["git_url"].strip())
    branch = (repo_row.get("branch") or "").strip() or args.branch
    start_date = (repo_row.get("start_date") or "").strip() or args.start_date
    weeks = int((repo_row.get("weeks") or "").strip() or args.weeks)

    repo_path = Path(args.repos_dir) / repository_id
    workdir = Path(args.output_dir) / repository_id
    workdir.mkdir(parents=True, exist_ok=True)

    step(f"{repository_id}: clone/update (local: {repo_path})")
    clone_or_update(git_url, repo_path)

    step(f"{repository_id}: write repo_context.json")
    context = {
        "repository_id": repository_id,
        "repo_path": str(repo_path.resolve()),
        "project_key": f"{repository_id}-td",
        "branch": branch,
    }
    with open(workdir / "repo_context.json", "w") as f:
        json.dump(context, f, indent=2)
    print(json.dumps(context, indent=2))

    date_args = [start_date, branch, str(weeks)]
    wd = str(workdir)

    step(f"{repository_id}: Sonar scanning (one snapshot per week)")
    if args.skip_scan:
        print("SKIPPED (--skip-scan)")
    else:
        run([PYTHON, here("scan_weekly_snapshots.py"), *date_args], cwd=wd)

    step(f"{repository_id}: git feature sources")
    run([PYTHON, here("pipeline_total_line.py"), context["repo_path"], TOTAL_LINES_DIR,
         start_date, branch, str(weeks)], cwd=wd)
    run([PYTHON, here("add_weekly_commits_churn.py"), start_date, branch, str(weeks),
         TOTAL_LINES_DIR], cwd=wd)
    run([PYTHON, here("pipeline_g3.py"), TOTAL_LINES_DIR, G3_DIR], cwd=wd)
    run([PYTHON, here("compute_git_window_features.py"), *date_args], cwd=wd)
    run([PYTHON, here("compute_ownership_entropy.py"), *date_args], cwd=wd)
    run([PYTHON, here("compute_lineage.py"), *date_args], cwd=wd)

    step(f"{repository_id}: Sonar trend + lifecycle features")
    run([PYTHON, here("compute_sonar_trend_features.py")], cwd=wd)
    run([PYTHON, here("compute_finding_lifecycle.py")], cwd=wd)

    step(f"{repository_id}: build td_mart.csv")
    run([PYTHON, here("build_td_mart.py"), start_date, branch, str(weeks), G3_DIR], cwd=wd)

    mart_path = workdir / "td_mart.csv"
    if not mart_path.exists():
        raise RuntimeError(f"{repository_id}: build_td_mart.py did not produce {mart_path}")
    return mart_path


def combine_marts(repo_ids, holdout_ids, output_dir):
    output_dir = Path(output_dir)
    all_rows, fieldnames = [], None
    for repository_id in repo_ids:
        mart_path = output_dir / repository_id / "td_mart.csv"
        if not mart_path.exists():
            print(f">>> Skipping {repository_id} in combine step (no td_mart.csv -- failed earlier).")
            continue
        with open(mart_path, newline="") as f:
            reader = csv.DictReader(f)
            if fieldnames is None:
                fieldnames = reader.fieldnames
            for row in reader:
                if repository_id in holdout_ids:
                    row["held_repository"] = repository_id
                    row["split_family"] = "REPOSITORY_HELD_OUT"
                    row["partition"] = "TEST"
                    row["fold_id"] = "repo_holdout"
                all_rows.append(row)

    if not all_rows:
        print(">>> No rows to combine -- every repo failed or produced nothing.")
        return

    combined_path = output_dir / "combined_td_mart.csv"
    with open(combined_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f">>> Wrote {combined_path}: {len(all_rows)} rows across "
          f"{len({r['repository_id'] for r in all_rows})} repo(s).")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("repos_csv")
    parser.add_argument("--branch", default="trunk", help="fallback if a repo row omits branch")
    parser.add_argument("--start-date", default=None, help="fallback if a repo row omits start_date")
    parser.add_argument("--weeks", type=int, default=60, help="fallback if a repo row omits weeks")
    parser.add_argument("--holdout-repos", default="", help="comma-separated repository_ids to hold out entirely")
    parser.add_argument("--skip-scan", action="store_true")
    parser.add_argument("--resume-from", default=None,
                         help="repository_id to start at, skipping every repo before it in the CSV "
                              "(e.g. after a crash partway through a long multi-repo run)")
    parser.add_argument("--repos-dir", default="repo_clones",
                         help="Where repos get git-cloned. Keep this on LOCAL disk -- not a Drive-synced folder.")
    parser.add_argument("--output-dir", default="runs",
                         help="Where every repo's CSV outputs + td_mart.csv + combined_td_mart.csv get "
                              "written. Safe to point at a Drive-synced folder, e.g. 'G:/My Drive/td_runs'.")
    args = parser.parse_args()

    repos = load_repos(args.repos_csv)
    if args.resume_from:
        idx = next((i for i, r in enumerate(repos) if r["repository_id"] == args.resume_from), None)
        if idx is None:
            print(f"!!! --resume-from {args.resume_from} not found in {args.repos_csv}")
            sys.exit(1)
        repos = repos[idx:]

    Path(args.repos_dir).mkdir(parents=True, exist_ok=True)
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    holdout_ids = {r.strip() for r in args.holdout_repos.split(",") if r.strip()}

    succeeded, failed = [], []
    for repo_row in repos:
        repository_id = repo_row["repository_id"].strip()
        try:
            run_one_repo(repo_row, args)
            succeeded.append(repository_id)
        except Exception as e:
            print(f"\n!!! {repository_id} FAILED: {e}")
            print(f"!!! Continuing with the next repo. Re-run later with "
                  f"--resume-from {repository_id} to retry from here.")
            failed.append(repository_id)

    step("Combining all repos' td_mart.csv")
    # Deliberately combine from EVERY repo listed in repos.csv, not just
    # `succeeded` (this run's list). Otherwise a targeted --resume-from
    # run that only touches 1-2 repos would rebuild combined_td_mart.csv
    # from just those repos, silently dropping every other repo's rows
    # that were already sitting on disk from an earlier run.
    all_repo_ids = [r["repository_id"].strip() for r in load_repos(args.repos_csv)]
    combine_marts(all_repo_ids, holdout_ids, args.output_dir)

    print()
    print(f">>> Succeeded: {succeeded}")
    if failed:
        print(f">>> FAILED (skipped): {failed}")


if __name__ == "__main__":
    main()
