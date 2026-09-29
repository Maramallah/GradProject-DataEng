import subprocess
import pandas as pd
from pathlib import Path


def extract_weekly_total_lines(
    repo_path,
    output_dir,
    start_date,
    branch="trunk",
    file_extension=".java",
    max_weeks=None
):
    """
    Extract total lines of code for every file at the end
    of each week in a Git repository.

    Parameters
    ----------
    repo_path : str or Path
        Path to the Git repository.

    output_dir : str or Path
        Folder where weekly CSV files will be saved.

    start_date : str or pd.Timestamp
        Starting date of the first week.

    branch : str, default="trunk"
        Git branch to analyze.

    file_extension : str, default=".java"
        File type to analyze.

    max_weeks : int or None, default=None
        Stop after this many weeks from start_date, even if the branch's
        latest commit is further out. None = walk all the way to the
        latest commit (this was the ONLY behavior before this parameter
        existed -- pass an explicit max_weeks to cap it, e.g. to match a
        fixed-size dataset like scan_weekly_snapshots.py's <weeks> arg).

    Returns
    -------
    dict
        Dictionary containing:
        - week dates
        - snapshot commits
        - output files
    """

    # ========================================================
    # SETUP
    # ========================================================

    repo_path = Path(repo_path)
    output_dir = Path(output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    start_date = pd.Timestamp(start_date)

    # Make sure extension starts with .
    if not file_extension.startswith("."):
        file_extension = "." + file_extension


    # ========================================================
    # GIT HELPER
    # ========================================================

    def run_git(args):

        result = subprocess.run(
            [
                "git",
                "-C",
                str(repo_path),
                *args
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace"
        )

        if result.returncode != 0:

            raise RuntimeError(
                f"Git command failed:\n"
                f"{' '.join(args)}\n\n"
                f"{result.stderr}"
            )

        return result.stdout


    # ========================================================
    # FIND LATEST COMMIT DATE
    # ========================================================

    latest_date = run_git([
        "log",
        "-1",
        "--format=%cI",
        branch
    ]).strip()

    latest_date = pd.Timestamp(
        latest_date
    ).tz_localize(None)


    # ========================================================
    # CREATE WEEKLY CALENDAR
    # ========================================================

    weeks = {}

    current_start = start_date
    week_number = 1

    while current_start <= latest_date:

        if max_weeks is not None and week_number > max_weeks:
            break

        current_end = (
            current_start +
            pd.Timedelta(days=6)
        )

        weeks[week_number] = (
            current_start,
            current_end
        )

        current_start += pd.Timedelta(days=7)

        week_number += 1


    print(
        f"Repository: {repo_path}"
    )

    print(
        f"Branch: {branch}"
    )

    print(
        f"Period: {start_date.date()} "
        f"-> {latest_date.date()}"
    )

    print(
        f"Weeks: {len(weeks)}"
    )


    # ========================================================
    # FIND WEEK-END SNAPSHOTS
    # ========================================================

    week_commits = {}

    for week, (start, end) in weeks.items():

        # First day after the week
        before_date = (
            end +
            pd.Timedelta(days=1)
        ).strftime("%Y-%m-%d")
        
        commit = run_git([
            "rev-list",
            "-1",
            f"--before={before_date}",
            branch
        ]).strip()

        if commit:

            week_commits[week] = commit

            print(
                f"W{week:02d}: "
                f"{start.date()} -> "
                f"{end.date()} | "
                f"{commit[:10]}"
            )

        else:

            week_commits[week] = None

            print(
                f"W{week:02d}: "
                f"{start.date()} -> "
                f"{end.date()} | "
                f"No commit"
            )


    # ========================================================
    # EXTRACT TOTAL LINES
    # ========================================================

    output_files = {}

    for week, commit in week_commits.items():

        if commit is None:
            continue

        print(
            f"\nProcessing W{week:02d}..."
        )


        # ----------------------------------------------------
        # Get files existing at this snapshot
        # ----------------------------------------------------

        files_output = run_git([
            "ls-tree",
            "-r",
            "--name-only",
            commit
        ])

        files = files_output.splitlines()


        # ----------------------------------------------------
        # Filter requested file type
        # ----------------------------------------------------

        target_files = [
            file_path
            for file_path in files
            if file_path.lower().endswith(
                file_extension.lower()
            )
        ]

        print(
            f"Files found: "
            f"{len(target_files):,}"
        )


        # ----------------------------------------------------
        # Count lines
        # ----------------------------------------------------

        records = []

        for i, file_path in enumerate(
            target_files,
            start=1
        ):

            try:

                result = subprocess.run(
                    [
                        "git",
                        "-C",
                        str(repo_path),
                        "show",
                        f"{commit}:{file_path}"
                    ],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace"
                )

                if result.returncode != 0:
                    continue

                total_lines = len(
                    result.stdout.splitlines()
                )

                records.append({
                    "artifact_id": file_path,
                    "total_lines": total_lines
                })

            except Exception:
                continue


            if i % 500 == 0:

                print(
                    f"  Processed "
                    f"{i:,}/{len(target_files):,}"
                )


        # ----------------------------------------------------
        # DataFrame
        # ----------------------------------------------------

        week_df = pd.DataFrame(
            records,
            columns=[
                "artifact_id",
                "total_lines"
            ]
        )


        # ----------------------------------------------------
        # Save
        # ----------------------------------------------------

        output_file = (
            output_dir /
            f"week_{week:02d}.csv"
        )

        week_df.to_csv(
            output_file,
            index=False
        )

        output_files[week] = output_file

        print(
            f"W{week:02d} saved: "
            f"{len(week_df):,} files"
        )


    # ========================================================
    # RETURN RESULTS
    # ========================================================

    return {
        "weeks": weeks,
        "week_commits": week_commits,
        "output_files": output_files
    }

if __name__ == "__main__":
    import sys

    if len(sys.argv) not in (4, 5, 6, 7):
        print(
            "Usage: python3 pipeline_total_line.py <repo_path> <output_dir> "
            "<start_date> [branch] [weeks] [file_extension]\n\n"
            "weeks caps how far from start_date this walks (e.g. 60) -- pass "
            "the SAME value you use for scan_weekly_snapshots.py's <weeks> arg, "
            "or this will walk all the way to the branch's latest commit "
            "regardless of what you passed there."
        )
        sys.exit(1)

    repo_path, output_dir, start_date = sys.argv[1], sys.argv[2], sys.argv[3]
    branch = sys.argv[4] if len(sys.argv) >= 5 else "trunk"
    max_weeks = int(sys.argv[5]) if len(sys.argv) >= 6 else None
    file_extension = sys.argv[6] if len(sys.argv) == 7 else ".java"

    extract_weekly_total_lines(
        repo_path=repo_path,
        output_dir=output_dir,
        start_date=start_date,
        branch=branch,
        file_extension=file_extension,
        max_weeks=max_weeks,
    )
