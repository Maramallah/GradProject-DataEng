import pandas as pd
import numpy as np
from pathlib import Path


# ============================================================
# 1. CHURN RATE
# ============================================================

def add_churn_rate(df):
    df = df.copy()

    df["churn_rate"] = (
        df["churn"] /
        df["total_lines"].replace(0, np.nan)
    )

    return df


# ============================================================
# 2. WEEKLY ACCELERATION
# ============================================================

def add_acceleration(df, previous_df):
    df = df.copy()

    if previous_df is None:
        df["churn_acceleration"] = pd.NA
        df["commit_acceleration"] = pd.NA
        return df

    previous = previous_df[
        ["artifact_id", "churn_rate", "commits"]
    ].copy()

    merged = df[
        ["artifact_id", "churn_rate", "commits"]
    ].merge(
        previous,
        on="artifact_id",
        how="left",
        suffixes=("_current", "_previous")
    )

    previous_commits = merged["commits_previous"].fillna(0)
    previous_churn_rate = merged["churn_rate_previous"].fillna(0)

    df["commit_acceleration"] = (
        merged["commits_current"] - previous_commits
    ).values

    df["churn_acceleration"] = (
        merged["churn_rate_current"] - previous_churn_rate
    ).values

    return df


# ============================================================
# 3. COMMIT BURSTINESS
# ============================================================

def add_commit_burstiness(df, previous_dfs):
    df = df.copy()

    if len(previous_dfs) < 3:
        df["commit_burstiness"] = pd.NA
        return df

    weekly = []

    for i, previous_df in enumerate(previous_dfs[-3:], start=1):

        temp = previous_df[
            ["artifact_id", "commits"]
        ].copy()

        temp = temp.rename(
            columns={"commits": f"commits_w{i}"}
        )

        weekly.append(temp)

    merged = weekly[0]

    for temp in weekly[1:]:
        merged = merged.merge(
            temp,
            on="artifact_id",
            how="outer"
        )

    commit_values = merged[
        ["commits_w1", "commits_w2", "commits_w3"]
    ].fillna(0)

    variance = commit_values.var(
        axis=1,
        ddof=0
    )

    mean = commit_values.mean(axis=1)

    merged["commit_burstiness"] = np.where(
        mean > 0,
        variance / mean,
        0
    )

    burstiness = merged[
        ["artifact_id", "commit_burstiness"]
    ]

    df = df.drop(
        columns=["commit_burstiness"],
        errors="ignore"
    )

    df = df.merge(
        burstiness,
        on="artifact_id",
        how="left"
    )

    return df


# ============================================================
# 4. CHURN BURSTINESS
# ============================================================

def add_churn_burstiness(df, previous_dfs):
    df = df.copy()

    if len(previous_dfs) < 3:
        df["churn_burstiness"] = pd.NA
        return df

    weekly = []

    for i, previous_df in enumerate(previous_dfs[-3:], start=1):

        temp = previous_df[
            ["artifact_id", "churn"]
        ].copy()

        temp = temp.rename(
            columns={"churn": f"churn_w{i}"}
        )

        weekly.append(temp)

    merged = weekly[0]

    for temp in weekly[1:]:
        merged = merged.merge(
            temp,
            on="artifact_id",
            how="outer"
        )

    churn_values = merged[
        ["churn_w1", "churn_w2", "churn_w3"]
    ].fillna(0)

    std = churn_values.std(
        axis=1,
        ddof=0
    )

    mean = churn_values.mean(axis=1)

    merged["churn_burstiness"] = np.where(
        std + mean > 0,
        (std - mean) / (std + mean),
        0
    )

    burstiness = merged[
        ["artifact_id", "churn_burstiness"]
    ]

    df = df.drop(
        columns=["churn_burstiness"],
        errors="ignore"
    )

    df = df.merge(
        burstiness,
        on="artifact_id",
        how="left"
    )

    return df


# ============================================================
# 5. COMPLETE PIPELINE
# ============================================================

def feature_pipeline(
    input_path,
    output_path,
    file_pattern="week_*.csv"
):

    input_path = Path(input_path)
    output_path = Path(output_path)

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    files = sorted(
        input_path.glob(file_pattern)
    )

    if not files:
        raise FileNotFoundError(
            f"No files found in {input_path} "
            f"using pattern '{file_pattern}'"
        )

    # Store processed weekly data
    weekly_data = {}

    for i, file in enumerate(files):

        print(f"Processing {file.name}...")

        # ----------------------------------------------------
        # Read input
        # ----------------------------------------------------

        df = pd.read_csv(file)

        # ----------------------------------------------------
        # Validate required columns
        # ----------------------------------------------------

        required_columns = {
            "artifact_id",
            "churn",
            "commits",
            "total_lines"
        }

        missing = required_columns - set(df.columns)

        if missing:
            raise ValueError(
                f"{file.name} is missing columns: {missing}"
            )

        # ----------------------------------------------------
        # Churn rate
        # ----------------------------------------------------

        df = add_churn_rate(df)

        # ----------------------------------------------------
        # Acceleration
        # ----------------------------------------------------

        previous_df = None

        if i > 0:
            previous_df = weekly_data[
                files[i - 1].name
            ]

        df = add_acceleration(
            df,
            previous_df
        )

        # ----------------------------------------------------
        # Burstiness
        # ----------------------------------------------------

        previous_dfs = []

        for j in range(
            max(0, i - 3),
            i
        ):
            previous_dfs.append(
                weekly_data[
                    files[j].name
                ]
            )

        df = add_commit_burstiness(
            df,
            previous_dfs
        )

        df = add_churn_burstiness(
            df,
            previous_dfs
        )

        # ----------------------------------------------------
        # Store current week
        # ----------------------------------------------------

        weekly_data[file.name] = df.copy()

        # ----------------------------------------------------
        # Save
        # ----------------------------------------------------

        output_file = output_path / file.name

        df.to_csv(
            output_file,
            index=False
        )

        print(f"{file.name} done")

    print("\nPipeline completed successfully.")

    return weekly_data

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python3 pipeline_g3.py <input_dir> <output_dir>")
        sys.exit(1)

    feature_pipeline(
        input_path=sys.argv[1],
        output_path=sys.argv[2],
    )
