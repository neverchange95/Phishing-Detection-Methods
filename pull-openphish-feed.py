import argparse
import subprocess
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from pathlib import Path
from typing import Optional


def run(cmd: list[str], cwd: Optional[str] = None) -> None:
    """
    execute shell command
    """
    completed = subprocess.run(
        cmd, cwd=cwd, capture_output=True, text=True, check=False
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(cmd)}\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        )


def git_pull(local_dir: Path) -> None:
    """
     update repository (feed.csv)
    """
    run(["git", "-C", str(local_dir), "pull", "--ff-only"])


def ensure_repo_cloned(repo_url: str, local_dir: Path) -> None:
    """
    if local_dir does not exist, create folder and clone repository.
    """
    if local_dir.exists():
        return
    local_dir.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["git", "clone", "--depth", "1"]
    cmd += [repo_url, str(local_dir)]
    run(cmd)


def read_feed_csv_as_dataframe(csv_path: Path) -> pd.DataFrame:
    """
    read feed csv-file into a pandas DataFrame to work with it
    """
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    return df


def get_new_entries(prev_df: pd.DataFrame, act_df: pd.DataFrame) -> pd.DataFrame:
    """
    return new entries from act_df that pev_df not contains
    """
    # align cols
    cols = list(prev_df.columns)
    act_aligned = act_df[cols].copy()
    prev_aligned = prev_df[cols].copy()

    # normalize strings
    for df in (act_aligned, prev_aligned):
        obj_cols = df.select_dtypes(include=["object"]).columns
        for c in obj_cols:
            df[c] = df[c].fillna("").astype(str).str.strip()

    # Anti-Join: all cols as join key
    merged = act_aligned.merge(
        prev_aligned.drop_duplicates(),
        on=cols,
        how="left",
        indicator=True
    )
    new_mask = merged["_merge"].eq("left_only")
    new_rows = act_aligned.loc[new_mask].copy()

    # console log
    if new_rows.empty:
        print("❌ no new entries found.")
    else:
        print(f"✅ {len(new_rows)} new entries found.")

    return new_rows


def write_new_entries(new_entries: pd.DataFrame) -> None:
    output = Path("evaluation-feed.csv")

    # write header only if file not exists or is empty
    write_header = not output.exists() or output.stat().st_size == 0

    if output.exists() and output.stat().st_size > 0:
        # read header from filled csv and use as sequence
        cols = pd.read_csv(output, nrows=0).columns.tolist()
    else:
        # write header first time -> use origin order
        cols = new_entries.columns.tolist()

    # adjust DataFrame to the target order
    new_entries = new_entries.reindex(columns=cols)

    # write output to csv
    new_entries.to_csv(output, mode="a", header=write_header, index=False, encoding="utf-8")


def main():
    # create argument parser
    parser = argparse.ArgumentParser(
        description="Watch OpenPhish feed.csv, add new entries to feature list and call blacklist with new entries"
    )

    # add arguments
    parser.add_argument(
        "--repo-url", required=True, help="https://<YOUR_GITHUB_USER>:<TOKEN>@github.com/openphish/academic"
    )
    parser.add_argument(
        "--pull-interval", type=int, default=30, help="interval between pull requests (default: 30s)"
    )

    # parse arguments
    args = parser.parse_args()

    # set local folder destination for openphish repo
    local_dir = Path("./openphish-academic-repo")

    # clone repo if not exist
    try:
        ensure_repo_cloned(args.repo_url, Path(local_dir).resolve())
    except Exception as e:
        print(f"ERROR: cloning repository failed: {e}", file=sys.stderr)
        sys.exit(2)

    print("\nOpenPhish feed watcher is running. Press Ctrl+C to stop.")
    print(f"Repo: {args.repo_url}")
    print(f"Local dir: {local_dir}")
    print(f"Pull interval: {args.pull_interval} seconds\n")

    # check if feed.csv exist
    feed_csv = Path(local_dir).joinpath("feed.csv")
    if not feed_csv.exists():
        print("ERROR: feed file not found: feed.csv")
        exit(1)

    # hold previous phish-feed data frame in memory to compare it with the actual one
    prev_df: pd.DataFrame = read_feed_csv_as_dataframe(feed_csv)

    # run watcher
    while True:
        # pull newest entries
        ts = datetime.now(ZoneInfo("Europe/Berlin")).strftime('%d/%m/%y %H:%M:%S')
        print(f"[{ts}]: pull repository for new entries.")
        git_pull(local_dir)

        # read feed.csv
        feed_csv = Path(local_dir).joinpath("feed.csv")
        if not feed_csv.exists():
            print("WARNING: feed file not found: feed.csv")
        else:
            # get update as data frame
            act_df: pd.DataFrame = read_feed_csv_as_dataframe(feed_csv)

            # get only new entries
            new_entries = get_new_entries(prev_df, act_df)

            # to check for new data in next iteration
            prev_df = act_df.copy()

            if not new_entries.empty:
                print("writing new entries...")
                write_new_entries(new_entries)

            # TODO: call blacklist function with list of new urls, if new entries have arrived

            # sleep
            try:
                print(f"💤 finished! Sleeping for {args.pull_interval} seconds.\n")
                time.sleep(args.pull_interval)
            except KeyboardInterrupt:
                print("\nStopped by user during sleep. Bye!")
                break


if __name__ == "__main__":
    main()