from pathlib import Path
import argparse
import sys
import pandas as pd


def split_by_label(input_csv: Path, out0: Path, out1: Path) -> None:
    """
    split dataset by label
    """

    # read in csv as string
    df = pd.read_csv(input_csv, dtype=str)

    # normalize column names and resolve case-insensitively
    df.columns = [c.strip() for c in df.columns]
    colmap = {c.lower(): c for c in df.columns}

    if "url" not in colmap or "label" not in colmap:
        raise ValueError(
            f"CSV must contain columns 'url' and 'label'."
            f"Found: {list(df.columns)}"
        )

    url_col = colmap["url"]
    label_col = colmap["label"]

    # clean values
    df = df[[url_col, label_col]].copy()
    df[url_col] = df[url_col].astype(str).str.strip()
    df = df[df[url_col] != ""]

    # parse label to int ('phishing' --> 0, 'legitimate' --> 1)
    df[label_col] = df[label_col].replace({'0': 0, '1': 1})
    df[label_col] = pd.to_numeric(df[label_col], errors="coerce")
    df = df[df[label_col].isin([0, 1])].copy()
    df[label_col] = df[label_col].astype(int)

    # split dataset
    df0 = df[df[label_col] == 0][[url_col, label_col]].copy()
    df1 = df[df[label_col] == 1][[url_col, label_col]].copy()
    df0.columns = ["url", "label"]
    df1.columns = ["url", "label"]

    # write splittet data to csv files
    write_header_out0 = (not out0.exists() or out0.stat().st_size == 0)
    write_header_out1 = (not out1.exists() or out1.stat().st_size == 0)
    df0.to_csv(out0, mode='a', header=write_header_out0, index=False, encoding='utf-8')
    df1.to_csv(out1, mode='a', header=write_header_out1, index=False, encoding='utf-8')

    print(f"Done!")
    print(f"  Label 0: {len(df0):>6} URLs -> {out0}")
    print(f"  Label 1: {len(df1):>6} URLs -> {out1}")


def parse_args() -> argparse.Namespace:
    """
    parse command line arguments
    """
    p = argparse.ArgumentParser(
        description="Splits a CSV into two CSV-Lists based on the 'label' column"
    )
    p.add_argument(
        "input_csv",
        type=Path,
        help="Path to the input csv (with columns 'url' and 'label')"
    )
    p.add_argument(
        "--out0",
        type=Path,
        default=Path("urls_label_0.csv"),
        help="output file for label==0 (Default: urls_label_0.csv)"
    )
    p.add_argument(
        "--out1",
        type=Path,
        default=Path("urls_label_1.csv"),
        help="output file for label==1 (Default: urls_label_1.csv)"
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        split_by_label(args.input_csv, args.out0, args.out1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)