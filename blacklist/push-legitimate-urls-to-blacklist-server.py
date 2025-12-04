import argparse
from pathlib import Path

import pandas as pd
import requests

BLACKLIST_SERVER_URL = "http://127.0.0.1:8080/ingest-urls"


def send_data_to_blacklist_server(df: pd.DataFrame):
    payload = df.to_dict(orient="records")

    r = requests.post(BLACKLIST_SERVER_URL, json=payload, timeout=10)

    if not r.status_code == 200:
        print("ERROR: request blacklist server failed. See server log for details.")


def read_feed_csv_as_dataframe(csv_path: Path) -> pd.DataFrame:
    """
    read begin url csv-file into a pandas DataFrame to work with i
    """
    cols = ["url", "discover_time", "pulled_time"]
    read_kwargs = dict(dtype=str, keep_default_na=False)
    df = pd.read_csv(csv_path, usecols=cols, **read_kwargs)
    return df


if __name__ == '__main__':
    # create argument parser
    parser = argparse.ArgumentParser()

    # add argument
    parser.add_argument(
        "--csv-file", required=True, help="Path to the CSV file to analyze begin urls against blacklist"
    )

    # parse arguments
    args = parser.parse_args()

    # read data as data frame
    csv_file = Path(args.csv_file)
    df = read_feed_csv_as_dataframe(csv_file)

    # send data to blacklist-server api
    send_data_to_blacklist_server(df)