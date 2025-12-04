import argparse
import itertools
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import requests
from flask import Flask, request
import json

import pandas as pd

app = Flask(__name__)

GSB_URL = "https://safebrowsing.googleapis.com/v4/threatMatches:find?key="
THREAT_TYPES = [
    "MALWARE", "SOCIAL_ENGINEERING", "UNWANTED_SOFTWARE",
    "POTENTIALLY_HARMFUL_APPLICATION", "THREAT_TYPE_UNSPECIFIED"
]
PLATFORM_TYPES = ["ANY_PLATFORM"]
THREAT_ENTRY_TYPES = ["URL"]


def chunks(seq, size):
    """
    create n-sized chunks from seq
    """
    it = iter(seq)
    while True:
        batch = list(itertools.islice(it, size))
        if not batch:
            break
        yield batch


def request_gsb_api(url_list):
    """
    request for listed urls in google safe browsing api blacklist
    """
    all_rows = []
    total = 0

    # send urls in batches. Max-number of urls per request is set to 500
    for i, batch in enumerate(chunks(url_list, 500), start=1):
        body = {
            "client": {
                "clientId": "Blacklist-Server",
                "clientVersion": "1.0.0",
            },
            "threatInfo": {
                "threatTypes": THREAT_TYPES,
                "platformTypes": PLATFORM_TYPES,
                "threatEntryTypes": THREAT_ENTRY_TYPES,
                "threatEntries": [{"url": u} for u in batch],
            },
        }

        # send batch request to google safe browsing api
        batch_request_time = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%d/%m/%y %H:%M:%S")
        r = requests.post(app.config["GSB_URL"], json=body, timeout=30)
        response = r.json()
        total += len(batch)

        # collect matches by url
        by_url = defaultdict(list)
        matches = response.get("matches", []) or []
        for match in matches:
            thread = match.get("threat") or {}
            url = thread.get("url")
            by_url[url].append(match)

        # create a labeled output object for each input-url
        for u in batch:
            matches_for_u = by_url.get(u, [])

            if matches_for_u:
                # match found
                # take only the first match if more than one are available
                match_obj = matches_for_u[0]
                match_json_obj = json.dumps(match_obj, ensure_ascii=False)
                label = 0
            else:
                # no match found
                match_obj = {}
                match_json_obj = "{}"
                label = 1

            all_rows.append({
                "request_url": u,
                "match_obj": match_obj,
                "match_json_obj": match_json_obj,
                "request_time": batch_request_time,
                "label": label,
            })

    # return result as dataframe
    df = pd.DataFrame(all_rows, columns=["request_url", "match_obj", "match_json_obj", "request_time", "label"])
    return df


def write_analyzed_urls_to_csv(df_request, df_response):
    """
    write the analyzed URLs with corresponding metadata into a CSV file
    """
    csv_path = Path("../data/evaluation/blacklist-evaluation-results.csv")

    # prepare subsets
    req = df_request.loc[:, ["url", "discover_time", "pulled_time"]].copy()
    req["url"] = req["url"].astype(str).str.strip()

    res = df_response.loc[:, ["request_url", "match_json_obj", "request_time", "label"]].copy()
    res["url"] = res["request_url"].astype(str).str.strip()

    # merge
    merged = res.merge(
        req.loc[:, ["url", "discover_time", "pulled_time"]],
        on="url",
        how="left",
    )

    # define output structure
    out_cols = ["url", "match_json_obj", "discover_time", "pulled_time", "request_time", "label"]
    out_df = merged.loc[:, out_cols].copy()

    # make NaN pretty
    out_df = out_df.fillna("")

    # write to csv-file
    write_header = not csv_path.exists() or csv_path.stat().st_size == 0
    out_df.to_csv(
        csv_path,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8",
    )

    return out_df


@app.post("/ingest-urls")
def ingest_urls() -> dict[str, bool]:
    """
    checks whether received URLs appear in the Google Safe Browsing API Blacklist
    """
    data = request.get_json(force=True)
    req_df = pd.DataFrame(data, columns=["url", "discover_time", "pulled_time"])
    urls = [str(x.get("url")).strip() for x in data if isinstance(x, dict) and x.get("url")]

    # call gsb api and get response
    request_time = datetime.now(ZoneInfo("Europe/Berlin")).strftime("%d/%m/%y %H:%M:%S")
    print(f"\n[{request_time}]: send {len(urls)} urls to google safe browsing api blacklist.")
    res_df = request_gsb_api(urls)

    # write response into csv-file with metadata
    written_data = write_analyzed_urls_to_csv(req_df, res_df)
    print(f"🧐 {len(written_data)} urls checked and results written into csv-file.")

    return {"ok": True}


if __name__ == "__main__":
    # create argument parser
    parser = argparse.ArgumentParser(
        description="Server to check incoming urls against Google Safe Browsing API v4 Blacklist."
    )

    # add arguments
    parser.add_argument(
        "--gsb-key", required=True, help="Google Safe Browsing API key"
    )
    parser.add_argument(
        "--port", type=int, default=8080, help="Server port (default: 8080)"
    )

    # parse arguments
    args = parser.parse_args()

    # get GSB-API-Key
    gsb_key = args.gsb_key
    if not gsb_key:
        raise SystemExit("ERROR: Missing Google API-Key. Please set your key by using argument --gsb-key")

    # set request url to app env
    app.config["GSB_URL"] = f"{GSB_URL}{gsb_key}"

    # run server
    app.run(host="0.0.0.0", port=args.port, debug=False)