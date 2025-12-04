"""
filter duplicate and unique data between evaluation-features.csv and blacklist-evaluation-results.csv
"""

import pandas as pd

eval_df = pd.read_csv("evaluation-features.csv", dtype={"url": str})
black_df = pd.read_csv("blacklist-evaluation-results.csv", dtype={"url": str})

# clean up URLs in both DataFrames
for df in (eval_df, black_df):
    df["url"] = df["url"].astype(str).str.strip()

# Schnittmenge der URLs bestimmen
urls_eval  = set(eval_df["url"])
urls_black = set(black_df["url"])
common_urls = urls_eval & urls_black

# Keep only shared URLs and remove duplicates for each URL.
eval_clean = (
    eval_df[eval_df["url"].isin(common_urls)]
    .drop_duplicates(subset="url")
    .reset_index(drop=True)
)

black_clean = (
    black_df[black_df["url"].isin(common_urls)]
    .drop_duplicates(subset="url")
    .reset_index(drop=True)
)

eval_clean.to_csv("evaluation-features.csv", index=False)
black_clean.to_csv("blacklist-evaluation-results.csv", index=False)

print("Rows eval_clean :", len(eval_clean))
print("Rows black_clean:", len(black_clean))
print("Unique URLs eval_clean :", eval_clean["url"].nunique())
print("Unique URLs black_clean:", black_clean["url"].nunique())