import pandas as pd

eval_df = pd.read_csv("evaluation-features.csv", dtype={"url": str})

only_urls = eval_df["url"].astype(str).str.strip()

only_urls.to_csv("chatgpt-evaluation-input.csv", index=False)