# pull-openphish-feed.py
    python pull-openphish-feed.py --repo-url https://<YOUR_GITHUB_USER>:<TOKEN>@github.com/openphish/academic

#### Is used to create a realtime pipeline on phishing urls for blacklist checking and write that urls with some metadata to a csv-file which can be used to evaluate machine learning and deep learning algorithms.
* Pulls OpenPhish Academic Use Program realtime phishing url feed within a specified time window
  * Time Window can be changed with `--pull-interval`. By default, set to 5 minutes.
* Writes only the most recent entries to a csv file that occurred within the specified time window
* Sends url and some metadata of recent entries to a listening blacklist-server for evaluation

# blacklist-server.py
    python blacklist-server.py --gsb-key <YOUR_API_KEY>
* Opens a HTTP-Server with one route `/ingest-urls` to receive phishing and begin urls
* Ckecks urls against Googe Safe Browsing API v4 Blacklist
* Creates a CSV-File witch shows the result for requested urls by their label and some metadata

# feature-extractor.py
    python feature-extractor.py --isPhishing <0/1> --file <YOUR_BENGIN/PHISHING_URL_CSV_FILE>
* Extracts 27 Features from given urls and safe them into a feature.csv file
* This feature list can then be used for machine learning algos
* `--isPhishing 0` --> CSV-File contains Phishing-URLs
* `--isPhishing 1` --> CSV-File contains Bengin-URLs
* Your CSV-File must contain a column named "url" where urls to analyse are stored

# push-begin-urls-to-blacklist-server.py
    python push-begin-urls-to-blacklist-server.py --csv-file <YOUR_CSV_FILE_WITH_URLS>
* Helper Script which is used to evaluate the blacklist with bengin urls.
* Pushes a list of urls from a CSV-File (column "url") against the Google Safe Browsing API Blacklist
* It can be used also for Phishing URLs not only Bengin

# split-by-label.py
    python split-by-label.py <YOUR_CSV_FILE_WITH_URLS_AND_LABEL> --out0 out0.csv --out1 out1.csv
* Helper Script which is used to split a CSV-File with both phishing and bengin labeld urls into two seperated files
* Input file must contain columns: "url" and "label"
* Label must be 0/1
    * 0 --> Phishing
    * 1 --> Bengin
* `--out0`: Output File for Phishing URLs
* `--out1`: Output File for Bengin URLs
