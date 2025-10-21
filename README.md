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
