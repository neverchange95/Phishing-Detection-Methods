# pull-openphish-feed.py
    python pull-openphish-feed.py --repo-url https://<YOUR_GITHUB_USER>:<TOKEN>@github.com/openphish/academic

#### Is used to create a realtime pipeline on phishing urls for blacklist checking and write that urls with some metadata to a csv-file which can be used to evaluate machine learning and deep learning algorithms.
* Pulls OpenPhish Academic Use Program realtime phishing url feed
* Writes only the most recent entries to a csv file that occurred within a specified time window
  * Time Window can be changed with `--pull-interval`. By default, set to 30 seconds.
