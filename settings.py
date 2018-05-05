# allows multiple forms for a single word
UNIQUE_TEST_PATTERN = 'ZSWITestTweet'

KEYWORD_GROUPS = [
    {"Sport": ["football", "tennis", "badminton", "hockey"]},
    {"BigData": ["bigquery", "big query", "machine learning", "deep learning", "neuron network"]},
    {"Blockchain": ["bitcoin", "etherium", "ripple", "litecoin", "stellar"]},
    {"Coding": ["python", "c++", "java", "dotnet", "php"]},
    {"Test": [UNIQUE_TEST_PATTERN]},
    {"Politics": ["putin", "zeman", "macron", "babis"]}]

MAX_TWEET_COUNT = 1000
PROJECT_NAME = "streaming-bq-123"
TABLE_NAME = "politika"
DATASET_NAME = 'twitterposts'


BQ_SETTING_FILE = 'streaming-bq-123-auth.json'

try:
    from private import *
except Exception:
    pass
