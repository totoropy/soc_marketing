# allows multiple forms for a single word
KEYWORD_GROUPS = [
    {"Sport": ["football", "soccer", "tennis", "badminton", "hockey"]},
    {"BigData": ["bigquery", "big query", "machine learning", "deep learning", "neuron network"]},
    {"Blockchain": ["bitcoin", "etherium", "ripple", "litecoin", "stellar", "btc", "eth"]},
    {"Coding": ["python", "c++", "java", "dotnet", "php"]},
    {"Trump": ["trump"]},
    {"Politics": ["putin", "brexit", "macron", "merkel", "immigrant"]}]

MAX_TWEET_COUNT = 1000
PROJECT_NAME = "streaming-bq-123"
TABLE_NAME = "politika"
DATASET_NAME = 'twitterposts'

BQ_SETTING_FILE = 'streaming-bq-123-auth.json'

try:
    from private import *
except Exception:
    pass
