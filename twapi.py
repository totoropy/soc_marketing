import tweepy
from pprint import pprint
import json
from listeners import PrintingListener, BigQueryListener
from google.cloud import bigquery
import settings
from http.client import IncompleteRead
from urllib3.exceptions import ProtocolError


def get_api():
    auth = tweepy.OAuthHandler(settings.consumer_key, settings.consumer_secret)
    auth.set_access_token(settings.access_token, settings.access_token_secret)
    return tweepy.API(auth)


def listen():
    api = get_api()
    listener = PrintingListener(api)
    try:
        stream = tweepy.Stream(auth=api.auth, listener=listener, tweet_mode='extended')
        print("Listening to Twitter API..   (to stop press Ctrl-C)")
        stream.filter(track=listener.keywords)
    except ProtocolError:
        print("-------- Twitter ProtocolError ---------")
        listen()


def record():
    api = get_api()
    listener = BigQueryListener(api)
    try:
        stream = tweepy.Stream(auth=api.auth, listener=listener, tweet_mode='extended')
        print("Listening to Twitter API..   (to stop press Ctrl-C)")
        stream.filter(track=listener.keywords)
    except ProtocolError:
        print("-------- Twitter ProtocolError ---------")
        record()


def delete_table():
    bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
    bq_dataset = bq_client.dataset(settings.DATASET_NAME)
    tables = list(bq_client.list_tables(bq_dataset))  # API request(s)
    if settings.TABLE_NAME not in [table.table_id for table in tables]:
        print("Table '{}:{}' not found.".format(settings.DATASET_NAME, settings.TABLE_NAME))
    else:
        # existing table
        table_ref = bq_dataset.table(settings.TABLE_NAME)
        bq_client.delete_table(table_ref)  # API request
        print("Table '{}:{}' was removed.".format(settings.DATASET_NAME, settings.TABLE_NAME))


def list_tables():
    bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
    bq_dataset = bq_client.dataset(settings.DATASET_NAME)
    tables = list(bq_client.list_tables(bq_dataset))  # API request(s)
    for table in tables:
        print("{}:{}".format(bq_dataset.dataset_id, table.table_id))
    print("{} tables.".format(len(tables)))
    print("")
    print("Current table: {}:{}".format(settings.DATASET_NAME, settings.TABLE_NAME))
    print("")


def info():
    print("KEYWORD_GROUPS:")
    for item in settings.KEYWORD_GROUPS:
        for label, variants in item.items():
            print("{}:".format(label))
            for key in variants:
                print("  - {}".format(key))
    print("")
    print("BigQuery:")
    print("DATASET_NAME: {}".format(settings.DATASET_NAME))
    print("TABLE_NAME: {}".format(settings.TABLE_NAME))
    print("")


def results():
    source = "{}.{}.{}".format(settings.PROJECT_NAME, settings.DATASET_NAME, settings.TABLE_NAME)
    query = "SELECT keyword, count(keyword) AS CC FROM `{}` " \
            "GROUP BY keyword ORDER BY keyword DESC".format(source)
    bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
    query_job = bq_client.query(query)
    rows = query_job.result()

    print("Results:")
    for row in rows:
        print(row[0], row[1])
