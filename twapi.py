import tweepy
from pprint import pprint
from listeners import PrintingListener, BigQueryListener
from google.cloud import bigquery
from urllib3.exceptions import ProtocolError
import settings
import time


def get_api():
    """ Connect to Twitter """
    auth = tweepy.OAuthHandler(settings.consumer_key, settings.consumer_secret)
    auth.set_access_token(settings.access_token, settings.access_token_secret)
    return tweepy.API(auth)


def post_test_tweet(message, delay=0):
    """ Post a tweet """
    print("\nTest-tweet will be posted in 5sec.\n")
    if delay:
        time.sleep(delay)

    api = get_api()
    api.update_status(message)
    print("\nTweet posted:   %s    at %s\n\n" % (message, time.ctime(time.time())))


def listen():
    """ Connect to Twitter and listen tweets with PrintingListener """
    api = get_api()
    listener = PrintingListener(api)
    stream = tweepy.Stream(auth=api.auth, listener=listener, tweet_mode='extended')
    print("Listening to Twitter API..   (to stop press Ctrl-C)")
    stream.filter(track=listener.keywords)


def record():
    """ Connect to Twitter and listen tweets with BigQueryListener """
    api = get_api()
    listener = BigQueryListener(api)
    try:
        stream = tweepy.Stream(auth=api.auth, listener=listener, tweet_mode='extended')
        print("Listening to Twitter API..   (to stop press Ctrl-C)")
        stream.filter(track=listener.keywords)
    except ProtocolError:
        # This is hack to prevent stopping listening,
        # but it set count = 0 and MAX_TWEET_COUNT will not work
        print("-------- Twitter ProtocolError ---------")
        record()


def delete_table():
    """ Remove current table """
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


def create_table():
    # new table in big_query
    bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
    bq_dataset = bq_client.dataset(settings.DATASET_NAME)
    schema = [
        bigquery.SchemaField('full_message', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('keyword', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('created', 'DATETIME', mode='REQUIRED'),
    ]
    table_ref = bq_dataset.table(settings.TABLE_NAME)
    table = bigquery.Table(table_ref, schema=schema)
    bq_client.create_table(table)  # API request
    print("Table '{}:{}' was created.".format(settings.DATASET_NAME, settings.TABLE_NAME))


def list_tables():
    """ List all tables in current dataset """
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
    """ Print setting info on screen """
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
    """ Executes SQL statement and prints results on screen """
    source = "{}.{}.{}".format(settings.PROJECT_NAME, settings.DATASET_NAME, settings.TABLE_NAME)
    query = "SELECT keyword, count(keyword) AS CC FROM `{}` " \
            "GROUP BY keyword ORDER BY keyword DESC".format(source)
    bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
    query_job = bq_client.query(query)
    return query_job.result()


def delete_data():
    """ Executes SQL DELETE data in table """
    source = "{}.{}.{}".format(settings.PROJECT_NAME, settings.DATASET_NAME, settings.TABLE_NAME)
    query = "DELETE FROM `{}` WHERE true".format(source)
    bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
    query_job = bq_client.query(query)
    return query_job.result()
