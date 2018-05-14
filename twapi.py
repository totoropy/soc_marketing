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


def record(table_name=None):
    """ Connect to Twitter and listen tweets with BigQueryListener """
    if not table_name:
        table_name = settings.TABLE_NAME
    if check_bq_connection(table_name):
        api = get_api()
        listener = BigQueryListener(api)
        try:
            stream = tweepy.Stream(auth=api.auth, listener=listener, tweet_mode='extended')
            print("Writing into table: {}".format(table_name))
            print("Listening to Twitter API..   (to stop press Ctrl-C)")
            stream.filter(track=listener.keywords)
        except ProtocolError:
            # This is hack to prevent stopping listening,
            # but it set count = 0 and MAX_TWEET_COUNT will not work
            print("-------- Twitter ProtocolError ---------")
            record()


def delete_table(table_name=None):
    """ Remove current table """
    if not table_name:
        table_name = settings.TABLE_NAME
    if check_bq_connection(table_name):
        bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
        bq_dataset = bq_client.dataset(settings.DATASET_NAME)
        tables = list(bq_client.list_tables(bq_dataset))  # API request(s)
        if table_name not in [table.table_id for table in tables]:
            print("Table '{}:{}' not found.".format(settings.DATASET_NAME, table_name))
        else:
            # existing table
            table_ref = bq_dataset.table(table_name)
            bq_client.delete_table(table_ref)  # API request
            print("Table '{}:{}' was removed.".format(settings.DATASET_NAME, table_name))


def create_table(table_name=None):
    # new table in big_query
    if not table_name:
        table_name = settings.TABLE_NAME
    bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
    bq_dataset = bq_client.dataset(settings.DATASET_NAME)
    schema = [
        bigquery.SchemaField('full_message', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('keyword', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('created', 'DATETIME', mode='REQUIRED'),
    ]
    table_ref = bq_dataset.table(table_name)
    table = bigquery.Table(table_ref, schema=schema)
    bq_client.create_table(table)  # API request
    print("Table '{}:{}' was created.".format(settings.DATASET_NAME, table_name))


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


def results(table_name=None):
    """ Executes SQL statement and prints results on screen """
    if not table_name:
        table_name = settings.TABLE_NAME
    print("Results for table: {}".format(table_name))
    if check_bq_connection(table_name):
        source = "{}.{}.{}".format(settings.PROJECT_ID, settings.DATASET_NAME, table_name)
        query = "SELECT keyword, count(keyword) AS CC FROM `{}` " \
                "GROUP BY keyword ORDER BY keyword DESC".format(source)
        bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
        query_job = bq_client.query(query)
        return query_job.result()


def delete_data(table_name=None):
    """ Executes SQL DELETE data in table """
    if not table_name:
        table_name = settings.TABLE_NAME
    if check_bq_connection(table_name):
        source = "{}.{}.{}".format(settings.PROJECT_ID, settings.DATASET_NAME, table_name)
        query = "DELETE FROM `{}` WHERE true".format(source)
        bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
        query_job = bq_client.query(query)
        return query_job.result()


def check_bq_table(table):
    """ Check connection string to BigQuery """
    bq_client = None
    try:
        bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
    except Exception as ex:
        print("Authentication with BigQuery failed. (check settings.BQ_SETTING_FILE)")
        return False

    tables = []
    try:
        bq_dataset = bq_client.dataset(settings.DATASET_NAME)
        tables = list(bq_client.list_tables(bq_dataset))
    except Exception as ex:
        print("Dataset '{}' not found in BigQuery. Command failed.".format(settings.DATASET_NAME))
        return False

    exist = table in [t.table_id for t in tables]
    if not exist:
        print("Table '{}' not found in BigQuery. Command failed.".format(table))
    return exist


def check_bq_connection(table):
    """ Executes SQL statement and prints results on screen """
    if check_bq_table(table):
        source = "{}.{}.{}".format(settings.PROJECT_ID, settings.DATASET_NAME, table)
        query = "SELECT keyword FROM `{}` LIMIT 1".format(source)
        try:
            bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
            query_job = bq_client.query(query)
            r = query_job.result()
            return True
        except Exception as ex:
            print("Connection source '{}' is not valid. Command failed.".format(source))
            print(ex)
            return False

