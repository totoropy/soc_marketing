from pprint import pprint
from google.cloud import bigquery
import json
import settings
import tweepy

LEN_OF_TEXT = 30


def get_label(message):
    for item in settings.KEYWORD_GROUPS:
        for label, variants in item.items():
            for key in variants:
                m = message.lower()
                if m.find(key.lower()) > -1:
                    return label


class PrintingListener(tweepy.StreamListener):
    """
    Listens Twitter API and print tweets on screen
    """

    def __init__(self, api):
        self.count = 0
        self.ticker = 0
        self.keywords = []
        for item in settings.KEYWORD_GROUPS:
            for keys in item.values():
                for key in keys:
                    self.keywords.append(key)
        super(PrintingListener, self).__init__(api)

    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def on_status(self, status):
        if status.retweeted:
            return

        # cesky
        # if status.lang != 'cs':
        #    return

        try:
            full_text = status.extended_tweet["full_text"]
        except AttributeError:
            full_text = status.text

        label = get_label(full_text)
        if not label:
            return

        self.count += 1
        label = "{}        ".format(label)
        text = full_text
        if len(full_text) > LEN_OF_TEXT:
            text = full_text[:LEN_OF_TEXT] + '..'

        spaces = ' ' * (len(str(settings.MAX_TWEET_COUNT))-len(str(self.count)))
        number = "{}{}. ".format(spaces, self.count)
        print("{}[{}  ]: {}".format(number, label[:10], text))
        return self.count < settings.MAX_TWEET_COUNT


class BigQueryListener(tweepy.StreamListener):
    """
    Listens tweets and save them in BigQuery
    """

    def __init__(self, api):
        self.count = 0
        self.keywords = []
        for item in settings.KEYWORD_GROUPS:
            for keys in item.values():
                for key in keys:
                    self.keywords.append(key)

        super(BigQueryListener, self).__init__(api)
        dataset_id = settings.DATASET_NAME
        table_id = settings.TABLE_NAME
        self.bq_client = bigquery.Client.from_service_account_json(settings.BQ_SETTING_FILE)
        self.bq_dataset = self.bq_client.dataset(dataset_id)
        tables = list(self.bq_client.list_tables(self.bq_dataset))  # API request(s)
        if table_id not in [table.table_id for table in tables]:
            self.bq_table = self.create_table()
        else:
            # existing table
            table_ref = self.bq_dataset.table(table_id)
            self.bq_table = self.bq_client.get_table(table_ref)

        if not self.bq_table:
            raise ValueError('Table not found in BigQuery.')

    def on_error(self, status_code):
        print(status_code)
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def on_status(self, status):
        # pprint(status)
        if status.retweeted:
            return

        # cesky
        # if status.lang != 'cs':
        #     return

        try:
            full_text = status.extended_tweet["full_text"]
        except AttributeError:
            full_text = status.text

        label = get_label(full_text)
        if not label:
            return

        self.count += 1
        data = {'full_message': full_text,
                'keyword': label,
                'created': status.created_at.strftime("%Y-%m-%d %H:%M:%S")}
        self.save_data(data)

        label = "{}        ".format(label)
        text = full_text
        if len(full_text) > LEN_OF_TEXT:
            text = full_text[:LEN_OF_TEXT] + '.. Saved!'

        spaces = ' ' * (len(str(settings.MAX_TWEET_COUNT)) - len(str(self.count)))
        number = "{}{}. ".format(spaces, self.count)
        print("{}[{}  ]: {}".format(number, label[:10], text))
        return self.count < settings.MAX_TWEET_COUNT

    def create_table(self):
        # new table in big_query
        schema = [
            bigquery.SchemaField('full_message', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('keyword', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('created', 'DATETIME', mode='REQUIRED'),
        ]
        table_ref = self.bq_dataset.table(settings.TABLE_NAME)
        table = bigquery.Table(table_ref, schema=schema)
        return self.bq_client.create_table(table)  # API request

    def save_data(self, data):
        jdata = json.dumps(data)
        row = json.loads(jdata)
        errors = self.bq_client.insert_rows(self.bq_table, [row])

        if errors:
            pprint(errors)
