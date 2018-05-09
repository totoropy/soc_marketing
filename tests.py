from unittest import TestCase
from datetime import datetime
from twapi import *
import time
import _thread


class BigQueryTest(TestCase):

    def __init__(self):
        self.api = get_api()
        self.listener = BigQueryListener(self.api)

    def test_tweet(self):
        # type this test-tweet into settings.KEYWORD_GROUPS
        tweet = '{} {}'.format(settings.UNIQUE_TEST_PATTERN, round(time.time()))
        _thread.start_new_thread(post_test_tweet, (tweet, 5))
        listen()
        return

    def test_counts(self):
        rows = results()
        counts = dict()
        for item in settings.KEYWORD_GROUPS:
            for key in item.keys():
                counts[key] = 0

        for row in rows:
            if row[0] in counts:
                counts[row[0]] += int(row[1])

        i = 1
        test_count = 5
        j = 1
        for j in range(test_count):
            for item in settings.KEYWORD_GROUPS:
                for key in item.keys():
                    data = {'full_message': 'test-message-{}'.format(i),
                            'keyword': key,
                            'created': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                    self.listener.save_data(data)
                    i += 1

        print('{} records added to each category.'.format(j+1))

        rows = results()
        print('COUNTS BEFORE AND AFTER:')
        errors = ''
        for row in rows:
            if row[0] in counts:
                spaces = ' ' * (20 - len(row[0]))
                cat = "{}{} ".format(row[0].upper(), spaces)
                error = ' <= PASSED' if test_count == row[1] - counts[row[0]] else ' <= ERROR'
                print("{}   {} ->  {}  {}".format(cat, counts[row[0]], row[1], error))

        if errors:
            print('Error occurred.')
            print('TEST FAILED.')
        else:
            print('No errors.')
            print('TEST PASSED.')

