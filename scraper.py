from cement.core.foundation import CementApp
from cement.core.controller import CementBaseController, expose
from cement.core.exc import CaughtSignal
from tests import BigQueryTest
import settings
import signal
import twapi


VERSION = '0.1.0'

BANNER = """
TwitterTrend v%s
===================
""" % VERSION


class MyBaseController(CementBaseController):
    """
    MyBaseController handles all basic commands accepted from CLI
    """
    class Meta:
        label = 'base'
        description = 'TwitterTrend listens to Twitter API to fetch tweets with keywords ' \
                      'and saves them to BigQuery cloud database to do some analytics.'

    @expose(help="Print info on screen.")
    def info(self):
        """
        Shows information about current setting of the application
        :return:
        """
        print(BANNER)
        twapi.info()

    @expose(help="Listen to Twitter API and print results on screen. (no saving)")
    def listen(self):
        """
        Listens to Twitter API and print results on screen

        :return:
        """
        print(BANNER)
        twapi.listen()

    @expose(help="Listen to Twitter API and save results into BigQuery.")
    def record(self):
        """
        Listens to Twitter API and save data(tweets) in BigQuery

        :return:
        """
        print(BANNER)
        twapi.record()

    @expose(help="A list of tables in BigQuery.")
    def tables(self):
        """
        Lists all tables in current dataset

        :return:
        """
        print(BANNER)
        twapi.list_tables()

    @expose(help="Print results on screen.")
    def results(self):
        """
        Prints counts of keywords on screen.
        :return:
        """
        print(BANNER)
        rows = twapi.results()
        print("Results:")
        for row in rows:
            print(row[0], row[1])

    @expose(help="Remove a table from BigQuery.  (A new table will be created next time.)")
    def delete(self):
        """
        Removes current table and create new one
        :return:
        """
        print(BANNER)
        twapi.delete_table()
        twapi.create_table()

    @expose(help="Run tests.")
    def testcounts(self):
        """
        Run tests.
        :return:
        """
        print(BANNER)
        print("Test will add data in current table: {} ".format(settings.TABLE_NAME))
        answer = input("Do you agree? [n]")
        if answer.lower() == 'y':
            t = BigQueryTest()
            t.test_counts()

    @expose(help="Run tests.")
    def testtweet(self):
        """
        Run tests.
        :return:
        """
        print(BANNER)
        t = BigQueryTest()
        t.test_tweet()


class MyApp(CementApp):
    """
    Main Application
    """
    class Meta:
        label = 'scraper'
        base_controller = MyBaseController

# Run the application
with MyApp() as app:
    try:
        app.run()
    except CaughtSignal as e:
        # handle interruption
        if e.signum == signal.SIGTERM:
            print(" [killed]")
        elif e.signum == signal.SIGINT:
            print(" [Ctrl-C pressed]")
