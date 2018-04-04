from cement.core.foundation import CementApp
from cement.core.controller import CementBaseController, expose
from cement.core.exc import CaughtSignal
import signal
import twapi


VERSION = '0.1.0'

BANNER = """
TwitterTrend v%s
===================
""" % VERSION


class MyBaseController(CementBaseController):
    class Meta:
        label = 'base'
        description = 'TwitterTrend listens to Twitter API to fetch tweets with keywords ' \
                      'and saves them to BigQuery cloud database to do some analytics.'

    @expose(help="Print info on screen.")
    def info(self):
        print(BANNER)
        twapi.info()

    @expose(help="Listen to Twitter API and print results on screen. (no saving)")
    def listen(self):
        print(BANNER)
        twapi.listen()

    @expose(help="Listen to Twitter API and save results into BigQuery.")
    def record(self):
        print(BANNER)
        twapi.record()

    @expose(help="A list of tables in BigQuery.")
    def tables(self):
        print(BANNER)
        twapi.list_tables()

    @expose(help="Print results on screen.")
    def results(self):
        print(BANNER)
        twapi.results()

    @expose(help="Remove a table from BigQuery.  (A new table will be created next time.)")
    def delete(self):
        print(BANNER)
        twapi.delete_table()


class MyApp(CementApp):
    class Meta:
        label = 'scraper'
        base_controller = MyBaseController


with MyApp() as app:
    try:
        app.run()
    except CaughtSignal as e:
        # handle interruption
        if e.signum == signal.SIGTERM:
            print(" [killed]")
        elif e.signum == signal.SIGINT:
            print(" [Ctrl-C pressed]")
