# soc_marketing
It listens to Twitter API to fetch tweets with keywords and saves them to BigQuery for analytics.

## Instalation  
1. It uses:  
   a. Python3  
   b. Twitter account
   c. GoogleCloudPlatform https://cloud.google.com/bigquery/   (need a google account!)
  
2. Download the project  
`git clone https://github.com/rjuppa/soc_marketing.git`  
`cd soc_marketing`    

3. use a virtual environment  
`virtualenv -p python3 venv`  
`. venv/bin/activate`  
  
4. install requirements  
`pip install -r requirements.txt`  
  
5. You need to use your own credentials for access to GCP (json file project-id-bq-12345.json)

6. update Twitter credentials in private.py

7. update settings.py

8. run `python3 scraper.py -h`

You should get:  
```
$ python3 scraper.py -h
usage: scraper (sub-commands ...) [options ...] {arguments ...}

TwitterTrend listens to Twitter API to fetch tweets with keywords and saves them to BigQuery cloud database to do some analytics.
  
commands:  
  
  delete  
    Remove a table from BigQuery.  (A new table will be created next time.)  
  
  info  
    Print info on screen.  
  
  listen  
    Listen to Twitter API and print results on screen. (no saving)  
  
  record  
    Listen to Twitter API and save results into BigQuery.  
  
  results  
    Print results on screen.  
   
  tables  
    A list of tables in BigQuery.  
  
optional arguments:  
  -h, --help  show this help message and exit  
  --debug     toggle debug output  
  --quiet     suppress all output  
  
```
You can access to the BigQuery data from Google DataStudio:

![Alt text](visualization.png?raw=true "Trump rules them all!")

