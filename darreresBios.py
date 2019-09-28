import requests
import pandas as pd
import numpy as np
import io
from urllib.parse import unquote
import argparse
import pprint
import json
import mwclient
import sqlite3

pp = pprint.PrettyPrinter(indent=4)

# Import JSON configuration
parser = argparse.ArgumentParser(description="""Script for testing MediaWiki API""")
parser.add_argument("-config",help="""Path to a JSON file with configuration options!""")
args = parser.parse_args()


host = "ca.wikipedia.org"
user = None
password = None
protocol = "https"
data = {}
dbfile = "mydb.db"

if "config" in args:
		if args.config is not None:
				with open(args.config) as json_data_file:
						data = json.load(json_data_file)

if "mw" in data:
		if "host" in data["mw"]:
				host = data["mw"]["host"]
		if "user" in data["mw"]:
				user = data["mw"]["user"]
		if "password" in data["mw"]:
				pwd = data["mw"]["password"]
		if "protocol" in data["mw"]:
				protocol = data["mw"]["protocol"]

if "dbfile" in data:
		dbfile = data["dbfile"]

site = mwclient.Site(host, scheme=protocol)
if user and pwd :
		# Login parameters
		site.login(user, pwd)

conn = sqlite3.connect( dbfile )
cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS `bios` (  `article` VARCHAR(255) NOT NULL PRIM, `cdate` datetime NULL, `cuser` VARCHAR(255) NULL  ) ;")

query = """
SELECT ?item ?itemLabel ?article WHERE {
  ?item wdt:P31 wd:Q5.
  ?item wdt:P21 wd:Q6581072 .
  ?article schema:about ?item .
  ?article schema:isPartOf <https://ca.wikipedia.org/> .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],ca" } .
} ORDER BY ?article limit 500
"""

headers = {
	'Accept': 'text/csv',
	'User-Agent': 'darreresBio/0.1.0 (https://github.com/WikimediaCAT/wikidata-pylisting; toniher@wikimedia.cat) Python/3.7',
}
params = { 'query':  query }
response = requests.get('https://query.wikidata.org/sparql', headers=headers, params=params)

c=pd.read_csv(io.StringIO(response.content.decode('utf-8')))

c['article'] = c['article'].apply(lambda x: unquote( x.replace("https://ca.wikipedia.org/wiki/", "") ) )
c['item'] = c['item'].apply( lambda x: x.replace("http://www.wikidata.org/entity/", "") )

# Get stored info
stored = pd.read_sql_query("SELECT * from `bios`", con )

# Merge both subsets centered in actual data
current = pd.merge( c, stored, how='left', on='article' )

# Iterate only entries with null user or timestamp
missing = current[(current[cuser].isnull()) & (current[cdate].isnull())]

for index, row in missing.iterrows():
		titles = row['article']
		result = site.api('query', prop='revisions', rvprop='timestamp|user', rvdir='newer', rvlimit=1, titles=titles )
		for page in result['query']['pages'].values():
				if 'revisions' in page:
						if len( page['revisions'] ) > 0  :
								timestamp = page['revisions'][0]['timestamp']
								userrev = page['revisions'][0]['user']
								print( timestamp )
								print( userrev )
		exit()

print( c )

