import requests
import pandas as pd
import numpy as np
import io
from urllib.parse import unquote
import argparse
import pprint
import json
import mwclient

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

site = mwclient.Site((protocol, host))
if user and pwd :
		# Login parameters
		site.login(user, pwd)

query = """
SELECT ?item ?itemLabel ?article WHERE {
  ?item wdt:P31 wd:Q5.
  ?item wdt:P21 wd:Q6581072 .
  ?article schema:about ?item .
  ?article schema:isPartOf <https://ca.wikipedia.org/> .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],ca" } .
} ORDER BY ?article
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

creation_date = np.zeros( ( len(c) ), dtype='datetime64' )

for g, df in c.groupby(np.arange(len(c)) // 50):
		titles = df['article'].str.cat(sep="|")
		result = site.api('query', prop='revisions', rvprop='timestamp|user', rvdir='newer', rvlimit=1, titles=titles )
		for page in result['query']['pages'].values():
				if 'revisions' in page:
						print( '{}'.format(page['title'].encode('utf8') ) )
						pp.pprint( page['revisions'] )
		exit()

print( c )

