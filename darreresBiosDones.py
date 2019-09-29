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
import time

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
targetpage = "User:Toniher/proves"
milestonepage = "Plantilla:FitaDones"

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
		
if "targetpage" in data:
		targetpage = data["targetpage"]
		
if "milestone" in data:
		milestone = data["milestone"]

site = mwclient.Site(host, scheme=protocol)
if user and pwd :
		# Login parameters
		site.login(user, pwd)

conn = sqlite3.connect( dbfile )
cur = conn.cursor()

def insertInDB( new_stored, conn ):
		
		c = conn.cursor()
		
		for index, row in new_stored.iterrows():

				c.execute( "INSERT INTO `bios`(`article`, `cdate`, `cuser`) VALUES (?, ?, ?)", [ row['article'], row['cdate'], row['cuser'] ] )
		
		
		conn.commit()
		
		return True
	
def printToWiki( toprint, mwclient, targetpage ):
	
		count = toprint.shape[0]
		i = 0
		
		print( count )

		text = "{| class='wikitable sortable' \n!" + "ordre !! " + " !! ".join( toprint.columns.values.tolist() ) + "\n"

		for index, row in toprint.head(100).iterrows():
				num = count - i			
				text = text + "|-\n|" + str( num ) + " || " + "[[d:" + row['item'] + "|" + row['item'] + "]]" + " || " + row['itemLabel'] + " || " + " [["+row['article']+"]]" + " || " + row['cdate']  + " || " +  "{{u|"+str( row['cuser'] ) + "}}" + "\n"
				i = i + 1
			
		text = text + "|}"
		
		print( text )
		page = site.pages[ targetpage ]
		page.save( text, summary='Bios', minor=False, bot=True )
		
		if milestonepage :
			sittext = count 
			page = site.pages[ milestonepage ]
			page.save( sittext, summary='Bios', minor=False, bot=True )
		
		return True

cur.execute("CREATE TABLE IF NOT EXISTS `bios` (  `article` VARCHAR(255) NOT NULL PRIMARY KEY, `cdate` datetime NULL, `cuser` VARCHAR(255) NULL  ) ;")

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

# Get stored info
stored = pd.read_sql_query("SELECT * from `bios`", conn )

# Merge both subsets centered in actual data
current = pd.merge( c, stored, how='left', on='article' )

# Iterate only entries with null user or timestamp
missing = current[(current['cuser'].isnull()) & (current['cdate'].isnull())]

new_stored = pd.DataFrame( columns = [ 'article', 'cdate', 'cuser' ] )

for index, row in missing.iterrows():
		titles = row['article']
		result = site.api('query', prop='revisions', rvprop='timestamp|user', rvdir='newer', rvlimit=1, titles=titles )
		for page in result['query']['pages'].values():
				if 'revisions' in page:
						if len( page['revisions'] ) > 0  :

								timestamp = page['revisions'][0]['timestamp']
								userrev = page['revisions'][0]['user']

								new_stored = new_stored.append( { 'article': titles, 'cdate': timestamp, 'cuser': userrev  }, ignore_index=True )
								time.sleep( 0.1 )

print( new_stored )

# Send to SQLite
# INSERT or REPLACE sqlite new_stored -> Need specific function here
insertInDB( new_stored, conn )

# Repeat stored
stored2 = pd.read_sql_query("SELECT * from `bios`", conn )

# Merge both subsets centered in actual data
current2 = pd.merge( c, stored2, how='left', on='article' )

# Here we list, order and have fun
toprint = current2.sort_values(by='cdate', ascending=False )
printToWiki( toprint[(toprint['cuser'].notnull()) ], mwclient, targetpage, milestone )

# Moved pages
print( current2[(current2['cuser'].isnull()) ] )

conn.close()
