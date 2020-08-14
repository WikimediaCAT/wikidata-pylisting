#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import numpy as np
import io
from urllib.parse import unquote
from urllib import request
import argparse
import pprint
import json
import mwclient
import MySQLdb
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
targetpage = "User:Toniher/Bios"
milestonepage = "Plantilla:NumBios"
targetpagedones = "Viquiprojecte:Viquidones/Progrés"
milestonepagedones = "Plantilla:FitaDones"

checkpage = "User:Toniher/CheckBios"
checkgender = "User:Toniher/CheckGender"

countgenderpage = "User:Toniher/StatsGender"

conn = None

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

if "mysql" in data:
	conn = MySQLdb.connect(host=data["mysql"]["host"], user=data["mysql"]["user"], passwd=data["mysql"]["password"], db=data["mysql"]["database"], use_unicode=True, charset='utf8mb4', init_command='SET NAMES utf8mb4')

if "targetpage" in data:
		targetpage = data["targetpage"]

if "milestonepage" in data:
		milestonepage = data["milestonepage"]

if "checkpage" in data:
		checkpage = data["checkpage"]

site = mwclient.Site(host, scheme=protocol)
if user and pwd :
		# Login parameters
		site.login(user, pwd)

if conn is None:
	print("CONNECTION PROBLEM")
	exit()

cur = conn.cursor()

def checkWikiDataJSON( item, type="iw", lang="ca" ) :

		url = "https://www.wikidata.org/wiki/Special:EntityData/" + item + ".json"

		if type != "iw" :
			print(url)

		req = request.Request(url)

		##parsing response
		r = request.urlopen(req).read()
		cont = json.loads(r.decode('utf-8'))

		##parcing json
		entitycont = cont['entities'][item]

		output  = []

		if type == "label" :
			if 'labels' in entitycont :
				if lang in entitycont['labels'] :
					output.append( entitycont['labels'][lang]['value'] )
		else :
			if 'sitelinks' in entitycont :
				output = list( entitycont['sitelinks'] )

		time.sleep( 0.2 )
		return output


def insertInDB( new_stored, conn ):

		c = conn.cursor()

		for index, row in new_stored.iterrows():

			# Handling Timezone
			row['cdate'] = row['cdate'].replace( "T", " ")
			row['cdate'] = row['cdate'].replace( "Z", "")

			c.execute( "SELECT * from bios where BINARY article = %s ", [ row['article'] ] )
			if c.rowcount > 0:
				print("UPDATE " + row['article'])
				c.execute( "UPDATE `bios` SET `cdate` = %s, `cuser` = %s where BINARY article = %s ", [ row['cdate'], row['cuser'], row['article'] ] )
			else :
				print("INSERT " + row['article'])
				c.execute( "INSERT INTO `bios` (`article`, `cdate`, `cuser`) VALUES (%s, %s, %s)", [ row['article'], row['cdate'], row['cuser'] ] )


		conn.commit()

		return True

def printToWiki( toprint, mwclient, targetpage, milestonepage ):

		count = toprint.shape[0]
		i = 0

		print(count)

		text = "{| class='wikitable sortable' \n!" + "ordre !! " + " !! ".join( toprint.columns.values.tolist() ) + "\n"

		for index, row in toprint.head(100).iterrows():
			num = count - i
			text = text + "|-\n|" + str( num ) + " || " + "[[d:" + row['item'] + "|" + row['item'] + "]]" + " || " + row['genere'] + " || " + " [["+row['article']+"]]" + " || " + str( row['cdate'] )  + " || " +  "{{u|"+str( row['cuser'] ) + "}}" + "\n"
			i = i + 1

		text = text + "|}"

		page = site.pages[ targetpage ]
		page.save( text, summary='Bios', minor=False, bot=True )

		if milestonepage :
			sittext = str( count ) + "\n<noinclude>[[Categoria:Plantilles]]</noinclude>"
			page = site.pages[ milestonepage ]
			page.save( sittext, summary='Bios', minor=False, bot=True )

		return True

def saveToDb( toprint, conn ):

		c = conn.cursor()

		c.execute("DROP TABLE IF EXISTS `wikidata`;")
		c.execute("CREATE TABLE IF NOT EXISTS `wikidata` ( `id` varchar(24), `article` VARCHAR(255) ) default charset='utf8mb4' collate='utf8mb4_bin';")
		c.execute("CREATE INDEX IF NOT EXISTS `idx_unique` ON wikidata (id, article);")
		c.execute("CREATE INDEX IF NOT EXISTS `idx_id` ON wikidata (id);")
		c.execute("CREATE INDEX IF NOT EXISTS `idx_article` ON wikidata (article);")
		c.execute("DROP TABLE IF EXISTS `gender`;")
		c.execute("CREATE TABLE IF NOT EXISTS `gender` ( `id` varchar(24), `gender` VARCHAR(24) ) default charset='utf8mb4' collate='utf8mb4_bin';")
		c.execute("CREATE INDEX IF NOT EXISTS `idx_unique` ON gender (id, gender);")
		c.execute("CREATE INDEX IF NOT EXISTS `idx_id` ON gender (id);")
		c.execute("CREATE INDEX IF NOT EXISTS `idx_gender` ON gender (gender);")

		for index, row in toprint.iterrows():

			if row['genere'] == "nan":
				row['genere'] = None

			c.execute( "INSERT INTO `gender` (`id`, `gender`) VALUES (%s, %s)", [ row['item'], row['genere'] ] )

		conn.commit()

		toprint = toprint.drop_duplicates(subset=['item', 'article'], keep='last')
		for index, row in toprint.iterrows():

			c.execute( "INSERT INTO `wikidata` (`id`, `article`) VALUES (%s, %s)", [ row['item'], row['article'] ] )

		conn.commit()

		return True

def cleanDb( conn ):

	c = conn.cursor()
	c.execute( "SELECT * from wikidata" )

	if c.rowcount > 0:
		# TODO Proceed cleaning
		c.execute( "delete from bios where article in (select b.article from bios b left join wikidata w on b.article=w.article where w.id is null order by b.article ) ;")

	conn.commit()

	return True

def printCheckWiki( toprint, mwclient, checkpage, checkwd=True ):

	if checkwd:
		text = "{| class='wikitable sortable' \n!" + " !! ".join( ['wikidata', 'genere', 'article' ] ) + "!! iwiki !! iwikicount\n"
	else :
		text = "{| class='wikitable sortable' \n!" + " !! ".join( ['wikidata', 'genere', 'article' ] ) + "\n"

	for index, row in toprint.iterrows():
		if checkwd is True:
			iwiki = checkWikiDataJSON( str( row['item'] ), "iw" )
			iwikicount = len( iwiki )
			text = text + "|-\n|" + "[[d:" + str( row['item'] ) + "|" + str( row['item'] ) + "]]" + " || " + str( row['genere'] ) + " || " + " [["+str( row['article'] )+"]]" + " || " + ", ".join( iwiki ) + "|| " + str( iwikicount ) + "\n"
		else :
			text = text + "|-\n|" + "[[d:" + str( row['item'] ) + "|" + str( row['item'] ) + "]]" + " || " + str( row['genere'] ) + " || " + " [["+str( row['article'] )+"]]" + "\n"

	text = text + "|}"

	page = site.pages[ checkpage ]
	page.save( text, summary='Bios', minor=False, bot=True )

	return True


def printCountGenere(toprint, mwclient, checkpage):

	list_generes = []
	text = "{| class='wikitable sortable' \n!" + " !! ".join( ['gènere', 'recompte' ] ) + "\n"

	for index, row in toprint.iterrows():

		genere = "NA"

		if row['genere'] == "unknown" :
			genere = "desconegut"
		else :
			genereA = checkWikiDataJSON(str(row['genere']), "label")
			if len( genereA ) > 0 :
				genere = genereA[0]
			else:
				genere = row['genere']

		list_generes.append(genere)
		text = text + "|-\n| [[" + str( genere ) + "]] || " + str( row['count'] ) + "\n"

	text = text + "|}"

	text = text + "\n----\n"

	text = text + "* {{#expr: {{NumBios}} + 0 }} biografies - [[" + targetpage + "|Seguiment]]\n"
	text = text + "* {{#expr: {{FitaDones}} + 0 }} biografies de dones - [[" + targetpagedones + "|Seguiment]]\n"

	text = text + "\n----\n"

	list_count = map(lambda x: str(x), toprint['count'].tolist())
	text = text + "{{Graph:Chart|width=100|height=100|type=pie|legend=Llegenda|x="+",".join(list_generes)+"|y="+",".join(list_count)+"|showValues=}}"

	page = site.pages[ checkpage ]
	page.save(text, summary='Recompte gènere', minor=False, bot=True)

	return True


cur.execute("CREATE TABLE IF NOT EXISTS `bios` (  `article` VARCHAR(255), `cdate` datetime, `cuser` VARCHAR(255) ) default charset='utf8mb4' collate='utf8mb4_bin';")
cur.execute("CREATE INDEX IF NOT EXISTS `idx_article` ON bios (`article`);")
cur.execute("CREATE INDEX IF NOT EXISTS `idx_cdate` ON bios (`cdate`);")
cur.execute("CREATE INDEX IF NOT EXISTS `idx_cuser` ON bios (`cuser`);")

query = """
SELECT ?item ?genere ?article WHERE {
  ?item wdt:P31 wd:Q5.
  ?article schema:about ?item .
  ?article schema:isPartOf <https://ca.wikipedia.org/> .
  #SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],ca" } .
  OPTIONAL {
    ?item wdt:P21 ?genere .
  }
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
c['genere'] = c['genere'].astype('str')
c['genere'] = c['genere'].apply(lambda x: x.replace("http://www.wikidata.org/entity/", ""))
c['genere'] = c['genere'].apply(lambda x: "unknown" if x.startswith( '_' ) else x)

c['item'] = c['item'].apply( lambda x: x.replace("http://www.wikidata.org/entity/", "") )

# Get stored info
stored = pd.read_sql_query("SELECT * from `bios`", conn)

# Merge both subsets centered in actual data
current = pd.merge(c, stored, how='left', on='article')

# Iterate only entries with null user or timestamp
missing = current[(current['cuser'].isnull()) & (current['cdate'].isnull())]
print("MISSING CUSER OR CDATE")
print(missing)

new_stored = pd.DataFrame(columns = ['article', 'cdate', 'cuser'])

for index, row in missing.iterrows():
		titles = row['article']
		print( titles )
		result = site.api('query', prop='revisions', rvprop='timestamp|user', rvdir='newer', rvlimit=1, titles=titles )
		for page in result['query']['pages'].values():
				if 'revisions' in page:
						if len( page['revisions'] ) > 0  :

								timestamp = None
								userrev = None

								if 'timestamp' in page['revisions'][0] :
										timestamp = page['revisions'][0]['timestamp']
								if 'user' in page['revisions'][0] :
										userrev = page['revisions'][0]['user']

								new_stored = new_stored.append( { 'article': titles, 'cdate': timestamp, 'cuser': userrev  }, ignore_index=True )
								time.sleep( 0.1 )

print("MISSING WITH EXTRA INFO FROM API")
print(new_stored)

# INSERT or REPLACE sqlite new_stored
insertInDB(new_stored, conn)

# Repeat stored
stored2 = pd.read_sql_query("SELECT * from `bios`", conn)

# Merge both subsets centered in actual data
current2 = pd.merge(c, stored2, how='left', on='article')

# Here we list, order and have fun
toprint = current2.sort_values(by='cdate', ascending=False)
toprint = toprint[(toprint['cdate'].notnull())]

clean_duplicates = toprint.drop_duplicates(subset=['item', 'article', 'genere'], keep='last')

printToWiki(clean_duplicates, mwclient, targetpage, milestonepage)

dones = toprint[toprint['genere'] == "Q6581072"]
printToWiki(dones, mwclient, targetpagedones, milestonepagedones)

# We store everything in DB

# Clean a bit
saveToDb(clean_duplicates, conn)
cleanDb(conn)

# Moved pages
printCheckWiki(current2[(current2['cdate'].isnull())], mwclient, checkpage, True)

printCheckWiki(clean_duplicates[clean_duplicates['genere'] == "nan"].sort_values(by='article', ascending=True), mwclient, checkgender, False)

# Print Gender studies
countgenere = clean_duplicates[['item','genere']].groupby('genere')['item'].count().reset_index(name='count').sort_values(['count'], ascending=False)
print(countgenere)

printCountGenere(countgenere, mwclient, countgenderpage)


conn.close()
