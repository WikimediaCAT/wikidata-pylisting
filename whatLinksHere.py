#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import pprint
import MySQLdb
import json
import mwclient
import time

pp = pprint.PrettyPrinter(indent=4)

parser = argparse.ArgumentParser(description="""Script for retrieving and storing whatlinks""")
parser.add_argument("-title",help="""Page title""")
parser.add_argument("-config",help="""Path to a JSON file with configuration options!""")
args = parser.parse_args()

data = {}
whatlinks = {}
mysqlmode = False

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
    mysqlmode = True
    conn = MySQLdb.connect(host=data["mysql"]["host"], user=data["mysql"]["user"], passwd=data["mysql"]["password"], db=data["mysql"]["database"], use_unicode=True, charset='utf8', init_command='SET NAMES UTF8')

site = mwclient.Site(host, scheme=protocol)
if user and pwd :
		# Login parameters
		site.login(user, pwd)


if conn is None:
    print("NO CONNECTION")
    exit()


def getBackLinks( site, title, links, cont=None):

    # syntax: https://ca.wikipedia.org/w/api.php?action=query&format=json&list=embeddedin&eititle=Plantilla:Autoritat&eilimit=500&einamespace=0
    if cont:
        result = site.api('query', list='embeddedin', eititle=args.title, einamespace=0, eilimit=500, eicontinue=cont)
    else:
        result = site.api('query', list='embeddedin', eititle=args.title, einamespace=0, eilimit=500)

    # pp.pprint(result['query']['embeddedin'])
    for page in result['query']['embeddedin']:
        if 'title' in page:
            title = page["title"].replace(" ", "_")
            links.append(title)

    if 'continue' in result:
        if 'eicontinue' in result['continue']:
            cont = result['continue']['eicontinue']
            links = getBackLinks(site, title, links, cont)

    time.sleep(0.2)
    return links


def addToDb(records, conn):

    c = conn.cursor()

    c.executemany("INSERT INTO `whatlinks` (`article`, `against`) VALUES ( %s, %s )", records)

    conn.commit()

    return True


if "title" in args:
    if args.title is not None:

        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS `whatlinks` (  `article` VARCHAR(255), `against` VARCHAR(255), PRIMARY KEY (`article`, `against`) ) default charset='utf8mb4' collate='utf8mb4_bin';")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_article ON whatlinks (`article`);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_against ON whatlinks (`against`);")

        cur.execute("DELETE from `whatlinks` where `against` = \""+args.title+"\"")

        links = []
        links = getBackLinks(site, args.title, links, None)

        records = []
        links = sorted(set(links))

        for link in links:
            records.append([link, args.title])

        addToDb(records, conn)

        conn.commit()
