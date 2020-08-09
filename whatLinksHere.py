#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import pprint
import MySQLdb
import json
import mwclient

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


if conn is None:
    print("NO CONNECTION")
    exit()


def addToDb( id, records, conn, iter ):

    c = conn.cursor()

    c.executemany( "INSERT INTO `whatlinks` (`article`, `against`) VALUES ( %s, %s )", records )

    conn.commit()

    return True


if "title" in args:
    if args.title is not None:

        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS `whatlinks` (  `article` VARCHAR(255), `against` VARCHAR(255), PRIMARY KEY (`article`, `against`) ) ;")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_article ON whatlinks (`article`);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_against ON whatlinks (`against`);")

        # Example syntax: https://en.wikipedia.org/w/api.php?action=query&format=json&list=backlinks&bltitle=philosophy&blnamespace=0&bllimit=100&blcontinue=0|10374
        records = []
        addToDb( id, records, conn, iter )

        conn.commit()
