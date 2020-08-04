#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import argparse
import pprint
import sqlite3
import MySQLdb
import time
import re
import json
import csv
import gzip # Using gzip file

pp = pprint.PrettyPrinter(indent=4)

parser = argparse.ArgumentParser(description="""Script for parsing authorities from dumps and DB""")
parser.add_argument("-dump",help="""Path to Dump file""")
parser.add_argument("-authorities",help="""Path to Authorities file""")
parser.add_argument("-config",help="""Path to a JSON file with configuration options!""")
args = parser.parse_args()

host = "ca.wikipedia.org"
user = None
password = None
protocol = "https"
data = {}
dbfile = "allbios.db"
authorities = {}
conn = None

if "config" in args:
    if args.config is not None:
        with open(args.config) as json_data_file:
            data = json.load(json_data_file)

if "mysql" in data:
	dbfile = None
	conn = MySQLdb.connect(host=data["mysql"]["host"], user=data["mysql"]["user"], passwd=data["mysql"]["password"], db=data["mysql"]["database"])

if "dbfile" in data:
	dbfile = data["dbfile"]
	conn = sqlite3.connect( dbfile )

if conn is not None:
	exit()

def addToDb( id, props, conn ):

    c = conn.cursor()
    records = []

    for prop in props:
        records.append( ( id, prop ) )

    c.executemany( "INSERT INTO `authorities` (`id`, `authority`) VALUES (?, ?)", records )

    conn.commit()

    return True


if "authorities" in args:
    if args.authorities is not None:
        with open(args.authorities) as authorities_file:
            csvreader = csv.reader(authorities_file, delimiter='\t')
            for row in csvreader:
                authorities[row[2]] = 1

#pp.pprint( authorities )

if "dump" in args:
    if args.dump is not None:

        conn = sqlite3.connect( dbfile )
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS `authorities`;")
        cur.execute("CREATE TABLE IF NOT EXISTS `authorities` (  `id` VARCHAR(25), `authority` VARCHAR(25) ) ;")
        cur.execute("CREATE INDEX idx_authorities ON authorities (authority);")

        with gzip.open(args.dump,'rt') as f:
            for line in f:
                detectid = re.findall( r'\"id\":\"(Q\d+)\"', line )
                if len( detectid ) > 0:
                    id = detectid[0]
                    # print( id )
                    listp = re.findall( r'\"(P\d+)\"', line )
                    authp = []
                    for prop in listp:
                        if prop in authorities:
                            authp.append( prop )
                    # pp.pprint( authp )
                    if len( authp ) > 0:
                        addToDb( id, authp, conn )
