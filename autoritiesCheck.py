#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import argparse
import pprint
import sqlite3
import time
import re
import csv
import gzip # Using gzip file

pp = pprint.PrettyPrinter(indent=4)

parser = argparse.ArgumentParser(description="""Script for parsing authorities from dumps and DB""")
parser.add_argument("-dump",help="""Path to Dump file""")
parser.add_argument("-authorities",help="""Path to Authorities file""")
args = parser.parse_args()

host = "ca.wikipedia.org"
user = None
password = None
protocol = "https"
data = {}
dbfile = "allbios.db"
authorities = {}

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
