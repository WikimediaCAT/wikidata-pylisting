#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pprint
import json
import mwclient
import argparse
import pprint
import time

# Import JSON configuration
parser = argparse.ArgumentParser(description="""Script for adding Autoritat template via API""")
parser.add_argument("-config",help="""Path to a JSON file with configuration options!""")
parser.add_argument("-file",help="""Path to a JSON file with configuration options!""")

args = parser.parse_args()

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

site = mwclient.Site(host, scheme=protocol)
if user and pwd :
		# Login parameters
		site.login(user, pwd)

def update_wiki(site, pagename, content):

    page = site.pages[ pagename ]
    summary = "Afegida plantilla Autoritat"
    if page.can('edit'):
        page.save( content, summary=summary, minor=False, bot=True)

def process_content(content):

    done = 0
    new_content = ""
    plantilla = "{{Autoritat}}\n"

    lines = content.split("\n")

    for line in lines:

        if line.find("[[Categor") >= 0 and done == 0:
            new_content = new_content + plantilla
            done = done + 1
        if line.find("{{Ordena") >= 0 and done == 0:
            new_content = new_content + plantilla
            done = done + 1
        if line.find("utoritat}}") >= 0 and done == 0:
            new_content = new_content
            done = done + 1
            return None
        new_content = new_content + line + "\n"

    return new_content

with open(args.file) as fp:

	for line in fp:

		pageline = line.strip()
		page = site.Pages[pageline]

		content = page.text()
		new_content = process_content( content )
		
		if new_content is None :
			continue

		if ( new_content ) and ( content != new_content ) :
		   update_wiki(site, pageline, new_content)

		print( pageline )
		time.sleep( 1 )
