import argparse
import csv
import json
from lxml import etree
import re
import requests
import sqlite3
import sys
import time
# local imports
import api_utils
import cspace_utils
import db_stuff
import wikidata_utils

def set_args():
	parser = argparse.ArgumentParser()
	parser.add_argument(
		'-m','--mode',
		choices=['cspace','csv'],
		default='cspace',
		help='what mode? cspace (extract from cspace) or csv. dfault = cspace'
		)

	return parser.parse_args()

def main():
	args = set_args()
	mode = args.mode

	with open('secrets.json','r') as f:
		secrets = json.load(f)
	with open('config.json','r') as f:
		config = json.load(f)
	db_path = "items.sqlite"

	database = db_stuff.Database(db_path)
	database.secrets = secrets
	database.config = config
	database.connection = sqlite3.connect(db_path)
	database.cursor = database.connection.cursor()

	if mode == 'cspace':
		try:
			authority = config['cspace details']['authority to use']
			authority_csid = config['cspace details']['authority cspace id']
			try:
				database.create_cspace_table(authority)
			except:
				print("problem w the database file??")
				sys.exit()
		except KeyError:
			print("You need to set up the config file correctly, read the readme.")
			sys.exit()

		# set up API handler objects
		cspace_api_handler = api_utils.APIHandler("collectionspace")
		database.api_handlers.append(cspace_api_handler)
		wd_api_handler = api_utils.APIHandler("wikidata")
		database.api_handlers.append(wd_api_handler)

		cspace_utils.fetch_cspace_items(
			secrets,
			config,
			authority,
			authority_csid,
			database
			)
		database.db_writer.run_me()
		rows = database.count_me()
		while rows < 1:
			# it might take a minute to write?
			time.sleep(1)
			rows = database.count_me()
			print(rows)

		cspace_api_handler.clean_me()

		# get some additional data points for matching/reconciliation
		cspace_utils.enrich_cspace_items(secrets,config,database)
		cspace_api_handler.clean_me()

		database.db_writer.run_me()

		# call the wikidata reconciliation api
		wikidata_utils.reconcile_items(config,database)
		wd_api_handler.clean_me()

		database.db_writer.run_me()

	else:
		# mode == csv
		pass
	wikidata_utils.reconcile_items(config,database)

	# write_csv(all_items,authority)

if __name__=='__main__':
	main()
