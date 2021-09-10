import argparse
import csv
import json
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

def run_cspace_queries(database):
	# set up cspace details from config.json
	try:
		authority = database.config['cspace details']['authority to use']
		authority_csid = database.config['cspace details']['authority cspace id']
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


	cspace_utils.fetch_cspace_items(
		database.secrets,
		database.config,
		authority,
		authority_csid,
		database
		)
	database.db_writer.run_me()
	rows = database.count_me()
	if not rows:
		print("Something is up with the database or the initial api query. Try again!")
		sys.exit()

	cspace_api_handler.clean_me()

	# get some additional data points for matching/reconciliation
	cspace_utils.enrich_cspace_items(database.secrets,database.config,database)
	cspace_api_handler.clean_me()

	database.db_writer.run_me()

def run_wikidata_queries(database):
	wd_api_handler = api_utils.APIHandler("wikidata")
	database.api_handlers.append(wd_api_handler)

	wikidata_utils.reconcile_items(database.config,database)
	database.db_writer.run_me()
	wd_api_handler.clean_me()

	wikidata_utils.refine_matches(database)

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
		run_cspace_queries(database)
	elif mode == 'csv':
		pass

	run_wikidata_queries(database)

if __name__=='__main__':
	main()
