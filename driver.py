import argparse
import csv
import html
import json
from lxml import etree
import re
import requests
import sqlite3
import sys
# local imports
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

	cspace_utils.fetch_cspace_items(secrets,authority,authority_csid,database)
	wikidata_utils.reconcile_items(database)

	# write_csv(all_items,authority)

if __name__=='__main__':
	main()