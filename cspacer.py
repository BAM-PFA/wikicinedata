import argparse
import csv
import html
import json
from lxml import etree
import re
import requests
import sqlite3

def set_args():
	parser = argparse.ArgumentParser()
	parser.add_argument(
		'-a','--authority',
		help='which cspace authority do you want to get items from'
		)
	parser.add_argument(
		'-c','--authority_csid',
		help="what is the authority's CSpace ID (i.e. the CSID for the 'persons' authority itself not an individual person record)"
		)

	return parser.parse_args()

def parse_response_xml(response):
	tree = etree.XML(response)
	# page_number = tree.findtext('pageNum')
	items_in_page = tree.findtext('itemsInPage')
	if not tree.find('list-item') == None:
		page_items = []
		for item in tree.findall('list-item'):
			page_items.append(item)

	return(items_in_page,page_items)

def insert_item(authority,item,cursor):
	if authority == "workauthorities":
		data = get_work_data(item)
		data.append(None) # account for the empty alt title column
		insertsql = "INSERT INTO items VALUES (?,?,?,?)"

	elif authority == "personauthorities":
		data = get_person_data(item)
		insertsql = "INSERT INTO items VALUES (?,?,?)"

	cursor.execute(insertsql,data)

def fetch_cspace_items(secrets,authority,authority_csid,cursor,conn):
	page_number = 0
	last_page = False
	# all_items = []
	if authority == "workauthorities":
		sql = "CREATE TABLE items (csid, title, creator, alt titles)"
		cursor.execute(sql)

	elif authority == "personauthorities":
		sql = "CREATE TABLE items (csid, name, dates)"
		pass

	while last_page == False:
		items_query = "{}/{}/{}/items?pgSz=10&pgNum={}".format(
			secrets["cspace_services_url"],
			authority,
			authority_csid,
			page_number
			)
		print(items_query)
		r = requests.get(items_query,auth=(secrets['username'],secrets['password']))
		items_in_page,page_items = parse_response_xml(r.content)
		page_number += 1
		print(page_items)
		if int(items_in_page) > 0:
			for item in page_items:
				insert_item(authority,item,cursor)
		# if int(items_in_page) < 10:	# this is the hardcoded number of items per page
		if page_number == 2:
			last_page = True
			break
	conn.commit()

def get_work_data(item):
	csid = item.findtext('csid')
	# print(csid)
	creator = item.findtext('creator')
	# strip unnecessary cspace junk
	try:
		creator = re.match(".*'(.+)'",creator).groups()[0]
	except:
		pass
	# print(creator)
	title = item.findtext('termDisplayName')
	# print(title)
	data = [csid,creator,title]
	# print(data)
	return data

def get_person_data(item):
	pass

def get_authority_data(item):
	pass

def get_object_data(item):
	pass

# def write_csv(all_items,authority,cursor,conn):
# 	# with open('out.xml','w') as f:
# 	# 	for item in all_items:
# 			# f.write(html.unescape(etree.tostring(item, pretty_print=True).decode()))
# 	if authority == "workauthorities":
# 		headers = ["csid","creator","title"]
# 		sql = "CREATE TABLE items (csid, title, creator, alt titles)"
# 		for item in all_items:
# 			data = get_work_data(item)
# 			all_data.append(data)
# 	elif authority == "personauthorities":
# 		headers = ["csid","name","dates"]
# 		data = get_person_data(item)
# 		pass
#
# 	with open('out.csv','w') as f:
# 		writer = csv.writer(f)
# 		writer.writerow(headers)
# 		for item in all_data:
# 			writer.writerow(item)



def main():
	args = set_args()
	authority = args.authority
	authority_csid = args.authority_csid
	with open('secrets.json','r') as f:
		secrets = json.load(f)
	db = "items.sqlite"
	conn = sqlite3.connect(db)
	cursor = conn.cursor()

	fetch_cspace_items(secrets,authority,authority_csid,cursor,conn)

	# write_csv(all_items,authority)

if __name__=='__main__':
	main()
