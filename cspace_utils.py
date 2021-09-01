import concurrent.futures
from lxml import etree
import re
import requests
import time

from db_stuff import DBChunk

import datetime

def fetch_cspace_items(secrets,config,authority,authority_csid,database):
	page_number = 0
	last_page = False

	while last_page == False:
		items_query = "{}/{}/{}/items?pgSz=10&pgNum={}".format(
			config["cspace details"]["cspace_services_url"],
			authority,
			authority_csid,
			page_number
			)
		# print(items_query)
		r = requests.get(items_query,auth=(secrets['username'],secrets['password']))
		items_in_page,page_items = parse_paged_response_items(r.content)
		page_number += 1
		# print(page_items)
		if int(items_in_page) > 0:
			for item in page_items:
				database.insert_cspace_item(authority,item)
		# if int(items_in_page) < 1000:	# this hardcodes the number of items per result page to 1000
		if page_number == 2:
			last_page = True
			break
	database.connection.commit()

def enrich_cspace_items(secrets,config,database):
	authority = config['cspace details']['authority to use']
	rows_in_db_sql = "SELECT COUNT(id) FROM items;"
	rows_in_db = database.cursor.execute(rows_in_db_sql).fetchall()[0][0]
	# print(rows_in_db)
	start_id = 1
	chunk_size = config['database details']['chunk_size']

	with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
		'''
		I found I had to limit the number of threads operating at once to
		avoid segmentation faults, i/o errors, max http request errors, etc.
		'''
		for chunk_start in range(start_id, rows_in_db, chunk_size):
			chunk_end = chunk_start + chunk_size - 1
			print(chunk_start,chunk_end)
			chunk = DBChunk(
				'cspace',
				secrets,
				config,
				database.filepath,
				chunk_start,
				chunk_end
				)
			# chunk.daemon = True
			executor.submit(chunk.start())
			# chunk.join()
			# chunk.shutdown_flag.set()

def get_chunked_cspace_items(db_chunk):
	authority = db_chunk.config['cspace details']['authority to use']
	if authority == 'workauthorities':
		uris_sql = "SELECT uri, id FROM items WHERE id BETWEEN {} AND {}".format(db_chunk.chunk_start,db_chunk.chunk_end)
		uris = db_chunk.query_db(uris_sql)
		print(uris)
		for item in uris:
			uri = item[0]
			id = item[1]
			item_cspace_query = "{}{}".format(
				db_chunk.config["cspace details"]["cspace_services_url"],
				uri
				)
			r = requests.get(
				item_cspace_query,
				auth=(db_chunk.secrets['username'],db_chunk.secrets['password'])
				)

			title_list,date_list = parse_single_item(r.content)

			if not title_list == []:
				insertable = " | ".join(title_list)
				insert_sql = '''\
					UPDATE items SET alt_titles=? WHERE id=?
					'''
				db_chunk.write_to_db(insert_sql,(insertable,id))

			# print(datetime.datetime.now())

def parse_single_item(response):
	tree = etree.XML(response)
	work_data = tree.find('{http://collectionspace.org/services/work}works_common')
	titles = work_data.find('./workTermGroupList')
	title_list = []
	if not titles == []:
		for element in titles:
			title = element.findtext('termName')
			print(title)
			title_list.append(title)
	dates = work_data.find('./workDateGroupList')
	date_list = []
	if not dates == []:
		print(dates)
		for element in dates:
			print(element)
			date = element.findtext('dateDisplayDate')
			date_list.append(date)

	return title_list,date_list

def parse_paged_response_items(response):
	tree = etree.XML(response)
	items_in_page = tree.findtext('itemsInPage')
	if not tree.find('list-item') == None:
		page_items = []
		for item in tree.findall('list-item'):
			page_items.append(item)

	return(items_in_page,page_items)

def get_work_data(item):
	csid = item.findtext('csid')
	print(csid)
	uri = item.findtext('uri')
	creator = item.findtext('creator')
	try:
		# strip unnecessary cspace junk
		creator = re.match(".*'(.+)'$",creator).groups()[0]
	except:
		pass
	# print(creator)
	title = item.findtext('termDisplayName')
	# print(title)
	data = [csid,uri,creator,title]
	# print(data)
	return data

def get_person_data(item):
	pass

def get_authority_data(item):
	pass

def get_object_data(item):
	pass
