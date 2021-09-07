import concurrent.futures
# import datetime
from lxml import etree
import re
import requests
import time

from db_stuff import DBChunk

def fetch_cspace_items(secrets,config,authority,authority_csid,database):
	'''
	This gets the initial paged results from CSpace.
	It is called from main()
	'''
	# set the number of results you want per page
	# hardcoded to default to 1000
	number_results_per_page = config["cspace details"]["query results per page"]

	max_results_limit = config["cspace details"]["cspace max query results"]

	# get the number of items cspace reports being in the authority
	total_items = get_total_number_of_items(secrets,config)

	# get the correct api handler object for API calls
	api_handler = [
		x for x in database.api_handlers if "collectionspace" in x.endpoint
		][0]

	if max_results_limit in ("",None,"null"):
		number_of_full_pages,last_page_num_items = divmod(total_items,number_results_per_page)
	else:
		# if you've set a limit on the number of results you want to deal with
		# in one pass
		number_of_full_pages,last_page_num_items = divmod(max_results_limit,number_results_per_page)
	# last_page = number_of_full_pages+1

	database.chunk_me(
		"cspace",
		0,
		number_of_full_pages,
		number_results_per_page,
		api_handler=api_handler
		)
	if not last_page_num_items == 0:
		database.chunk_me(
			"cspace",
			number_of_full_pages+1,
			number_of_full_pages+1,
			number_results_per_page,
			api_handler=api_handler
			)

	futures = api_handler.run_me()

	for chunk_id,future in futures.items():
		db_chunk = [x for x in database.chunks if x.uuid == chunk_id][0]

		items_in_page,page_items = parse_paged_response_items('items',future.result().content)
		if int(items_in_page) > 0:
			for item in page_items:
				insert_cspace_item(db_chunk,item)
				# print("INSERT")

def insert_cspace_item(db_chunk,item):
	# item is an XML object
	if db_chunk.config["cspace details"]["authority to use"] == "workauthorities":
		data = get_work_data(item)
		data.insert(0,None) # leave space for primary key
		data.extend([None,None,None,None,None,None]) # account for the empty wikidata columns
		insertsql = "INSERT INTO items VALUES (?,?,?,?,?,?,?,?,?,?,?)"

	elif authority == "personauthorities":
		data = cspace_utils.get_person_data(item)
		data.insert(0,None) # leave space for primary key
		data.extend([None,None,None,None,None]) # account for the empty wikidata columns
		insertsql = "INSERT INTO items VALUES (?,?,?,?,?,?,?,?,?)"

	db_chunk.write_to_db(insertsql,data)

def fetch_chunked_cspace_page(db_chunk):
	"""
	This gets a single page of results from an initial cspace query
	All the XML items on the page get inserted to the database
	It's called from the DBChunk object
	"""
	page_query = "{}/{}/{}/items?pgSz={}&pgNum={}".format(
		db_chunk.config["cspace details"]["cspace_services_url"],
		db_chunk.config["cspace details"]["authority to use"],
		db_chunk.config["cspace details"]["authority cspace id"],
		db_chunk.config["cspace details"]["query results per page"],
		db_chunk.cspace_page_number
		)
	print(page_query)
	try:
		db_chunk.api_handler.feed_me(
			db_chunk,
			page_query,
			(
				db_chunk.secrets['username'],
				db_chunk.secrets['password']
			)
		)
	except Exception as e:
		print(e)
		# r = requests.get(page_query,auth=(db_chunk.secrets['username'],db_chunk.secrets['password']))
		# r.raise_for_status()
		# status = True


		# THE FOLLOWING STUFF NEEDS TO BE SPLIT OUT
	# 	items_in_page,page_items = parse_paged_response_items('items',r.content)
	# 	# page_number += 1
	# 	# print(page_items)
	# 	if int(items_in_page) > 0:
	# 		for item in page_items:
	# 			db_chunk.insert_cspace_item(item)
	# 			print("INSERT")
	# 	# this looks for the number of items you want from a cspace query request
	# 	# if int(items_in_page) < number_results_per_page:
	# 	while True:
	# 		try:
	# 			db_chunk.connection.commit()
	# 			break
	# 		except Exception as e:
	# 			print("INSERT ERROR")
	# 			time.sleep(.2) # wait for db to unlock
	# except requests.exceptions.RequestException as e:
	# 	status = False
	# 	print(e)
	#
	# return status

def get_total_number_of_items(secrets,config):
	# do a first query to figure out how many total items there are in the authority
	# just retrieve the first page (0) and ask for one item (pgSz=1)
	# then use the totalItems element to get the total number
	initial_query = "{}/{}/{}/items?pgSz=1&pgNum=0".format(
		config["cspace details"]["cspace_services_url"],
		config["cspace details"]["authority to use"],
		config["cspace details"]["authority cspace id"]
		)
	try:
		print("Getting the total number of items in the authority.")
		r = requests.get(initial_query,auth=(secrets['username'],secrets['password']))
		r.raise_for_status()
		total_items = parse_paged_response_items('top level',r.content)
	except requests.exceptions.RequestException as e:
		print(e)
		total_items = None

	return total_items

############ STEP 2 ####################

def enrich_cspace_items(secrets,config,database):
	start_id = 1
	chunk_size = config['database details']['chunk_size']
	target = 'cspace items'
	print("Starting to enrich fetched CSpace items.")
	print(target,start_id,database.rows_in_db,chunk_size)

	# get the correct api handler object for API calls
	api_handler = [
		x for x in database.api_handlers if "collectionspace" in x.endpoint
		][0]

	database.chunk_me(
		target,
		start_id,
		database.rows_in_db,
		chunk_size,
		api_handler=api_handler
		)

def get_chunked_cspace_items(db_chunk):
	'''
	This gets a chunk of cspace items as XML from cspace and retrieves extra data
	to be used for reconciliation.
	It's called from the DBChunk class
	'''
	authority = db_chunk.config['cspace details']['authority to use']
	status = None
	api_handler = db_chunk.api_handler
	if authority == 'workauthorities':
		uris_sql = "SELECT uri, id FROM items WHERE id BETWEEN {} AND {}".format(db_chunk.chunk_start,db_chunk.chunk_end)
		uris = db_chunk.query_db(uris_sql)
		# print(uris)
		for item in uris:
			uri = item[0]
			id = item[1]
			item_cspace_query = "{}{}".format(
				db_chunk.config["cspace details"]["cspace_services_url"],
				uri
				)
			# print(uri)
			try:
				db_chunk.api_handler.feed_me(
					db_chunk,
					page_query,
					(
						db_chunk.secrets['username'],
						db_chunk.secrets['password']
					)
				)
			except Exception as e:
				print(e)
		futures = api_handler.run_me()

		for chunk_id,future in futures.items():
			db_chunk = [x for x in db_chunk.database.chunks if x.uuid == chunk_id][0]
			title_list,date_list = parse_single_work_item(future.result().content)

			if not title_list == []:
				insertable = " | ".join(title_list)
				insert_sql = '''\
					UPDATE items SET alt_titles=? WHERE id=?
					'''
				db_chunk.write_to_db(insert_sql,(insertable,id))
			if not date_list == []:
				insertable = " | ".join(date_list)
				insert_sql = '''\
					UPDATE items SET year=? WHERE id=?
					'''
				db_chunk.write_to_db(insert_sql,(insertable,id))

def parse_single_work_item(response):
	'''
	This grabs extra data from a single item XML returned from CSpace
	'''
	tree = etree.XML(response)
	work_data = tree.find('{http://collectionspace.org/services/work}works_common')
	titles = work_data.find('./workTermGroupList')
	title_list = []
	if not titles == []:
		for element in titles:
			title = element.findtext('termName')
			# print(title)
			title_list.append(title)
	dates = work_data.find('./workDateGroupList')
	date_list = []
	if not dates == []:
		# print(dates)
		for element in dates:
			# print(element)
			date = element.findtext('dateDisplayDate')
			date_list.append(date)

	return title_list,date_list

def parse_single_person_item():
	pass

def parse_paged_response_items(operation,response):
	'''
	This parses a page of results from an initial CSpace query
	operation = what you want the parsing to do:
	* return all the items in the page
	* return top-level info from the page header
	'''
	try:
		root = etree.XML(response)
	except:
		print(response)

	if operation == 'items':
		items_in_page = root.findtext('itemsInPage')
		if not root.find('list-item') == None:
			page_items = []
			for item in root.findall('list-item'):
				page_items.append(item)

		return(items_in_page,page_items)
	elif operation == 'top level':
		total_items = root.findtext('totalItems')
		if not total_items in (None,[],''):
			total_items = int(total_items)
		else:
			total_items = 0

		return total_items

def get_work_data(item):
	'''
	This gets data from a single listed XML item in a page of CSpace query results
	'''
	csid = item.findtext('csid')
	# print(csid)
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
	data = [csid,uri,title,creator]
	# print(data)
	return data

def get_person_data(item):
	pass

def get_authority_data(item):
	pass

def get_object_data(item):
	pass
