from lxml import etree
import re
import requests

def fetch_cspace_items(secrets,authority,authority_csid,database):
	page_number = 0
	last_page = False

	while last_page == False:
		items_query = "{}/{}/{}/items?pgSz=10&pgNum={}".format(
			secrets["cspace_services_url"],
			authority,
			authority_csid,
			page_number
			)
		# print(items_query)
		r = requests.get(items_query,auth=(secrets['username'],secrets['password']))
		items_in_page,page_items = parse_response_xml(r.content)
		page_number += 1
		# print(page_items)
		if int(items_in_page) > 0:
			for item in page_items:
				database.insert_cspace_item(authority,item)
		# if int(items_in_page) < 10:	# this is the hardcoded number of items per page
		if page_number == 2:
			last_page = True
			break
	database.connection.commit()

def parse_response_xml(response):
	tree = etree.XML(response)
	# page_number = tree.findtext('pageNum')
	items_in_page = tree.findtext('itemsInPage')
	if not tree.find('list-item') == None:
		page_items = []
		for item in tree.findall('list-item'):
			page_items.append(item)

	return(items_in_page,page_items)

def get_work_data(item):
	csid = item.findtext('csid')
	print(csid)
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
