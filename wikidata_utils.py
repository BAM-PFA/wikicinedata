import json

###########################################
##########    STEP ONE   ##################
#
#  • query the wikidata reconciliation service endpoint
#    with a chunk_size batch of db records
#  • parse the items returned as JSON from wikidata
#  • insert match information (if any; only taking the top match)
#    to the db record in question
#
def reconcile_items(config,database):
	'''
	This splits the database into :chunk_size records and creates a call to
	the wikidata reconciliation service for each row. The chunk of rows is
	sent as a single batch query to wikidata.
	it's called from main()
	'''
	item_type_id = config["wikidata details"]["item type to reconcile"]
	start_id = 1
	chunk_size = config['database details']['chunk_size']
	target = 'wikidata'
	secrets = None # don't need secrets for wikidata

	# get the correct api handler object for API calls
	api_handler = [
		x for x in database.api_handlers if "wikidata" in x.endpoint
		][0]

	database.chunk_me(
		target,
		start_id,
		database.rows_in_db,
		chunk_size,
		api_handler=api_handler
		)
	for chunk in database.chunks:
		chunk.join()

	futures = api_handler.run_me()
	for future,chunk_id in futures.items():
		db_chunk = [x for x in database.chunks if x.uuid == chunk_id][0]
		wikidata_response = json.loads(future.result().content)
		print(wikidata_response)
		parse_reconciled_batch(wikidata_response,db_chunk)

def reconcile_chunked_items(db_chunk):
	'''
	For a chunk of the database (rows between two id numbers)
	create a query based on the existing data
	'''
	item_type_id = db_chunk.config["wikidata details"]["item type to reconcile"]
	item_type_label = db_chunk.config["wikidata details"]["item type label"]
	batch_query_dict = {}
	if item_type_label == 'film':
		data_points_sql = "SELECT title, creator, year, id FROM items WHERE id BETWEEN ? AND ?;"
		data_points = db_chunk.query_db(data_points_sql,(db_chunk.chunk_start,db_chunk.chunk_end))
		# print(data_points)
		for item in data_points:
			# get the relevant data points

			title = item[0]
			print("Getting Wikidata data for "+title)
			creator = item[1]
			year = item[2]
			id = item[3]
			# build the sub query for an individual item
			query = {
				"query":title,
				"type":item_type_id
				}
			if year or creator:
				query["properties"] = []
				if year:
					query["properties"].append({
					"pid":"P577", # property id for "date of publication"
					"v":year
					})
				if creator:
					query["properties"].append({
					"pid":"P57", # property id for "director"
					"v":creator
					}
					)
			# add the query to the batch w/ q+id as key (q1,q2,q3,etc.)
			batch_query_dict["q{}".format(str(id))] = query
		# print(batch_query_dict)
	elif item_type_label == "human":
		pass

	query_payload = json.dumps({"queries":json.dumps(batch_query_dict)})

	# try:
	db_chunk.api_handler.feed_me(
		db_chunk.uuid,
		db_chunk.config['wikidata details']['wikidata reconciliation endpoint'],
		auth=None,
		data=json.loads(query_payload),
		header=None
	)

def parse_reconciled_batch(wikidata_response,db_chunk):
	for query,result in wikidata_response.items():
		# print(query)
		# print(result)
		if not result["result"] == []:
			# i.e. if there was no match at all in wikidata

			item_id = int(query.replace("q",""))
			top_match = result["result"][0]
			if not top_match['type'] == []:
				top_match_type = top_match['type'][0]['id']
			else:
				top_match_type = None
			intended_match_type = db_chunk.config['wikidata details']['item type to reconcile']
			if top_match_type == intended_match_type or top_match_type == None:
				# first filter out false positives that are obviously wrong

				# ACTUALLY there could be subtypes listed like "short film = Q24862" instead of "film/Q11424"
				# maybe leave this filter out

				top_match_score = top_match["score"]
				if top_match_score > 25:
					# now filter out low-quality matches
					top_match_Qid = top_match["id"]
					top_match_label = top_match["name"]
					# print(top_match_label)
					top_match_is_match = top_match["match"]

					update_sql = '''\
					UPDATE items SET \
					top_match_is_match=?, \
					top_match_score=?, \
					top_match_label=?, \
					top_match_Qid=? \
					WHERE id=?;
					'''
					values = (
						top_match_is_match,
						top_match_score,
						top_match_label,
						top_match_Qid,
						item_id
						)
					# print(update_sql,values)
					db_chunk.write_to_db(update_sql,values)

###########################################
##########    STEP TWO   ##################
#
#  • query wikidata itself (not the reconciliation service) with
#    QID values from top matches that are not 100%
#  • we're only interested in matches that are between 30%-99% confidence
#  • parse the items returned as JSON from wikidata
#  • using selected additional data points
#    (director/creator; country; language; aliases?) see if any of the
#    less than 100% matches can be refined
#

def requery_batch_sparql():

	property_sparql="""\
	SELECT ?itemQID ?propertyLabel ?property \
	WHERE {{ \
	values ?itemQID {{ {} }} \
	OPTIONAL {{ ?itemQID wdt:{} ?property }} \
	SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en" }} \
	}}\
	""".format(qid_list,property_PID)



def refine_matches(database):
	get_matches_to_refine_sql = """\
	SELECT top_match_Qid from items \
	WHERE top_match_score between 30 and 99.9;
	"""
	qid_list = []
	chunk_size = database.config["database details"]["chunk_size"]
	api_handler = [
		x for x in database.api_handlers if "wikidata" in x.endpoint
		][0]
	matches = database.connection.execute(get_matches_to_refine_sql).fetchall()
	for match in matches:
		qid = match[0]
		qid_list.append(qid)
	if not qid_list == []:
		database.chunk_me(
			"wikidata requery",
			0,
			len(qid_list),
			chunk_size,
			api_handler=api_handler,
			requery_list=qid_list
		)
	for chunk in database.chunks:
		chunk.join()

	futures = api_handler.run_me()
	for future,chunk_id in futures.items():
		print(type(future.result().content))
		print(future.result().content)
		db_chunk = [x for x in database.chunks if x.uuid == chunk_id][0]
		wikidata_response = json.loads(future.result().content)
		print(wikidata_response)
		parse_requery_batch(wikidata_response,db_chunk)

def requery_qid_batch(db_chunk):
	"""
	Given a list of wikidata QIDs search for extra data points.
	To be used for matches on items with scores between 30-99
	"""
	qid_chunk = db_chunk.database.requery_list[
		db_chunk.chunk_start:db_chunk.chunk_end
		]
	# print(qid_chunk)
	ids = "|".join(qid_chunk)
	wikidata_entities_url = ("https://www.wikidata.org/w/api.php?action=wbgetentities&ids={}&props=labels|claims&format=json").format(ids)
	# print(wikidata_entities_url)
	db_chunk.api_handler.feed_me(
		db_chunk.uuid,
		wikidata_entities_url,
		auth=None,
		data=None,
		header={'Accept-Encoding':'gzip'}
		)
	# from the results, this is the dict with all the actual data points:
	# api_response_json['entities'][QID]['claims']
	# dict_keys(['P31' (instance of), 'P495' (country of origin), 'P345' (imdb id), 'P364'(original language),
	# 'P577'(publication date), 'P3445', 'P344'(dir photography),'P170'(CREATOR)
	# 'P3138', 'P57' (DIRECTOR), 'P162'(producer), 'P6127', 'P8033', 'P646', 'P4947'])
	# From wikidata: Use GZip compression when making API calls by setting Accept-Encoding: gzip to reduce bandwidth usage.

def parse_requery_batch(wikidata_response,db_chunk):
	for qid,description in wikidata_response['entities'].items():
		titles = [
			values['value']
			for lang,values
			in description['labels'].items()
			]
		print(titles)
		if 'P57' in description['claims']:
			directors = [
				dir['mainsnak']['datavalue']['value']['id']
				for dir
				in description['claims']['P57']
				]
			print(directors)
