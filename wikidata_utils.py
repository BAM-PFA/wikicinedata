import json
import requests

def reconcile_items(config,database):
	'''
	Do the stuff
	'''
	item_type_id = config["wikidata details"]["item type to reconcile"]
	start_id = 1
	# rows_in_db = database.rows_in_db
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

def reconcile_chunked_items(db_chunk):
	status = None
	item_type_id = db_chunk.config["wikidata details"]["item type to reconcile"]
	item_type_label = db_chunk.config["wikidata details"]["item type label"]
	batch_query_dict = {}
	if item_type_label == 'film':
		data_points_sql = "SELECT title, creator, year, id FROM items WHERE id BETWEEN {} AND {}".format(db_chunk.chunk_start,db_chunk.chunk_end)
		data_points = db_chunk.query_db(data_points_sql)
		# print(data_points)
		for item in data_points:
			# get the relevant data points
			title = item[0]
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
		print(batch_query_dict)
	elif item_type_label == "human":
		pass

	query_payload = json.dumps({"queries":json.dumps(batch_query_dict)})

	try:
		print("Getting Wikidata data for "+title)
		r = requests.post(
			db_chunk.config['wikidata details']['wikidata reconciliation endpoint'],
			data=json.loads(query_payload)
			)
		r.raise_for_status()
		status = True
		wikidata_response = json.loads(r.content)

		parse_reconciled_batch(wikidata_response,db_chunk)
	except Exception as e:#requests.exceptions.RequestException as e:
		print(e)
		status = False

	return status

def parse_reconciled_batch(wikidata_response,db_chunk):
	for query,result in wikidata_response.items():
		# print(query)
		print(result)
		if not result["result"] == []:
			# i.e. if there was no match at all in wikidata
			item_id = int(query.replace("q",""))
			top_match = result["result"][0]
			top_match_Qid = top_match["id"]
			top_match_label = top_match["name"]
			print(top_match_label)
			top_match_is_match = top_match["match"]
			top_match_score = top_match["score"]

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
			db_chunk.write_to_db(update_sql,values)
