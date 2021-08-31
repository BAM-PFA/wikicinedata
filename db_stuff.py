import sqlite3

import cspace_utils

class DBChunk(object):
	"""
	This represents a chunk of the sqlite3 db from start_id to end_id
	"""

	def __init__(self, filepath, start_id, chunk_size):
		self.filepath = filepath
		self.connection = None
		self.cursor = None
		self.start_id = start_id
		self.chunk_size = chunk_size
		self.end_id = None

	def get_end_id(self):
		'''
		set the chunk to 500 rows
		'''
		self.end_id = self.start_id + self.chunk_size

	def create(self):
		self.connection = sqlite3.connect(self.filepath)
		self.cursor = self.connection.cursor()

	def kill(self):
		self.conn.close()

class Database:
	"""represents the database during creation"""

	def __init__(self, filepath):
		self.filepath = filepath
		self.connection = None
		self.cursor = None

	def create_cspace_table(self,authority):
		if authority == 'workauthorities':
			sql = "CREATE TABLE IF NOT EXISTS items (id integer PRIMARY KEY, csid, uri, title, creator, alt_titles, top_match_score, top_match_label, top_match_Qid)"
			self.cursor.execute(sql)
		elif authority == "personauthorities":
			sql = "CREATE TABLE IF NOT EXISTS items (id integer PRIMARY KEY, csid, uri, name, dates, top_match_score, top_match_label, top_match_Qid)"
			pass
		else:
			pass

	def insert_cspace_item(self, authority,item):
		if authority == "workauthorities":
			data = cspace_utils.get_work_data(item)
			data.insert(0,None) # leave space for primary key
			data.extend([None,None,None,None]) # account for the empty wikidata columns
			insertsql = "INSERT INTO items VALUES (?,?,?,?,?,?,?,?,?)"

		elif authority == "personauthorities":
			data = cspace_utils.get_person_data(item)
			data.insert(0,None) # leave space for primary key
			insertsql = "INSERT INTO items VALUES (?,?,?,?,?)"

		self.cursor.execute(insertsql,data)
