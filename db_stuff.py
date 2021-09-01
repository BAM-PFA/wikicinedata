import concurrent.futures
import sqlite3
import threading

import cspace_utils

class DBChunk(threading.Thread):
	"""
	This represents a chunk of the sqlite3 db from start_id to end_id
	"""

	def __init__(self,
		target,
		secrets,
		config,
		filepath,
		chunk_start,
		chunk_end
		):
		threading.Thread.__init__(self)
		self.target = target
		self.secrets = secrets
		self.config = config
		self.filepath = filepath
		self.connection = None
		self.cursor = None
		# starting row id
		self.chunk_start = chunk_start
		# ending row id
		self.chunk_end = chunk_end
		self.rows_in_db = None
		# self.shutdown_flag = threading.Event()

	def connect(self):
		self.connection = sqlite3.connect(self.filepath)
		self.cursor = self.connection.cursor()

	def write_to_db(self,sql,values):
		self.cursor.execute(sql,values)
		self.connection.commit()

	def query_db(self,sql):
		result = self.cursor.execute(sql)
		return result.fetchall()

	def kill(self):
		self.conn.close()

	def run(self):
		self.connect()
		if self.target == 'cspace':
			self.fetch_cspace_data()

	def fetch_cspace_data(self):
		cspace_utils.get_chunked_cspace_items(self)

class Database:
	"""represents the database during creation"""

	def __init__(self, filepath):
		self.filepath = filepath
		self.connection = None
		self.cursor = None

	def create_cspace_table(self,authority):
		if authority == 'workauthorities':
			sql = "CREATE TABLE IF NOT EXISTS items (id integer PRIMARY KEY, csid, uri, title, creator, year, alt_titles, top_match_score, top_match_label, top_match_Qid)"
			self.cursor.execute(sql)
		elif authority == "personauthorities":
			sql = "CREATE TABLE IF NOT EXISTS items (id integer PRIMARY KEY, csid, uri, name, dates, top_match_score, top_match_label, top_match_Qid)"
			pass
		else:
			pass

	def count_me(self):
		rows_in_db_sql = "SELECT COUNT(id) FROM items;"
		self.rows_in_db = self.cursor.execute(rows_in_db_sql).fetchall()[0][0]

	def insert_cspace_item(self, authority,item):
		if authority == "workauthorities":
			data = cspace_utils.get_work_data(item)
			data.insert(0,None) # leave space for primary key
			data.extend([None,None,None,None,None]) # account for the empty wikidata columns
			insertsql = "INSERT INTO items VALUES (?,?,?,?,?,?,?,?,?,?)"

		elif authority == "personauthorities":
			data = cspace_utils.get_person_data(item)
			data.insert(0,None) # leave space for primary key
			insertsql = "INSERT INTO items VALUES (?,?,?,?,?)"

		self.cursor.execute(insertsql,data)

	def chunk_me(self,target,start_id,chunk_size,secrets,config):
		with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
			'''
			I found I had to limit the number of threads operating at once to
			avoid segmentation faults, i/o errors, max http request errors, etc.
			max_workers=10 sets the limit to 10 threads, maybe it could be more?
			'''
			for chunk_start in range(start_id, self.rows_in_db, chunk_size):
				chunk_end = chunk_start + chunk_size - 1
				print(chunk_start,chunk_end)
				chunk = DBChunk(
					target,
					secrets,
					config,
					self.filepath,
					chunk_start,
					chunk_end
					)
				executor.submit(chunk.start())
