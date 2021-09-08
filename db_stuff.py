import asyncio
import concurrent.futures
import queue
import sqlite3
import sys
import threading
import time
import uuid

import cspace_utils
import wikidata_utils

# class APIhandler(threading.Thread):

class DBChunk(threading.Thread):
	"""
	This represents a chunk of the sqlite3 db from start_id to end_id
	"""

	def __init__(self,
		target,
		secrets,
		config,
		api_handler,
		filepath,
		write_queue,
		chunk_start,
		chunk_end,
		cspace_page_number=None
		):
		threading.Thread.__init__(self)
		self.database = None
		self.uuid = str(uuid.uuid4())
		self.target = target
		self.secrets = secrets
		self.config = config
		self.api_handler = api_handler
		self.filepath = filepath
		self.write_queue = write_queue
		self.connection = None
		self.cursor = None
		# starting row id
		self.chunk_start = chunk_start
		# ending row id
		self.chunk_end = chunk_end
		self.rows_in_db = None
		self.cspace_page_number = cspace_page_number
		# self.shutdown_flag = threading.Event()

	def connect(self):
		self.connection = sqlite3.connect(self.filepath)
		self.cursor = self.connection.cursor()

	def write_to_db(self,sql,values):
		# self.cursor.execute(sql,values)
		# self.connection.commit()
		print("FEEDING "+str(values))
		self.write_queue.feed_queue(sql,values)

	def query_db(self,sql,values):
		result = self.cursor.execute(sql,values)
		return result.fetchall()

	def kill(self):
		self.connection.close()

	def run(self):
		self.connect()
		# while True:
		if self.target == 'cspace':
			# status = self.fetch_chunked_cspace_page()
			cspace_utils.fetch_chunked_cspace_page(self)
		elif self.target == 'cspace items':
			# status = self.fetch_cspace_item_data()
			# self.fetch_cspace_item_data()
			cspace_utils.get_chunked_cspace_items(self)
		elif self.target == 'wikidata':
			# status =
			# self.reconcile_items()
			wikidata_utils.reconcile_chunked_items(self)

class DBWriter(threading.Thread):
	"""class to queue database write processes"""
	def __init__(self,filepath):
		# self.arg = sql_to_run
		self.write_queue = queue.Queue()
		self.filepath = filepath
		self.connection = None
		self.cursor = None

	def connect(self):
		self.connection = sqlite3.connect(self.filepath)
		self.cursor = self.connection.cursor()

	def feed_queue(self,sql_to_run,values):
		self.write_queue.put((sql_to_run,values))

	def run_me(self):
		self.connect()
		print("ACTUALLY WRITING TO DB NOW")
		while not self.write_queue.empty():
			(sql_to_run,values) = self.write_queue.get()
			print(sql_to_run,values)
			# while True:
				# try:
			self.cursor.execute(sql_to_run,values)
			self.write_queue.task_done()
					# break
				# except:
					# time.sleep(.01)
		self.connection.commit()

class Database:
	"""represents the database during creation"""

	def __init__(self, filepath):
		self.filepath = filepath
		self.connection = None
		self.cursor = None
		self.secrets = None
		self.config = None
		self.db_writer = DBWriter(self.filepath)
		self.api_handlers = []
		self.chunks = []

	def feed_queue(self,sql_to_run):
		self.db_writer.feed_queue(sql_to_run)

	def create_cspace_table(self,authority):
		if authority == 'workauthorities':
			sql = "CREATE TABLE IF NOT EXISTS items (id integer PRIMARY KEY, csid, uri, title, creator, enriched, year, alt_titles, top_match_is_match, top_match_score, top_match_label, top_match_Qid)"
			self.cursor.execute(sql)
		elif authority == "personauthorities":
			sql = "CREATE TABLE IF NOT EXISTS items (id integer PRIMARY KEY, csid, uri, name, dates, enriched, top_match_is_match, top_match_score, top_match_label, top_match_Qid)"
			pass
		else:
			pass

	def count_me(self):
		rows_in_db_sql = "SELECT COUNT(id) FROM items;"
		self.rows_in_db = self.cursor.execute(rows_in_db_sql).fetchall()[0][0]
		# print(self.rows_in_db)

		return self.rows_in_db

	def chunk_me(self,target,start_number,end_number,chunk_size,api_handler=None):#,cspace_page_number=None):
		with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
			'''
			I found I had to limit the number of threads operating at once to
			avoid segmentation faults, i/o errors, max http request errors, etc.
			max_workers=10 sets the limit to 10 threads, maybe it could be more?
			'''
			futures = []
			iteration=0
			print("Target = "+target)
			print(start_number,end_number,chunk_size)
			for chunk_start in range(start_number, end_number, chunk_size):
				chunk_end = chunk_start + chunk_size - 1
				# print(chunk_start,chunk_end)
				chunk = DBChunk(
					target,
					self.secrets,
					self.config,
					api_handler,
					self.filepath,
					self.db_writer,
					chunk_start,
					chunk_end,
					cspace_page_number=iteration
					)
				chunk.database = self
				self.chunks.append(chunk)
				executor.submit(chunk.start())
				# futures.append(executor.submit(chunk.run()))
				# for future in concurrent.futures.as_completed(futures):
				# 	print(future._result)
				iteration+=1
