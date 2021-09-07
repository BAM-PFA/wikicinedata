import asyncio
import concurrent.futures
import queue
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import sqlite3
import sys
import threading
import time

from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore

import cspace_utils
import wikidata_utils

class APIHandler:
	"""
	handler for a single API endpoint
	"""
	def __init__(self,endpoint):
		self.endpoint = endpoint
		self.url_queue = queue.Queue()
		self.session = requests.Session()
		self.retry = Retry(other=0,backoff_factor=1)
		self.adapter = HTTPAdapter(max_retries=self.retry)
		self.session.mount('http://', self.adapter)
		self.session.mount('https://', self.adapter)
		self.futures = {}

	def feed_me(self,chunk,url,auth):
		self.url_queue.put({chunk.uuid:[url,auth]})

	def dummy_future(self):
		# this is a placeholder function to generate an initial future object below
		# print()
		return None

	def run_me(self):
		with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
			# process_futures = {0:executor.submit(self.dummy_future())}
			process_futures = {}
			while process_futures:
				done,not_done = concurrent.futures.wait(process_futures.values())
				process_futures.pop(0)
				print(process_futures)
				while not self.url_queue.empty():
					# print(self.url_queue)

					# pull a chunk:url pair from the queue
					(chunk_id,[url,auth]) = list(self.url_queue.get().items())[0]
					print(chunk_id,[url,auth])
					process_futures[chunk_id] = executor.submit(self.worker, url, auth)
				for future in done:
					print([x for x in done])
					if not chunk_id == 0:
						self.futures[chunk_id] = future
					process_futures.pop(chunk_id)
					# done.remove(future)


		return self.futures

	def worker(self,url,auth=None,data=None):
		r = self.session.get(url,auth=auth,data=data)
		print(r.content)

		return(r)
