#!/usr/bin/python
# coding: utf-8
# @author: pf Li


import requests
from gevent import Greenlet
from gevent import queue
from gevent import monkey
from gevent import pool
from gevent import threadpool
import gevent
import purl
from utility import *
import sys
import MySQLdb

monkey.patch_all()

"""
	Spider for exercise with Gevent and Requests libs
"""

class MySQLConfig:
	""" 
	this class holds the mysql connection data (host, port,
	databasename, user, password)
	"""
	def __init__(self, host, port, db, user, pw):
		"""
		@param host: name or ip of the MySQL database host
		@param port: port on which the MySQL database is available
		@param db: name of the MySQL database
		@param user: MySQL username
		@param pw: MySQL password 
		"""
		self.host = host
		self.port = port
		self.db = db
		self.user = user
		self.pw = pw

class Fetcher(Greenlet):
	"""Fetcher is used to fetch and download url"""
	def __init__(self, fetcher_url_queue,process_html_queue,start_url,max_depth,url_list):
		Greenlet.__init__(self)
		self.fetcher_url_queue = fetcher_url_queue;
		self.process_html_queue = process_html_queue;
		self.start_url = start_url;
		self.max_depth = max_depth;
		self.url_list  = url_list;

	def _run(self):
		self._fetch()

	def _fetch(self):
		while True:
			try :
				(url,depth) = self.fetcher_url_queue.get(block=False);
				print "Begin to fetch url is %s and depth is %s" % (url,depth)
				if not self._checkDomain(url):
					print "[+] Not the same domain with the start url."
					self.fetcher_url_queue.task_done()
					continue
			except queue.Empty,e:
				print "[+] The queue is empty"
				gevent.sleep(2)
			else :
					# Todo: we can define an function to add request header defined by ourselves
					if depth < self.max_depth:
						print "Here is url: %s" % url
						resp = requests.get(str(url))
						print "Response status code "
						if resp.status_code != 200:
							print "[+] Can not open the url: %s" % (url)
							continue
						else:
							print "[+] The response code is 200."
							self.process_html_queue.put((resp,depth),block=True)
							print "[+] Process the html content for url: %s" % (url)
							continue
					else:
						print "[+] The crawler depth is reached"
						break
					self.fetcher_url_queue.task_done()


	def _checkDomain(self,url):
		if purl.URL(self.start_url).netloc() != purl.URL(url).netloc():
			return False
		else:
			# Todo url not in url_list and the format follows the URL standard
			return True

class Spider:
	def __init__(self,fetcher_url_queue,process_html_queue,url_list,concurrent_num,max_depth,start_url):
		self.fetcher_url_queue = fetcher_url_queue
		self.process_html_queue = process_html_queue
		self.url_list = url_list
		self.concurrent_num = int(concurrent_num)
		self.max_depth = max_depth
		self.start_url = start_url
		self.fetchPool = pool.Pool(self.concurrent_num)
		self.crawPool = pool.Pool(self.concurrent_num)

	def startFetch(self):
		for _ in xrange(self.concurrent_num):
			fetcher = Fetcher(self.fetcher_url_queue,self.process_html_queue,self.start_url,self.max_depth,self.url_list)
			self.fetchPool.start(fetcher)

	def crawl(self):
		while True:
			try:
				(resp,current_depth)= self.process_html_queue.get(block=False)
			except queue.Empty,e:
				print "[+] Can not get the queue from process_html_queue"
				gevent.sleep(2)
			else:
				DP = DynamicProcessor(resp.url)
				url_pool = [link for link in DP.extractURL()]
				url_pool = list(set(url_pool))
				for link in url_pool:
					if link not in url_list:
						url_list.append(link)
						print "[+] The length of url_list is %d" % len(url_list)
						self.fetcher_url_queue.put((link,current_depth+1),block=True)
				self.process_html_queue.task_done()

	def startCrawl(self):
		for _ in xrange(self.concurrent_num):
			self.crawPool.spawn(self.crawl)

	def insertDB(self, url, depth, html):
		con = MySQLdb.connect(host=self.mysql_conf.host,
				      port=self.mysql_conf.port,
				      db=self.mysql_conf.db,
				      user=self.mysql_conf.user,
				      passwd=self.mysql_conf.pw)
		cursor = con.cursor()
		try:
			cursor.execute(""" 
				INSERT INTO sites(start_url, url, depth, html) 
				VALUES(%s, %s, %s, %s)
				""", (MySQLdb.escape_string(self.start_url), 
					  MySQLdb.escape_string(url), 
					  depth, 
					  MySQLdb.escape_string(html))
				)
			con.commit()
		except Exception as e:
			print e            
			con.rollback()
		finally:
			con.close();

	def run(self):
		self.fetcher_url_queue.put((self.start_url,0))
		self.startFetch()
		self.startCrawl()
		self.fetchPool.join()
		self.crawPool.join()

if __name__=="__main__":
	fetch_queue = threadpool.Queue()
	process_queue = threadpool.Queue()
	url_list = []
	start_url = sys.argv[1]
	concurrent_num = sys.argv[2]
	max_depth = sys.argv[3]

	spider = Spider(fetch_queue,process_queue,url_list,concurrent_num,max_depth,start_url)
	spider.run()


