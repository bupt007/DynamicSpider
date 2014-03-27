#!/usr/bin/python
# coding: utf-8
# @author: pengfei Li


import urlparse
import splinter
from bs4 import BeautifulSoup
import lxml


class DynamicProcessor(object):
	def __init__(self,url):
		self.url = url
		self.browser = splinter.Browser('phantomjs')

	def extractURL(self):
		'''
			extract url from page
			para1 url: from this page to extract urls
		'''
		try:
			self.browser.visit(self.url)
		except Exception,e:
			print "[+] Fail to visit the website"
			return
		else:
			print "[+] Begin to extract urls"
			html = self.browser.html
			bs = BeautifulSoup(html,'lxml')
			linktags = ['a','link','img','frame','iframe'] # form to be processed later
			for tag in linktags:
				for item in bs.find_all(tag):   
					if item.get('href'):
						result = self.checkURL(item.get('href'))
						if result:
							yield result
					elif item.get('src'):
						result = self.checkURL(item.get('src'))
						if result:
							yield result
					else:
						continue

	def checkURL(self,url):
		if not url.startswith('javascript'):
			if url.startswith('http:') or url.startswith("https:"):
				return url
			elif url:
				return urlparse.urljoin(self.url,url)

	def __del__(self):
		self.browser.quit()






