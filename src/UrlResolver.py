#!/usr/bin/env python

"""
UrlResolver.py: A small threaded url resolution utility, originally written for the 
University of Illinois' Graduate School of Library and Information Science's TREC 2011 
Microblog team.


Usage:
from ResolveUrls import UrlResolver

# Optional params and defaults: num_threads = 400, mysql_host = 'localhost', http_redirects = 10
resolver = UrlResolver(url_file = '~/urls.txt', mysql_user = 'root', 
                       mysql_password = 'password', mysql_db = 'my_db')

resolver.run()


Alternate usage:
from ResolveUrls import ResolverThread

myQueue = Queue.Queue(maxsize=100)
for i in range(100):
    # Optional params and defaults: mysql_host = 'localhost', http_redirects = 10
    t = ResolverThread(myQueue, mysql_user = 'root', mysql_password = 'password', 
                  mysql_db = 'my_db')
    t.setDaemon(True)
    t.start()

# Add your own logic here to add urls to the queue
...
    urlQueue.put((url), block = True, timeout = None)
...

myQueue.join()


MySQL table structure:
resolved_urls
+---------------+---------------------+------+-----+---------+----------------+
| Field         | Type                | Null | Key | Default | Extra          |
+---------------+---------------------+------+-----+---------+----------------+
| id            | int(10) unsigned    | NO   | PRI | NULL    | auto_increment |
| short_url     | text                | NO   |     | NULL    |                |
| response_code | int(11)             | NO   |     | NULL    |                |
| long_url      | text                | YES  |     | NULL    |                |
| num_redirects | int(10) unsigned    | YES  |     | NULL    |                |
+---------------+---------------------+------+-----+---------+----------------+

url_headers
+---------+---------+------+-----+---------+-------+
| Field   | Type    | Null | Key | Default | Extra |
+---------+---------+------+-----+---------+-------+
| id      | int(11) | NO   | MUL | NULL    |       |
| headers | text    | NO   |     | NULL    |       |
+---------+---------+------+-----+---------+-------+
"""

__author__      = "Sunah Suh"
__copyright__   = "Copyright 2011, Sunah Suh"
__license__     = "MIT"

import httplib2 as http
import MySQLdb as mysql
import threading
import Queue


'''
UrlResolver: main class
'''
class UrlResolver():
    
    def __init__(self, url_file, mysql_user, mysql_password, mysql_db, 
             mysql_host = 'localhost', num_threads = 400, http_redirects = 10,
             ua = None):
        self._url_file = url_file
        self._urlQueue = Queue.Queue(maxsize=num_threads)
        
        # Start threads
        for i in range(num_threads):
            t = ResolverThread(self._urlQueue, mysql_user, mysql_password, mysql_db, 
                           mysql_host, http_redirects, ua)
            t.setDaemon(True)
            t.start()
 
 
    def run(self):          
        # Read through the file, find links and add them to the queue 
        f = open(self._url_file, 'r')
        for line in f:
            line = line.strip()
            self._urlQueue.put((line), block = True, timeout = None)
        
        self._urlQueue.join()
        


class ResolverThread(threading.Thread):

    def __init__(self, urlQueue, mysql_user, mysql_password, mysql_db, 
                 mysql_host = 'localhost', http_redirects = 10, ua = None):
        threading.Thread.__init__(self)
        self._urlQueue = urlQueue
        self._mysql_user = mysql_user
        self._mysql_password = mysql_password
        self._mysql_db = mysql_db
        self._mysql_host = mysql_host
        self._http_redirects = http_redirects
        self._ua = ua
    
    
    def run(self):
        while True:
            h = http.Http(timeout=30)
            h.force_exception_to_status_code = True
            url = self._urlQueue.get()
            headers = {}
            if (self._ua):
                headers = {'User-agent': self._ua}
            response, content = h.request(url, method="HEAD", 
                                          redirections = self._http_redirects,
                                          headers = headers)
            longUrl = ''
            if 'content-location' in response:
                longUrl = response['content-location']
            respChain = str(response)
            prevResp = response.previous
            redirects = 0
            while (prevResp):
                redirects = redirects + 1
                respChain = respChain + str(prevResp)
                prevResp = prevResp.previous
            
            self._mysql_save(url, response, longUrl, redirects, respChain)
        
            #signals to queue job is done
            self._urlQueue.task_done()


    def _mysql_save(self, url, response, longUrl, redirects, respChain):
        # Save results into the database
        db = mysql.connect(user=self._mysql_user, passwd=self._mysql_password, 
                           db=self._mysql_db, host=self._mysql_host)
        cursor = db.cursor()
        cursor.execute("""INSERT INTO resolved_urls (short_url, response_code,
                       long_url, num_redirects) VALUES (%s, %s, %s, %s)""", 
                       (url, response.status, longUrl, redirects,))
        
        # Save the headers
        urlID = cursor.lastrowid
        cursor.execute("""INSERT INTO url_headers VALUES (%s, %s)""", (urlID, respChain,))
        db.close()
    
