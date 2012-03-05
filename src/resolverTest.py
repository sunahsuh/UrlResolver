#!/usr/bin/env python
"""
resolverTest.py: an example script for ResolveUrls which uses the twitter API 
to find tweets with links shortened with 't.co', writes the URLs to a file
and resolves them using ResolveUrls

NOTE: Most of the links resolved will be spam. Do not visit these urls with a
browser
"""

__author__      = "Sunah Suh"
__copyright__   = "Copyright 2012, Sunah Suh"
__license__     = "MIT"

import twitter
import re
from ResolveUrls import UrlResolver
from time import sleep

MYSQL_USER = ''
MYSQL_PASSWORD = ''
MYSQL_DB = ''
URL_FILE = '/tmp/urls.txt'
UA_STRING = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
NUM_THREADS = 50
URL_LIMIT = 100

# Twitter Api Instance
tw_api = twitter.Api()

tco_regex = re.compile(r'http://t\.co/\w+', re.IGNORECASE)

urls = []

# collect urls from twitter until URL_LIMIT is reached 
while (len(urls) < URL_LIMIT):
    results = tw_api.GetSearch(term="http://t.co")[0:15]
    last_tweet_id = results[0].id
    for r in results:
        matches = tco_regex.findall(r.text)
        urls.extend(matches)
    # Trying to avoid rate limits
    sleep(1)

# urls might be bigger than URL_LIMIT, so concatenate it
urls = urls[0:100]

# open a temp url file
f = open(URL_FILE, 'w')

for url in urls:
    f.write(url + '\n')

f.close()


resolver = UrlResolver(url_file = URL_FILE, mysql_user = MYSQL_USER, 
                       mysql_password = MYSQL_PASSWORD, mysql_db = MYSQL_DB,
                       ua = UA_STRING, num_threads = NUM_THREADS)

resolver.run()
