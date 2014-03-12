#! /usr/bin/env python
import cPickle
import datetime
import os
import requests
import time
import feedparser
from urlparse import urlparse, urljoin
from bs4 import BeautifulSoup as bs
from Queue import Queue
from Categorizer import *


class TaskHandler:
    """
    Abstract class that manages the cache for URLs, and writes result out to file

    cache is a dictionary with key=URL and value=int. 
    If int value is -1, means that a result has been obtained and need not be fetched again
    If int value is 0, means that URL hasn't been fetch

    """
    def __init__(self, url_list, name="", MAX_TRIES=3):
        self.task_type = name
        self.__cache_name = ".%s_cache.pkl" % name
        self.__result_file = "%s_result.csv" % name
        self.cache = self.__init_cache(url_list)
        self.MAX_TRIES = MAX_TRIES # how many times to refetch a URL 
        
    def __enter__(self):
        self.start()
    def __exit__(self):
        self.exit()
    def __init_cache(self, url_list):
        if os.path.exists(self.__cache_name):
            with open(self.__cache_name, 'rb') as frdr:
                return cPickle.load(frdr)
        else:
            return {url:0 for url in url_list}

    def start(self):
        self.fwtr = open(self.__result_file, 'a') 
    def exit(self):
        with open(self.__cache_name, 'wb') as fwtr: 
            cPickle.dump(self.cache, fwtr)
        self.fwtr.flush()
        self.fwtr.close() 
    def get_url_list(self, step_size=5000):
        # returns the next url that doesn't already have a result
        url_list = [url for url, res in self.cache.iteritems() if res >= 0 and res < self.MAX_TRIES]
        for idx in range(step_size, len(url_list), step_size):
            yield url_list[idx - step_size: idx]
    def sync_cache_and_results(self):
        with open(self.__cache_name, 'wb') as fwtr:
            cPickle.dump(self.cache, fwtr)
        self.fwtr.flush()
    def write_result(self,url, result):
        raise NotImplemented 
    def check_res(self, url):
        """
        returns True if result exist, else False
        """
        if self.cache[url] < 0 :
            return True
        else:
            return False 

class YahooTaskHandler(TaskHandler):
    def __init__(self, url_list, name='yahoo'):
        TaskHandler.__init__(self, url_list, name)
    def update_url(self,url,result=None):
        yctCategories = None
        query = result.get("query")
        if query :
            res = query.get("results")
            if res:
                yctCategories = res.get("yctCategories")
        if yctCategories: 
            # parsing this bitch is annoying
            # implement logic to serialize result out to a csv file
            print "    Result received for url", url
            res = {}
            # yctCategory is a list of dict items :\
            yctCategory = yctCategories.get("yctCategory")
            if type(yctCategory) == dict:
                label = yctCategory.get('content','unknown')
                score = yctCategory.get('score','0')
                res[label] = score
            elif type(yctCategory) == list:
                for cat in yctCategory:
                    label = cat.get('content','unknown')
                    score = cat.get('score','0')
                    res[label] = score
            self.write_result(url, res)
            self.cache[url] = -1 # mark this as an entry with a result 
        else :
            self.cache[url] += 1
    def write_result(self, url, result=None):
        # result should be formatted first
        self.fwtr.write('%s\t' % url)
        for label, score in result.iteritems():
            self.fwtr.write('%s:%s,' % (label, score))
        self.fwtr.write('\n')

class RateLimiter:
    def __init__(self, max_query_per_hr=19000):
        self.max_query_per_hr = max_query_per_hr
        self.max_query_per_day = 95000 
        self.current = datetime.datetime.now()
        self.queries_this_hr = 0
        self.queries_this_day = 0 

    def check_limit(self):
        """
        @returns int val:  val represents number of seconds to sleep if limit is exceeded. 0 if limit is not reached 
        if val is -1, then limit for day is reached
        """
        now = datetime.datetime.now()
        elapsed = (now-self.current).total_seconds()
        self.queries_this_hr += 1 
        if elapsed < 3600:
            if self.queries_this_hr < self.max_query_per_hr:
                return 0
            else:
                return 3600 - elapsed
        else:
            self.queries_this_day += self.queries_this_hr
            self.queries_this_hr = 0
            return 0 

def query_yahoo(url_list):
    yapi = YahooCategorizer()
    yahoo = YahooTaskHandler(url_list, 'yahoo')
    rlim = RateLimiter()
    yahoo.start()
    print "preparing to get url_list"
    for ulist in yahoo.get_url_list(step_size=50):
        # get a chunk of url to process
        for count in range(3):
            # fetches each url at most 3 times. 
            for url in ulist:
                print "Attempting to query url:%s" % url
                # perform multipass over url_list to make sure results is added
                lim = rlim.check_limit()
                if lim > 0 :
                    print "hourly limit exceeded, sleeping for %s seconds" % lim 
                    yahoo.sync_cache_and_results()
                    time.sleep(lim)
                elif lim < 0 :
                    print "limit for day reached, exiting..."   
                    yahoo.sync_cache_and_results()
                    yahoo.exit()
                    return
                else :
                    if not yahoo.check_res(url):
                        try:
                            res = yapi.fetch_url(url)
                            yahoo.update_url(url, res) 
                        except ValueError:
                            print "unable to decode JSON ... perhaps limit reached ?"
                            yahoo.sync_cache_and_results()
                            yahoo.exit()
                            return
            yahoo.sync_cache_and_results()
    yahoo.exit()

class AlexaTaskHandler(TaskHandler):
    """
    cache value is a bit different from other task handler
    -1 : there exist at least one category label for a url
    0 : Not queried 
    1 : Error in response
    2 : No category label
    """
    __query = "http://www.alexa.com/siteinfo/"
    def __init__(self, url_list, name='alexa'):
        TaskHandler.__init__(self, url_list, name)
    def __extract_dom_name(self, url):
        o = urlparse(url)
        return o.netloc.strip('wwww.')
    def __parse_res(self, res):
        """
        extracts the relevant category labels given a webpage src
        @param res string : raw content of a webpage
        @return list : a list of labels associated with webpage
        """
        soup = bs(res)
        try:
            cat_table = soup.select('#category_link_table')[0]
        except IndexError:
            print "IndexError while checking if category labels exist"
            return []
        # if category tables exist, then categories will be located within an anchor tag
        return [ tag.get('href') for tag in cat_table.find_all('a')]

    def get_site(self, url):
        dom_name = self.__extract_dom_name(url)
        query_loc = urljoin(self.__query, dom_name)
        r = requests.get(query_loc)
        print "Attempting to query for url:", url
        if r.ok:
            categories = self.__parse_res(r.text)
            if categories and len(categories) > 0:
                print "    Results obtained! %s" % categories 
                self.write_result(url, categories)
                self.cache[url] = -1
            else:
                self.cache[url] =  2
        else:
            self.cache[url] = 1
        
    def write_result(self, url, result=None):
        """
        @param url string:
        @param result list: a list of categories label
        """
        self.fwtr.write('%s\t' % url)
        for category in result:
            self.fwtr.write('%s,' % category)
        self.fwtr.write('\n')

def query_alexa(url_list):
    rlim = RateLimiter()
    alexa = AlexaTaskHandler(url_list)
    alexa.start()
    for ulist in alexa.get_url_list(step_size=100):
        for url in ulist:
            lim = rlim.check_limit()
            if lim > 0:
                alexa.sync_cache_and_results()
                time.sleep(lim)
            elif lim < 0:
                print "limit for day reached ... exiting"
                alexa.sync_cache_and_results()
                alexa.exit()
                return
            else:
                if not alexa.check_res(url):
                    alexa.get_site(url)
        alexa.sync_cache_and_results()
    alexa.exit()



