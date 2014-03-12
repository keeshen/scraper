#! /usr/bin/env python
import csv
from rauth import OAuth1Service
from TaskHandler import TaskHandler 
from RateLimiter import *

class YahooTaskHandler(TaskHandler):
    """ 
    A manager for scraping data off of yahoo's developer network

    Simplifies the OAuth authentication process (OAuth v1)
    Makes queries to the YQL interface of Yahoo's developer network

    Reads client key and client secret needed for OAuth from a file. Defaults to 'auth.csv'

    """

    __url = "http://query.yahooapis.com/v1/yql"
    def __init__(self, url_list, auth_file='auth.csv',\
            query_str=''):
        TaskHandler.__init__(self, url_list, name='yahoo')
        self.__read_authfile(auth_file)
        self.__query_str = query_str 
        self.__init__oauth()

    def __read__authfile(self, auth_file):
        vals = {}
        with open(auth_file,'rb') as frdr:
            f_iter = csv.DictReader(frdr)
            vals = f_iter.next()
        self.consumer_key = vals["consumer_key"]
        self.consumer_secret = vals["consumer_secret"]

    def __init_oauth(self):
        print "Initializing OAuth with yahoo API"
        yahoo = OAuth1Service(consumer_key=self.consumer_key,
            consumer_secret=self.consumer_secret,
            name='yahoo',
            access_token_url='https://api.login.yahoo.com/oauth/v2/get_token',
            authorize_url='https://api.login.yahoo.com/oauth/v2/request_auth',
            request_token_url='https://api.login.yahoo.com/oauth/v2/get_request_token',
            base_url='https://api.login.yahoo.com/oauth/v2/'
        )
        self.request_token, self.request_token_secret = \
            yahoo.get_request_token(data={'oauth_callback': 'http://example.com/callback'})
        self.auth_url = yahoo.get_authorize_url(self.request_token)
        print "Access this URL in a browser ..."
        print self.auth_url
        self.pin = raw_input("\nEnter oauth_verifier pin found in address bar: ")
        self.session = yahoo.get_auth_session(self.request_token, self.request_token_secret,
            data={'oauth_verifier': self.pin})

    def fetch_url(self, url):
        # check if session is still active, if not re-negotiate another session
        query_str = "%s='%s'" % (self.__query_str, url)
        r = self.session.get(self.__url, params={'q':query_str, 'format':'json'})
        return r.json()

    def update_url(self,url,result=None):
        """
        Needs to check if response contains a result, which is set to JSON format. 

        yctCategory can be a list of dicts or a single dict (if there is only 1 category)
        yctCategory dict format
        { 'content' : <str>, 'score' : <str> }
        """
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


def query_yahoo(url_list):
    """ 
    Simple function that gathers the category labels of a URL, from yahoo's content-analysis dataset

    Multiple queries should be made since the target system is REST-based and results may not be cached
    when query is first made.

    """
    query_str="select * from contentanalysis.analyze where url"
    with YahooTaskHandler(url_list, query_str=query_str) as yahoo:
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

if __name__ == "__main__":
    # unit test    
    yahoo = YahooCategorizer()
    yahoo.fetch_url('http://www.espn.com')
