#! /usr/bin/env python

# Takes a url, queries a webpage categorizer (Alexa, Yahoo Content
# Analysis, Google APM) and then returns the set of categories associated
# with it

from abc import ABCMeta
from rauth import OAuth1Service # get from rauth.readthedocs.org/en/latest or pip install rauth 
import csv

class Website:
    def __init__(self, url):
        self.url = url # URL should be sanitized. 
        self.data = {} # stores the scraped data as key-value pair
    def __str__(self):
        return "Website " + self.url

class Categorizer:
    def __init__(self):
        self.items = {}
        pass
    def __getitem__(self, key):
        return self.items[key]
    def __setitem__(self, key, value):
        self.items[key] = value
    def __delitem__(self, key):
        del self.items[key]
    def __contains__(self, item):
        if item in self.items: return True
        else: return False
    def __iter__(self):
        return iter(self.items)
    def fetch_url(self, url):
        pass


class YahooCategorizer(Categorizer):
    """
    Yahoo YQL uses OAuth1.0 
    """
    __url = "http://query.yahooapis.com/v1/yql"
    __query_str = "select * from contentanalysis.analyze where url"
    def __init__(self, auth_file='auth.csv'):
        Categorizer.__init__(self)
        self.__read_authfile(auth_file)
        self.__init_oauth()
    def __read_authfile(self, auth_file):
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

        
        
if __name__ == "__main__":
    # unit test    
    yahoo = YahooCategorizer()
    yahoo.fetch_url('http://www.espn.com')
