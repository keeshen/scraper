import requests
import feedparser

from urlparse import urlparse, urljoin
from bs4 import BeautifulSoup as bs

from TaskHandler import TaskHandler
from RateLimiter import RateLimiter

class AlexaTaskHandler(TaskHandler):
    """
    cache value is a bit different from other task handler
    -1 : there exist at least one category label for a url
    0 : Not queried 
    1 : Error in response
    2 : No category label
    """
    __query = "http://www.alexa.com/siteinfo/"
    def __init__(self, url_list):
        TaskHandler.__init__(self, url_list, name='alexa')
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
    with AlexaTaskHandler(url_list) as alexa:
        for ulist in alexa.get_url_list(step_size=100):
            for url in ulist:
                lim = rlim.check_limit()
                if lim > 0:
                    alexa.sync_cache_and_results()
                    time.sleep(lim)
                elif lim < 0:
                    print "limit for day reached ... exiting"
                    alexa.sync_cache_and_results()
                    return
                else:
                    if not alexa.check_res(url):
                        alexa.get_site(url)
            alexa.sync_cache_and_results()


