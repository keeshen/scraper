import cPickle
import os

class TaskHandler:
    """
    Abstract class that provides basic caching mechanism for URLs scrapers 

    Initialize it with a list of URLs to scrape later on

    self.cache is a dictionary with key=URL and value=int. 
    If int value is -1, means that a result has been obtained and need not be fetched again
    If int value is 0, means that URL hasn't been fetch

    """
    def __init__(self, url_list, name="", MAX_TRIES=3):
        """
        @param url_list list : A list of URL 
        @param name string :
        """
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

