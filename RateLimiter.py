import time
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


