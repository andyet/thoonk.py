from thoonk.consts import *
from thoonk.exceptions import *
from thoonk.feeds import Queue


class Job(Queue):

    class JobDoesNotExist(Exception):
        pass

    class JobNotPending(Exception):
        pass

    def __init__(self, *args, **kwargs):
        Queue.__init__(self, *args, **kwargs)

    def get_channels(self):
        return (FEEDPUB % self.feed, FEEDRETRACT % self.feed)

    def event_stalled(self, id, value):
        pass

    def _check_running(self, id):
        return self.redis.sismember(FEEDJOBRUNNING % self.feed, id)

    def put(self, item):
        return self.publish(item)

    def get(self, timeout=0):
        self.check_feed()

        id = self.redis.brpop(FEEDIDS % self.feed, timeout)[1]
        if id is None:
            raise self.Empty
        self.redis.hset(FEEDJOBRUNNING % self.feed, id, time.time())
        value = self.redis.hget(FEEDITEMS % self.feed, id)
        self._publish_number()
        return id, value

    def finish(self, id, value=None, result=False):
        self.check_feed()
        #TODO: should pipe
        if self.redis.hdel(FEEDJOBRUNNING % self.feed, id):
            #self._publish_number()
            if result:
                query = self.redis.hget(FEEDITEMS % self.feed, id)
                self.redis.rpush(FEEDJOBFINISHED % (self.feed, id), "%s\x00%s" % (query, value))
            self.redis.hdel(FEEDITEMS % self.feed, id)
        else:
            raise JobDoesNotExist

    def get_result(self, id, timeout=0):
        result = self.redis.blpop(FEEDJOBFINISHED % (self.feed, id), timeout)
        if result is None:
            raise self.empty
        return result[1].split("\x00")

    def check_result(self, id):
        return self.redis.llen(FEEDJOBFINISHED % (self.feed, id))

    def cancel(self, id):
        self.check_feed()
        if self.redis.hdel(FEEDJOBRUNNING % self.feed, id):
            pipe = self.redis.pipeline()
            pipe.lpush(FEEDIDS % self.feed, id)
            pipe.execute()
            self._publish_number()
        else:
            raise self.JobNotPending()

    def stall(self, id):
        self.check_feed()
        self._check_running(id)
        if not self.redis.hdel(FEEDJOBRUNNING % self.feed, id):
            raise self.JobDoesNotExist
        self.redis.lpush(FEEDJOBSTALLED % self.feed, id)

    def retry(self, id):
        if not self.redis.lrem(FEEDJOBSTALL % self.feed, id, num=1):
            raise self.JobDoesNotExist
        self.redis.rpush(FEEDIDS % self.feed, id)
        self._publish_number(self.feed)

