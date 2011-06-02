import uuid

from thoonk.consts import *
from thoonk.exceptions import *
from thoonk.feeds import Feed


class Queue(Feed):

    class Empty(Exception):
        pass

    def __init__(self, thoonk, feed, config=None):
        Feed.__init__(self, thoonk, feed, config)
        self.NORMAL = 0
        self.HIGH = 1

    def publish(self, item, priority=None):
        self.put(item, priority)

    def put(self, item, priority=None):
        """alias to publish"""
        self.check_feed()

        if priority is None:
            priority = self.NORMAL

        id = uuid.uuid4().hex
        pipe = self.redis.pipeline()

        if priority == self.HIGH:
            pipe.rpush(FEEDIDS % self.feed, id)
            pipe.hset(FEEDITEMS % self.feed, id, item)
            pipe.incr(FEEDPUBS % self.feed)
        else:
            pipe.lpush(FEEDIDS % self.feed, id)
            pipe.hset(FEEDITEMS % self.feed, id, item)
            pipe.incr(FEEDPUBS % self.feed)

        pipe.execute()

    def get(self, timeout=0):
        self.check_feed()
        result = self.redis.brpop(FEEDIDS % self.feed, timeout)
        if result is None:
            raise self.Empty
        id = result[1]
        pipe = self.redis.pipeline()
        pipe.hget(FEEDITEMS % self.feed, id)
        pipe.hdel(FEEDITEMS % self.feed, id)
        results = pipe.execute()
        return results[0]
