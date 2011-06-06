import uuid

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
        if priority is None:
            priority = self.NORMAL

        id = uuid.uuid4().hex
        pipe = self.redis.pipeline()

        if priority == self.HIGH:
            pipe.rpush(self.feed_ids, id)
            pipe.hset(self.feed_items, id, item)
            pipe.incr(self.feed_publishes % self.feed)
        else:
            pipe.lpush(self.feed_ids, id)
            pipe.hset(self.feed_items, id, item)
            pipe.incr(self.feed_publishes)

        pipe.execute()

    def get(self, timeout=0):
        result = self.redis.brpop(self.feed_ids, timeout)
        if result is None:
            raise self.Empty
        id = result[1]
        pipe = self.redis.pipeline()
        pipe.hget(self.feed_items, id)
        pipe.hdel(self.feed_items, id)
        results = pipe.execute()
        return results[0]
