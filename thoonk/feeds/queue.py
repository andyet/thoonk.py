import uuid

from thoonk.consts import *
from thoonk.exceptions import *
from thoonk.feeds import Feed


class Queue(Feed):

    class Empty(Exception):
        pass

    def publish(self, item):
        self.check_feed()
        pipe = self.redis.pipeline()
        id = uuid.uuid4().hex
        pipe.lpush(FEEDIDS % self.feed, id)
        pipe.hset(FEEDITEMS % self.feed, id, item)
        pipe.execute()
        self._publish_number()
        return id

    def put(self, item):
        """alias to publish"""
        return self.publish(item)

    def get(self, timeout=0):
        self.check_feed()
        result = self.redis.brpop(FEEDIDS % self.feed, timeout)
        if result is None:
            raise self.Empty
        id = result[1]
        value = self.redis.hget(FEEDITEMS % self.feed, id)
        self.redis.hdel(FEEDITEMS % self.feed, id)
        self._publish_number()
        return value

    def _publish_number(self):
        #indicates that the length of FEEDIDS has changed
        #self.redis.publish(FEEDPUB % self.feed, "__size__\x00%d" % self.redis.llen(FEEDIDS % self.feed))
        pass
