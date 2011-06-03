from thoonk.consts import *
from thoonk.exceptions import *
from thoonk.feeds import *


class List(Feed):

    def append(self, item):
        self.publish(item)

    def prepend(self, item):
        id = self.redis.incr(FEEDIDINCR % self.feed)
        pipe = self.redis.pipeline()
        pipe.lpush(FEEDIDS % self.feed, id)
        pipe.incr(FEEDPUBS % self.feed)
        pipe.hset(FEEDITEMS % self.feed, id, item)
        pipe.publish(FEEDPUB % self.feed, '%s\x00%s' % (id, item))
        pipe.execute()
        return id

    def __insert(self, item, rel_id, method):
        id = self.redis.incr(FEEDIDINCR % self.feed)
        while True:
            self.redis.watch(FEEDITEMS % self.feed)
            if not self.redis.hexists(FEEDITEMS % self.feed, rel_id):
                self.redis.unwatch()
                return # raise exception?

            pipe = self.redis.pipeline()
            pipe.linsert(FEEDIDS % self.feed, method, rel_id, id)
            pipe.hset(FEEDITEMS % self.feed, id, item)
            pipe.publish(FEEDPUB % self.feed, '%s\x00%s' % (id, item))

            try:
                pipe.execute()
                break
            except redis.exceptions.WatchError:
                pass

    def publish(self, item):
        id = self.redis.incr(FEEDIDINCR % self.feed)
        pipe = self.redis.pipeline()
        pipe.rpush(FEEDIDS % self.feed, id)
        pipe.incr(FEEDPUBS % self.feed)
        pipe.hset(FEEDITEMS % self.feed, id, item)
        pipe.publish(FEEDPUB % self.feed, '%s\x00%s' % (id, item))
        pipe.execute()
        return id

    def edit(self, id, item):
        while True:
            self.redis.watch(FEEDITEMS % self.feed)
            if not self.redis.hexists(FEEDITEMS % self.feed, id):
                self.redis.unwatch()
                return # raise exception?

            pipe = self.redis.pipeline()
            pipe.hset(FEEDITEMS % self.feed, id, item)
            pipe.incr(FEEDPUBS % self.feed)
            pipe.publish(FEEDPUB % self.feed, '%s\x00%s' % (id, item))

            try:
                pipe.execute()
                break
            except redis.exceptions.WatchError:
                pass

    def publish_before(self, before_id, item):
        self.__insert(item, before_id, 'BEFORE')

    def publish_after(self, after_id, item):
        self.__insert(item, after_id, 'AFTER')

    def retract(self, id):
        while True:
            self.redis.watch(FEEDITEMS % self.feed)
            if self.redis.hexists(FEEDITEMS % self.feed, id):
                pipe = self.redis.pipeline()
                pipe.lrem(FEEDIDS % self.feed, id, 1)
                pipe.hdel(FEEDITEMS % self.feed, id)
                pipe.publish(FEEDRETRACT % self.feed, id)
                try:
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    pass
            else:
                self.redis.unwatch()
                return

    def get_ids(self):
        return self.redis.lrange(FEEDIDS % self.feed, 0, -1)

    def get_item(self, id):
        return self.redis.hget(FEEDITEMS % self.feed, id)

    def get_items(self):
        return self.redis.hgetall(FEEDITEMS % self.feed)
