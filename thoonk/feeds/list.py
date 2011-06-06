from thoonk.exceptions import *
from thoonk.feeds import *


class List(Feed):

    def __init__(self, thoonk, feed, config=None):
        Feed.__init__(self, thoonk, feed, config)

        self.feed_id_incr = 'feed.idincr:%s' % feed

    def get_schemas(self):
        schema = set((self.feed_id_incr,))
        return schema.union(Feed.get_schemas(self))

    def append(self, item):
        self.publish(item)

    def prepend(self, item):
        id = self.redis.incr(self.feed_id_incr)
        pipe = self.redis.pipeline()
        pipe.lpush(self.feed_ids, id)
        pipe.incr(self.feed_publishes)
        pipe.hset(self.feed_items, id, item)
        pipe.publish(self.feed_publish, '%s\x00%s' % (id, item))
        pipe.execute()
        return id

    def __insert(self, item, rel_id, method):
        id = self.redis.incr(self.feed_id_incr)
        while True:
            self.redis.watch(self.feed_items)
            if not self.redis.hexists(self.feed_items, rel_id):
                self.redis.unwatch()
                return # raise exception?

            pipe = self.redis.pipeline()
            pipe.linsert(self.feed_ids, method, rel_id, id)
            pipe.hset(self.feed_items, id, item)
            pipe.publish(self.feed_publish, '%s\x00%s' % (id, item))

            try:
                pipe.execute()
                break
            except redis.exceptions.WatchError:
                pass

    def publish(self, item):
        id = self.redis.incr(self.feed_id_incr)
        pipe = self.redis.pipeline()
        pipe.rpush(self.feed_ids, id)
        pipe.incr(self.feed_publishes)
        pipe.hset(self.feed_items, id, item)
        pipe.publish(self.feed_publish, '%s\x00%s' % (id, item))
        pipe.execute()
        return id

    def edit(self, id, item):
        while True:
            self.redis.watch(self.feed_items)
            if not self.redis.hexists(self.feed_items, id):
                self.redis.unwatch()
                return # raise exception?

            pipe = self.redis.pipeline()
            pipe.hset(self.feed_items, id, item)
            pipe.incr(self.feed_publishes)
            pipe.publish(self.feed_publish, '%s\x00%s' % (id, item))

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
            self.redis.watch(self.feed_items)
            if self.redis.hexists(self.feed_items, id):
                pipe = self.redis.pipeline()
                pipe.lrem(self.feed_ids, id, 1)
                pipe.hdel(self.feed_items, id)
                pipe.publish(self.feed_retract, id)
                try:
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    pass
            else:
                self.redis.unwatch()
                return

    def get_ids(self):
        return self.redis.lrange(self.feed_ids, 0, -1)

    def get_item(self, id):
        return self.redis.hget(self.feed_items, id)

    def get_items(self):
        return self.redis.hgetall(self.feed_items)
