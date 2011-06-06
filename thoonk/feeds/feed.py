import json
import threading
import time
import uuid
try:
    import queue
except ImportError:
    import Queue as queue

from thoonk.exceptions import *


class Feed(object):

    def __init__(self, thoonk, feed, config=None):
        self.config_lock = threading.Lock()
        self.config_valid = False
        self.thoonk = thoonk
        self.redis = thoonk.redis
        self.feed = feed
        self._config = None

        self.feed_ids = 'feed.ids:%s' % feed
        self.feed_items = 'feed.items:%s' % feed
        self.feed_publish = 'feed.publish:%s' % feed
        self.feed_published = 'feed.published:%s' % feed
        self.feed_publishes = 'feed.publishes:%s' % feed
        self.feed_retract = 'feed.retract:%s' % feed
        self.feed_config = 'feed.config:%s' % feed
        self.feed_edit = 'feed.edit:%s' % feed
        self.feed_cancelled = 'feed.cancelled:%s' % feed

    def get_channels(self):
        return (self.feed_publish, self.feed_retract)

    def event_publish(self, id, value):
        pass

    def event_retract(self, id):
        pass

    @property
    def config(self):
        with self.config_lock:
            if not self.config_valid:
                conf = self.redis.get(self.feed_config)
                self._config = json.loads(conf)
                self.config_valid = True
            return self._config

    @config.setter
    def config(self, config):
        with self.config_lock:
            self.thoonk.set_feed_config(self.feed, config)
            self.config_valid = False

    @config.deleter
    def config(self):
        with self.config_lock:
            self.config_valid = False

    def delete_feed(self):
        self.thoonk.delete_feed(self.feed)

    def get_schemas(self):
        return set((self.feed_ids, self.feed_items, self.feed_publish,
                    self.feed_published, self.feed_publishes,
                    self.feed_retract, self.feed_config, self.feed_edit,
                    self.feed_cancelled))

    # Thoonk Standard API
    # =================================================================

    def get_ids(self):
        return self.redis.zrange(self.feed_ids, 0, -1)

    def get_item(self, item=None):
        if item is None:
            self.redis.hget(self.feed_items,
                            self.redis.lindex(self.feed_ids, 0))
        else:
            return self.redis.hget(self.feed_items, item)

    def get_all(self):
        return self.redis.hgetall(self.feed_items)

    def publish(self, item, id=None):
        publish_id = id
        if publish_id is None:
            publish_id = uuid.uuid4().hex
        while True:
            self.redis.watch(self.feed_ids)

            max = int(self.config.get('max_length', 0))
            pipe = self.redis.pipeline()
            if max > 0:
                delete_ids = self.redis.zrange(self.feed_ids, 0, -max)
                for id in delete_ids:
                    if id != publish_id:
                        pipe.zrem(self.feed_ids, id)
                        pipe.hdel(self.feed_items, id)
                        pipe.publish(self.feed_retract, id)
            pipe.zadd(self.feed_ids, publish_id, time.time())
            pipe.incr(self.feed_publishes)
            pipe.hset(self.feed_items, publish_id, item)
            try:
                results = pipe.execute()
                break
            except redis.exceptions.WatchError:
                pass

        if results[-3]:
            # If zadd was successful
            self.thoonk._publish(self.feed_publish, (publish_id, item))
        else:
            self.thoonk._publish(self.feed_edit, (publish_id, item))

        return publish_id

    def retract(self, id):
        while True:
            self.redis.watch(self.feed_ids)
            if self.redis.zrank(self.feed_ids, id):
                pipe = self.redis.pipeline()
                pipe.zrem(self.feed_ids, id)
                pipe.hdel(self.feed_items, id)
                pipe.publish(self.feed_retract, id)
                try:
                    pipe.execute()
                    return
                except redis.exceptions.WatchError:
                    pass
            else:
                self.redis.unwatch()
                break
