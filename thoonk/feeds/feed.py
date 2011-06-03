import json
import threading
import time
import uuid
try:
    import queue
except ImportError:
    import Queue as queue

from thoonk.consts import *
from thoonk.exceptions import *


class Feed(object):

    def __init__(self, thoonk, feed, config=None):
        self.config_lock = threading.Lock()
        self.config_valid = False
        self.thoonk = thoonk
        self.redis = thoonk.redis
        self.feeds = thoonk.feeds
        self.feedconfig = thoonk.feedconfig
        self.feed = feed
        self._config = None

    def get_channels(self):
        return (FEEDPUB % self.feed, FEEDRETRACT % self.feed)

    def event_publish(self, id, value):
        pass

    def event_retract(self, id):
        pass

    @property
    def config(self):
        with self.config_lock:
            if not self.config_valid:
                conf = self.redis.get(FEEDCONFIG % self.feed)
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

    # Thoonk Standard API
    # =================================================================

    def get_ids(self):
        return self.redis.zrange(FEEDIDS % self.feed, 0, -1)

    def get_item(self, item=None):
        if item is None:
            self.redis.hget(FEEDITEMS % self.feed,
                            self.redis.lindex(FEEDIDS % self.feed, 0))
        else:
            return self.redis.hget(FEEDITEMS % self.feed, item)

    def get_all(self):
        return self.redis.hgetall(FEEDITEMS % self.feed)

    def publish(self, item, id=None):
        publish_id = id
        if publish_id is None:
            publish_id = uuid.uuid4().hex
        while True:
            self.redis.watch(FEEDIDS % self.feed)

            max = int(self.config.get('max_length', 0))
            pipe = self.redis.pipeline()
            if max > 0:
                delete_ids = self.redis.zrange(FEEDIDS % self.feed, 0, -max)
                for id in delete_ids:
                    if id != publish_id:
                        pipe.zrem(FEEDIDS % self.feed, id)
                        pipe.hdel(FEEDITEMS % self.feed, id)
                        pipe.publish(FEEDRETRACT % self.feed, id)
            pipe.zadd(FEEDIDS % self.feed, publish_id, time.time())
            pipe.incr(FEEDPUBS % self.feed)
            pipe.hset(FEEDITEMS % self.feed, publish_id, item)
            try:
                results = pipe.execute()
                break
            except redis.exceptions.WatchError:
                pass

        if results[-3]:
            # If zadd was successful
            self.thoonk._publish(FEEDPUB % self.feed, (publish_id, item))
        else:
            self.thoonk._publish(FEEDEDIT % self.feed, (publish_id, item))

        return publish_id

    def retract(self, id):
        while True:
            self.redis.watch(FEEDIDS % self.feed)
            if self.redis.zrank(FEEDIDS % self.feed, id):
                pipe = self.redis.pipeline()
                pipe.zrem(FEEDIDS % self.feed, id)
                pipe.hdel(FEEDITEMS % self.feed, id)
                pipe.publish(FEEDRETRACT % self.feed, id)
                try:
                    pipe.execute()
                    return
                except redis.exceptions.WatchError:
                    pass
            else:
                self.redis.unwatch()
                break
