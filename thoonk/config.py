import json
import threading
import uuid

from thoonk.consts import *


class ConfigCache(object):
    def __init__(self, pubsub):
        self._feeds = {}
        self.pubsub = pubsub
        self.lock = threading.Lock()
        self.instance = uuid.uuid4().hex

    def __getitem__(self, feed):
        with self.lock:
            if feed in self._feeds:
                return self._feeds[feed]
            else:
                if not self.pubsub.feed_exists(feed):
                    raise FeedDoesNotExist
                config = json.loads(self.pubsub.redis.get(FEEDCONFIG % feed))
                self._feeds[feed] = self.pubsub.feedtypes[config.get(u'type', u'feed')](self.pubsub, feed, config)
                return self._feeds[feed]

    def invalidate(self, feed, instance, delete=False):
        if instance != self.instance:
            with self.lock:
                if feed in self._feeds:
                    if delete:
                        del self._feeds[feed]
                    else:
                        del self._feeds[feed].config
