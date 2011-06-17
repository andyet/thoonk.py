"""
    Written by Nathan Fritz and Lance Stout. Copyright 2011 by &yet, LLC.
    Released under the terms of the MIT License
"""

import json
import threading
import uuid


class ConfigCache(object):

    """
    The ConfigCache class stores an in-memory version of each
    feed's configuration. As there may be multiple systems using
    Thoonk with the same Redis server, and each with its own
    ConfigCache instance, each ConfigCache has a self.instance
    field to uniquely identify itself.

    Attributes:
        thoonk   -- The main Thoonk object.
        instance -- A hex string for uniquely identifying this
                    ConfigCache instance.

    Methods:
        invalidate -- Force a feed's config to be retrieved from
                      Redis instead of in-memory.
    """

    def __init__(self, thoonk):
        """
        Create a new configuration cache.

        Arguments:
            thoonk -- The main Thoonk object.
        """
        self._feeds = {}
        self.thoonk = thoonk
        self.lock = threading.Lock()
        self.instance = uuid.uuid4().hex

    def __getitem__(self, feed):
        """
        Return a feed object for a given feed name.

        Arguments:
            feed -- The name of the requested feed.
        """
        with self.lock:
            if feed in self._feeds:
                return self._feeds[feed]
            else:
                if not self.thoonk.feed_exists(feed):
                    raise FeedDoesNotExist
                config = self.thoonk.redis.get('feed.config:%s' % feed)
                config = json.loads(config)
                feed_type = config.get(u'type', u'feed')
                feed_class = self.thoonk.feedtypes[feed_type]
                self._feeds[feed] = feed_class(self.thoonk, feed, config)
                return self._feeds[feed]

    def invalidate(self, feed, instance, delete=False):
        """
        Delete a configuration so that it will be retrieved from Redis
        instead of from the cache.

        Arguments:
            feed     -- The name of the feed to invalidate.
            instance -- A UUID identifying the cache which made the
                        invalidation request.
            delete   -- Indicates if the entire feed object should be
                        invalidated, or just its configuration.
        """
        if instance != self.instance:
            with self.lock:
                if feed in self._feeds:
                    if delete:
                        del self._feeds[feed]
                    else:
                        del self._feeds[feed].config
