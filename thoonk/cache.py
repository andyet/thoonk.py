"""
    Written by Nathan Fritz and Lance Stout. Copyright 2011 by &yet, LLC.
    Released under the terms of the MIT License
"""

import threading
import uuid
from thoonk.exceptions import FeedDoesNotExist

class FeedCache(object):

    """
    The FeedCache class stores an in-memory version of each
    feed. As there may be multiple systems using
    Thoonk with the same Redis server, and each with its own
    FeedCache instance, each FeedCache has a self.instance
    field to uniquely identify itself.

    Attributes:
        thoonk   -- The main Thoonk object.
        instance -- A hex string for uniquely identifying this
                    FeedCache instance.

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

    def __getitem__(self, feed):
        """
        Return a feed object for a given feed name.

        Arguments:
            feed -- The name of the requested feed.
        """
        with self.lock:
            if feed not in self._feeds:
                feed_type = self.thoonk.redis.hget('feed.config:%s' % feed, "type")
                if not feed_type:
                    raise FeedDoesNotExist
                self._feeds[feed] = self.thoonk.feedtypes[feed_type](self.thoonk, feed)
            return self._feeds[feed]
    
    def __delitem__(self, feed):
        with self.lock:
            if feed in self._feeds:
                self._feeds[feed].delete()
                del self._feeds[feed]
