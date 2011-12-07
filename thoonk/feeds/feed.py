"""
    Written by Nathan Fritz and Lance Stout. Copyright 2011 by &yet, LLC.
    Released under the terms of the MIT License
"""

import time
import uuid
try:
    import queue
except ImportError:
    import Queue as queue

from thoonk.exceptions import *
import redis.exceptions

class Feed(object):

    """
    A Thoonk feed is a collection of items ordered by publication date.

    The collection may either be bounded or unbounded in size. A bounded
    feed is created by adding the field 'max_length' to the configuration
    with a value greater than 0.

    Attributes:
        thoonk -- The main Thoonk object.
        redis  -- A Redis connection instance from the Thoonk object.
        feed   -- The name of the feed.

    Redis Keys Used:
        feed.ids:[feed]       -- A sorted set of item IDs.
        feed.items:[feed]     -- A hash table of items keyed by ID.
        feed.publish:[feed]   -- A pubsub channel for publication notices.
        feed.publishes:[feed] -- A counter for number of published items.
        feed.retract:[feed]   -- A pubsub channel for retraction notices.
        feed.config:[feed]    -- A JSON string of configuration data.
        feed.edit:[feed]      -- A pubsub channel for edit notices.

    Thoonk.py Implementation API:
        get_channels  -- Return the standard pubsub channels for this feed.
        event_publish -- Process publication events.
        event_retract -- Process item retraction events.
        delete_feed   -- Delete the feed and its contents.
        get_schemas   -- Return the set of Redis keys used by this feed.

    Thoonk Standard API:
        get_ids  -- Return the IDs of all items in the feed.
        get_item -- Return a single item from the feed given its ID.
        get_all  -- Return all items in the feed.
        publish  -- Publish a new item to the feed, or edit an existing item.
        retract  -- Remove an item from the feed.
    """

    def __init__(self, thoonk, feed):
        """
        Create a new Feed object for a given Thoonk feed.

        Note: More than one Feed objects may be create for the same
              Thoonk feed, and creating a Feed object does not
              automatically generate the Thoonk feed itself.

        Arguments:
            thoonk -- The main Thoonk object.
            feed   -- The name of the feed.
            config -- Optional dictionary of configuration values.
        """
        self.thoonk = thoonk
        self.redis = thoonk.redis
        self.feed = feed

        self.feed_ids = 'feed.ids:%s' % feed
        self.feed_items = 'feed.items:%s' % feed
        self.feed_publish = 'feed.publish:%s' % feed
        self.feed_publishes = 'feed.publishes:%s' % feed
        self.feed_retract = 'feed.retract:%s' % feed
        self.feed_config = 'feed.config:%s' % feed
        self.feed_edit = 'feed.edit:%s' % feed

    # Thoonk.py Implementation API
    # =================================================================

    def get_channels(self):
        """
        Return the Redis key channels for publishing and retracting items.
        """
        return (self.feed_publish, self.feed_retract, self.feed_edit)

    def event_publish(self, id, value):
        """
        Process an item published event.

        Meant to be overridden.

        Arguments:
            id    -- The ID of the published item.
            value -- The content of the published item.
        """
        pass

    def event_retract(self, id):
        """
        Process an item retracted event.

        Meant to be overridden.

        Arguments:
            id -- The ID of the retracted item.
        """
        pass

    def delete_feed(self):
        """Delete the feed and its contents."""
        self.thoonk.delete_feed(self.feed)

    def get_schemas(self):
        """Return the set of Redis keys used exclusively by this feed."""
        return set((self.feed_ids, self.feed_items, self.feed_publish,
                    self.feed_publishes, self.feed_retract, self.feed_config,
                    self.feed_edit))
    
    # Thoonk Standard API
    # =================================================================

    def get_ids(self):
        """Return the set of IDs used by items in the feed."""
        return self.redis.zrange(self.feed_ids, 0, -1)

    def get_item(self, id=None):
        """
        Retrieve a single item from the feed.

        Arguments:
            id -- The ID of the item to retrieve.
        """
        if id is None:
            self.redis.hget(self.feed_items,
                            self.redis.lindex(self.feed_ids, 0))
        else:
            return self.redis.hget(self.feed_items, id)

    def get_all(self):
        """Return all items from the feed."""
        return self.redis.hgetall(self.feed_items)

    def publish(self, item, id=None):
        """
        Publish an item to the feed, or replace an existing item.

        Newly published items will be at the top of the feed, while
        edited items will remain in their original order.

        If the feed has a max length, then the oldest entries will
        be removed to maintain the maximum length.

        Arguments:
            item -- The content of the item to add to the feed.
            id   -- Optional ID to use for the item, if the ID already
                    exists, the existing item will be replaced.
        """
        publish_id = id
        if publish_id is None:
            publish_id = uuid.uuid4().hex
        
        def _publish(pipe):
            max = int(pipe.hget(self.feed_config, "max_length") or 0)
            if max > 0:
                delete_ids = pipe.zrange(self.feed_ids, 0, -max)
                pipe.multi()
                for id in delete_ids:
                    if id != publish_id:
                        pipe.zrem(self.feed_ids, id)
                        pipe.hdel(self.feed_items, id)
                        self.thoonk._publish(self.feed_retract, (id,), pipe)
            else:
                pipe.multi()
            pipe.zadd(self.feed_ids, **{publish_id: time.time()})
            pipe.incr(self.feed_publishes)
            pipe.hset(self.feed_items, publish_id, item)
        
        results = self.redis.transaction(_publish, self.feed_ids)
        
        if results[-3]:
            # If zadd was successful
            self.thoonk._publish(self.feed_publish, (publish_id, item))
        else:
            self.thoonk._publish(self.feed_edit, (publish_id, item))

        return publish_id

    def retract(self, id):
        """
        Remove an item from the feed.

        Arguments:
            id -- The ID value of the item to remove.
        """
        def _retract(pipe):
            if pipe.zrank(self.feed_ids, id) is not None:
                pipe.multi()
                pipe.zrem(self.feed_ids, id)
                pipe.hdel(self.feed_items, id)
                self.thoonk._publish(self.feed_retract, (id,), pipe)
        
        self.redis.transaction(_retract, self.feed_ids)
