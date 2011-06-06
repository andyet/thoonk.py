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

    """
    A Thoonk feed is a collection of items ordered by publication date.

    The collection may either be bounded or unbounded in size. A bounded
    feed is created by adding the field 'max_length' to the configuration
    with a value greater than 0.

    Attributes:
        thoonk -- The main Thoonk object.
        redis  -- A Redis connection instance from the Thoonk object.
        feed   -- The name of the feed.
        config -- A dictionary of configuration values.

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

    def __init__(self, thoonk, feed, config=None):
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
        self.config_lock = threading.Lock()
        self.config_valid = False
        self.thoonk = thoonk
        self.redis = thoonk.redis
        self.feed = feed
        self._config = None

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
        return (self.feed_publish, self.feed_retract)

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

    @property
    def config(self):
        """
        Return the feed's configuration.

        If the cached version is marked as invalid, then a new copy of
        the config will be retrieved from Redis.
        """
        with self.config_lock:
            if not self.config_valid:
                conf = self.redis.get(self.feed_config)
                self._config = json.loads(conf)
                self.config_valid = True
            return self._config

    @config.setter
    def config(self, config):
        """
        Set a new configuration for the feed.

        Arguments:
            config -- A dictionary of configuration values.
        """
        with self.config_lock:
            self.thoonk.set_feed_config(self.feed, config)
            self.config_valid = False

    @config.deleter
    def config(self):
        """Mark the current configuration cache as invalid."""
        with self.config_lock:
            self.config_valid = False

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

    def get_item(self, item=None):
        """
        Retrieve a single item from the feed.

        Arguments:
            item --
        """
        if item is None:
            self.redis.hget(self.feed_items,
                            self.redis.lindex(self.feed_ids, 0))
        else:
            return self.redis.hget(self.feed_items, item)

    def get_all(self):
        """Return all items from the feed."""
        return self.redis.hgetall(self.feed_items)

    def publish(self, item, id=None):
        """
        """
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
        """
        Remove an item from the feed.

        Arguments:
            id -- The ID value of the item to remove.
        """
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
