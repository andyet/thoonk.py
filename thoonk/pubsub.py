"""
    Written by Nathan Fritz and Lance Stout. Copyright 2011 by &yet, LLC.
    Released under the terms of the MIT License
"""

import json
import redis
import threading
import uuid

from thoonk import feeds
from thoonk.exceptions import *
from thoonk.config import ConfigCache


class Thoonk(object):

    """
    Thoonk provides a set of additional, high level datatypes with feed-like
    behaviour (feeds, queues, sorted feeds, job queues) to Redis. A default
    Thoonk instance will provide four feed types:
      feed       -- A simple list of entries sorted by publish date. May be
                    either bounded or bounded in size.
      queue      -- A feed that provides FIFO behaviour. Once an item is
                    pulled from the queue, it is removed.
      job        -- Similar to a queue, but an item is not removed from the
                    queue after it has been request until a job complete
                    notice is received.
      sorted feed -- Similar to a normal feed, except that the ordering of
                     items is not limited to publish date, and can be
                     manually adjusted.

    Thoonk.py also provides an additional pyqueue feed type which behaves
    identically to a queue, except that it pickles/unpickles Python
    datatypes automatically.

    The core Thoonk class provides infrastructure for creating and
    managing feeds.

    Attributes:
        db           -- The Redis database number.
        feeds        -- A set of known feed names.
        _feed_config   -- A cache of feed configurations.
        feedtypes    -- A dictionary mapping feed type names to their
                        implementation classes.
        handlers     -- A dictionary mapping event names to event handlers.
        host         -- The Redis server host.
        listen_ready -- A thread event indicating when the listening
                        Redis connection is ready.
        listening    -- A flag indicating if this Thoonk instance is for
                        listening to publish events.
        lredis       -- A Redis connection for listening to publish events.
        port         -- The Redis server port.
        redis        -- The Redis connection instance.

    Methods:
        close             -- Terminate the listening Redis connection.
        create_feed       -- Create a new feed using a given type and config.
        create_notice     -- Execute handlers for feed creation event.
        delete_feed       -- Remove an existing feed.
        delete_notice     -- Execute handlers for feed deletion event.
        feed_exists       -- Determine if a feed has already been created.
        get_feeds         -- Return the set of active feeds.
        listen            -- Start the listening Redis connection.
        publish_notice    -- Execute handlers for item publish event.
        register_feedtype -- Make a new feed type available for use.
        register_handler  -- Assign a function as an event handler.
        retract_notice    -- Execute handlers for item retraction event.
        set_config        -- Set the configuration for a given feed.
    """

    def __init__(self, host='localhost', port=6379, db=0, listen=False):
        """
        Start a new Thoonk instance for creating and managing feeds.

        Arguments:
            host   -- The Redis server name.
            port   -- Port for connecting to the Redis server.
            db     -- The Redis database to use.
            listen -- Flag indicating if this Thoonk instance should listen
                      for feed events and relevant event handlers. Defaults
                      to False.
        """
        self.host = host
        self.port = port
        self.db = db
        self.redis = redis.Redis(host=self.host, port=self.port, db=self.db)
        self.lredis = None

        self.feedtypes = {}
        self.feeds = set()
        self._feed_config = ConfigCache(self)
        self.handlers = {
                'create_notice': [],
                'delete_notice': [],
                'publish_notice': [],
                'retract_notice': [],
                'position_notice': [],
                'stalled_notice': [],
                'retried_notice': [],
                'finished_notice': [],
                'claimed_notice': [],
                'cancelled_notice': []}

        self.listen_ready = threading.Event()
        self.listening = listen

        self.feed_publish = 'feed.publish:%s'
        self.feed_retract = 'feed.retract:%s'
        self.feed_config = 'feed.config:%s'
        self.conf_feed = 'conffeed'
        self.new_feed = 'newfeed'
        self.del_feed = 'delfeed'

        self.register_feedtype(u'feed', feeds.Feed)
        self.register_feedtype(u'queue', feeds.Queue)
        self.register_feedtype(u'job', feeds.Job)
        self.register_feedtype(u'pyqueue', feeds.PythonQueue)
        self.register_feedtype(u'sorted_feed', feeds.SortedFeed)

        if listen:
            #start listener thread
            self.lthread = threading.Thread(target=self.listen)
            self.lthread.daemon = True
            self.lthread.start()
            self.listen_ready.wait()

    def _publish(self, schema, items):
        """
        A shortcut method to publish items separated by \x00.

        Arguments:
            schema -- The key to publish the items to.
            items  -- A tuple or list of items to publish.
        """
        self.redis.publish(schema, "\x00".join(items))

    def __getitem__(self, feed):
        """
        Return the configuration for a feed.

        Arguments:
            feed -- The name of the feed.

        Returns: Dict
        """
        return self._feed_config[feed]

    def __setitem__(self, feed, config):
        """
        Set the configuration for a feed.

        Arguments:
            feed   -- The name of the feed.
            config -- A dict of config values.
        """
        self.set_config(feed, config)

    def register_feedtype(self, feedtype, klass):
        """
        Make a new feed type availabe for use.

        New instances of the feed can be created by using:
            self.<feedtype>()

        For example: self.pyqueue() or self.job().

        Arguments:
            feedtype -- The name of the feed type.
            klass    -- The implementation class for the type.
        """
        self.feedtypes[feedtype] = klass

        def startclass(feed, config=None):
            """
            Instantiate a new feed on demand.

            Arguments:
                feed -- The name of the new feed.
                config -- A dictionary of configuration values.

            Returns: Feed of type <feedtype>.
            """
            if config is None:
                config = {}
            if self.feed_exists(feed):
                return self[feed]
            else:
                if not config.get('type', False):
                    config['type'] = feedtype
                return self.create_feed(feed, config)

        setattr(self, feedtype, startclass)

    def register_handler(self, name, handler):
        """
        Register a function to respond to feed events.

        Event types:
            - create_notice
            - delete_notice
            - publish_notice
            - retract_notice
            - position_notice

        Arguments:
            name    -- The name of the feed event.
            handler -- The function for handling the event.
        """
        if name not in self.handlers:
            self.handlers[name] = []
        self.handlers[name].append(handler)

    def remove_handler(self, name, handler):
        """
        Unregister a function that was registered via register_handler

        Arguments:
            name    -- The name of the feed event.
            handler -- The function for handling the event.
        """
        try:
            self.handlers[name].remove(handler)
        except (KeyError, ValueError):
            pass
        

    def create_feed(self, feed, config):
        """
        Create a new feed with a given configuration.

        The configuration is a dict, and should include a 'type'
        entry with the class of the feed type implementation.

        Arguments:
            feed   -- The name of the new feed.
            config -- A dictionary of configuration values.
        """
        if config is None:
            config = {}
        if not self.redis.sadd("feeds", feed):
            raise FeedExists
        self.feeds.add(feed)
        self.set_config(feed, config)
        self._publish(self.new_feed, (feed, self._feed_config.instance))
        return self[feed]

    def delete_feed(self, feed):
        """
        Delete a given feed.

        Arguments:
            feed -- The name of the feed.
        """
        feed_instance = self._feed_config[feed]
        deleted = False
        while not deleted:
            self.redis.watch('feeds')
            if not self.feed_exists(feed):
                return FeedDoesNotExist
            pipe = self.redis.pipeline()
            pipe.srem("feeds", feed)
            for key in feed_instance.get_schemas():
                pipe.delete(key)
                self._publish(self.del_feed, (feed, self._feed_config.instance))
            try:
                pipe.execute()
                deleted = True
            except redis.exceptions.WatchError:
                deleted = False

    def set_config(self, feed, config):
        """
        Set the configuration for a given feed.

        Arguments:
            feed   -- The name of the feed.
            config -- A dictionary of configuration values.
        """
        if not self.feed_exists(feed):
            raise FeedDoesNotExist
        if type(config) == dict:
            if u'type' not in config:
                config[u'type'] = u'feed'
            jconfig = json.dumps(config)
            dconfig = config
        else:
            dconfig = json.loads(config)
            if u'type' not in dconfig:
                dconfig[u'type'] = u'feed'
            jconfig = json.dumps(dconfig)
        self.redis.set(self.feed_config % feed, jconfig)
        self._publish(self.conf_feed, (feed, self._feed_config.instance))

    def get_config(self, feed):
        if not self.feed_exists(feed):
            raise FeedDoesNotExist
        config = self.redis.get(self.feed_config % feed)
        return json.loads(config)

    def get_feeds(self):
        """
        Return the set of known feeds.

        Returns: set
        """
        return self.feeds

    def feed_exists(self, feed):
        """
        Check if a given feed exists.

        Arguments:
            feed -- The name of the feed.
        """
        if not self.listening:
            if not feed in self.feeds:
                if self.redis.sismember('feeds', feed):
                    self.feeds.add(feed)
                    return True
                return False
            else:
                return True
        return feed in self.feeds

    def close(self):
        """Terminate the listening Redis connection."""
        self.redis.connection.disconnect()
        if self.listening:
            self.lredis.connection.disconnect()

    def listen(self):
        """
        Listen for feed creation and manipulation events and execute
        relevant event handlers. Specifically, listen for:
            - Feed creations
            - Feed deletions
            - Configuration changes
            - Item publications.
            - Item retractions.
        """
        # listener redis object
        self.lredis = redis.Redis(host=self.host, port=self.port, db=self.db)

        # subscribe to feed activities channel
        self.lredis.subscribe((self.new_feed, self.del_feed, self.conf_feed))

        # get set of feeds
        self.feeds.update(self.redis.smembers('feeds'))
        if self.feeds:
            # subscribe to exist feeds retract and publish
            for feed in self.feeds:
                self.lredis.subscribe(self[feed].get_channels())

        self.listen_ready.set()
        for event in self.lredis.listen():
            if event['type'] == 'message':
                if event['channel'].startswith('feed.publish'):
                    #feed publish event
                    id, item = event['data'].split('\x00', 1)
                    self.publish_notice(event['channel'].split(':', 1)[-1],
                                        item, id)
                elif event['channel'].startswith('feed.retract'):
                    self.retract_notice(event['channel'].split(':', 1)[-1],
                                        event['data'])
                elif event['channel'].startswith('feed.position'):
                    self.position_notice(event['channel'].split(':', 1)[-1],
                                         event['data'])
                elif event['channel'].startswith('feed.claimed'):
                    self.claimed_notice(event['channel'].split(':', 1)[-1],
                                        event['data'])
                elif event['channel'].startswith('feed.finished'):
                    id, data = event['data'].split(':', 1)[-1].split("\x00", 1)
                    self.finished_notice(event['channel'].split(':', 1)[-1], id,
                                        data)
                elif event['channel'].startswith('feed.cancelled'):
                    self.cancelled_notice(event['channel'].split(':', 1)[-1],
                                        event['data'])
                elif event['channel'].startswith('feed.stalled'):
                    self.stalled_notice(event['channel'].split(':', 1)[-1],
                                        event['data'])
                elif event['channel'].startswith('feed.retried'):
                    self.retried_notice(event['channel'].split(':', 1)[-1],
                                        event['data'])
                elif event['channel'] == self.new_feed:
                    #feed created event
                    name, instance = event['data'].split('\x00')
                    self.feeds.add(name)
                    config = self.get_config(name)
                    if config["type"] == "job":
                        self.lredis.subscribe((self.feed_publish % name,
                                               "feed.cancelled:%s" % name,
                                               "feed.claimed:%s" % name,
                                               "feed.finished:%s" % name,
                                               "feed.stalled:%s" % name,))
                    else:
                        self.lredis.subscribe((self.feed_publish % name,
                                               self.feed_retract % name))
                    self.create_notice(name)
                elif event['channel'] == self.del_feed:
                    #feed destroyed event
                    name, instance = event['data'].split('\x00')
                    try:
                        self.feeds.remove(name)
                    except KeyError:
                        #already removed -- probably locally
                        pass
                    self._feed_config.invalidate(name, instance, delete=True)
                    self.delete_notice(name)
                elif event['channel'] == self.conf_feed:
                    feed, instance = event['data'].split('\x00', 1)
                    self._feed_config.invalidate(feed, instance)

    def create_notice(self, feed):
        """
        Generate a notice that a new feed has been created and
        execute any relevant event handlers.

        Arguments:
            feed -- The name of the created feed.
        """
        for handler in self.handlers['create_notice']:
            handler(feed)

    def delete_notice(self, feed):
        """
        Generate a notice that a feed has been deleted, and
        execute any relevant event handlers.

        Arguments:
            feed -- The name of the deleted feed.
        """
        for handler in self.handlers['delete_notice']:
            handler(feed)

    def publish_notice(self, feed, item, id):
        """
        Generate a notice that an item has been published to a feed, and
        execute any relevant event handlers.

        Arguments:
            feed -- The name of the feed.
            item -- The content of the published item.
            id   -- The ID of the published item.
        """
        self[feed].event_publish(id, item)
        for handler in self.handlers['publish_notice']:
            handler(feed, item, id)

    def retract_notice(self, feed, id):
        """
        Generate a notice that an item has been retracted from a feed, and
        execute any relevant event handlers.

        Arguments:
            feed -- The name of the feed.
            id   -- The ID of the retracted item.
        """
        self[feed].event_retract(id)
        for handler in self.handlers['retract_notice']:
            handler(feed, id)

    def position_notice(self, feed, id, rel_id):
        """
        Generate a notice that an item has been moved, and
        execute any relevant event handlers.

        Arguments:
            feed   -- The name of the feed.
            id     -- The ID of the moved item.
            rel_id -- Where the item was moved, in relation to
                      existing items.
        """
        for handler in self.handlers['position_notice']:
            handler(feed, id, rel_id)
    
    def stalled_notice(self, feed, id):
        """
        Generate a notice that a job has been stalled, and
        execute any relevant event handlers.

        Arguments:
            feed   -- The name of the feed.
            id     -- The ID of the stalled item.
        """
        for handler in self.handlers['stalled_notice']:
            handler(feed, id)

    def retried_notice(self, feed, id):
        """
        Generate a notice that a job has been retried, and
        execute any relevant event handlers.

        Arguments:
            feed   -- The name of the feed.
            id     -- The ID of the retried item.
        """
        for handler in self.handlers['retried_notice']:
            handler(feed, id)

    def cancelled_notice(self, feed, id):
        """
        Generate a notice that a job has been cancelled, and
        execute any relevant event handlers.

        Arguments:
            feed   -- The name of the feed.
            id     -- The ID of the stalled item.
        """
        for handler in self.handlers['cancelled_notice']:
            handler(feed, id)

    def finished_notice(self, feed, id, result):
        """
        Generate a notice that a job has finished, and
        execute any relevant event handlers.

        Arguments:
            feed   -- The name of the feed.
            id     -- The ID of the stalled item.
        """
        for handler in self.handlers['finished_notice']:
            handler(feed, id, result)

    def claimed_notice(self, feed, id):
        """
        Generate a notice that a job has been claimed, and
        execute any relevant event handlers.

        Arguments:
            feed   -- The name of the feed.
            id     -- The ID of the stalled item.
        """
        for handler in self.handlers['claimed_notice']:
            handler(feed, id)
        