"""
    Written by Nathan Fritz and Lance Stout. Copyright 2011 by &yet, LLC.
    Released under the terms of the MIT License
"""

import redis
import threading
import uuid

from thoonk import feeds, cache
from thoonk.exceptions import FeedExists, FeedDoesNotExist, NotListening

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
        self.redis = redis.StrictRedis(host=self.host, port=self.port, db=self.db)
        self._feeds = cache.FeedCache(self)
        self.instance = uuid.uuid4().hex

        self.feedtypes = {}

        self.listening = listen
        self.listener = None

        self.feed_publish = 'feed.publish:%s'
        self.feed_retract = 'feed.retract:%s'
        self.feed_config = 'feed.config:%s'

        self.register_feedtype(u'feed', feeds.Feed)
        self.register_feedtype(u'queue', feeds.Queue)
        self.register_feedtype(u'job', feeds.Job)
        self.register_feedtype(u'pyqueue', feeds.PythonQueue)
        self.register_feedtype(u'sorted_feed', feeds.SortedFeed)

        if listen:
            self.listener = ThoonkListener(self)
            self.listener.start()
            self.listener.ready.wait()

    def _publish(self, schema, items=[], pipe=None):
        """
        A shortcut method to publish items separated by \x00.

        Arguments:
            schema -- The key to publish the items to.
            items  -- A tuple or list of items to publish.
            pipe   -- A redis pipeline to use to publish the item using.
                      Note: it is up to the caller to execute the pipe after
                      publishing
        """
        if pipe:
            pipe.publish(schema, "\x00".join(items))
        else:
            self.redis.publish(schema, "\x00".join(items))

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
            config['type'] = feedtype
            try:
                self.create_feed(feed, config)
            except FeedExists:
                pass
            return self._feeds[feed]


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
        if self.listener:
            self.listener.register_handler(name, handler)
        else:
            raise NotListening

    def remove_handler(self, name, handler):
        """
        Unregister a function that was registered via register_handler

        Arguments:
            name    -- The name of the feed event.
            handler -- The function for handling the event.
        """
        if self.listener:
            self.listener.remove_handler(name, handler)
        else:
            raise NotListening

    def create_feed(self, feed, config):
        """
        Create a new feed with a given configuration.

        The configuration is a dict, and should include a 'type'
        entry with the class of the feed type implementation.

        Arguments:
            feed   -- The name of the new feed.
            config -- A dictionary of configuration values.
        """
        if not self.redis.sadd("feeds", feed):
            raise FeedExists
        self.set_config(feed, config, True)

    def delete_feed(self, feed):
        """
        Delete a given feed.

        Arguments:
            feed -- The name of the feed.
        """
        feed_instance = self._feeds[feed]

        def _delete_feed(pipe):
            if not pipe.sismember('feeds', feed):
                raise FeedDoesNotExist
            pipe.multi()
            pipe.srem("feeds", feed)
            for key in feed_instance.get_schemas():
                pipe.delete(key)
            self._publish('delfeed', (feed, self.instance), pipe)

        self.redis.transaction(_delete_feed, 'feeds')

    def set_config(self, feed, config, new_feed=False):
        """
        Set the configuration for a given feed.

        Arguments:
            feed   -- The name of the feed.
            config -- A dictionary of configuration values.
        """
        if not self.feed_exists(feed):
            raise FeedDoesNotExist
        if u'type' not in config:
            config[u'type'] = u'feed'
        pipe = self.redis.pipeline()
        for k, v in config.iteritems():
            pipe.hset('feed.config:' + feed, k, v)
        pipe.execute()
        if new_feed:
            self._publish('newfeed', (feed, self.instance))
        self._publish('conffeed', (feed, self.instance))

    def get_feed_names(self):
        """
        Return the set of known feeds.

        Returns: set
        """
        return self.redis.smembers('feeds') or set()

    def feed_exists(self, feed):
        """
        Check if a given feed exists.

        Arguments:
            feed -- The name of the feed.
        """
        return self.redis.sismember('feeds', feed)

    def close(self):
        """Terminate the listening Redis connection."""
        if self.listening:
            self.redis.publish(self.listener._finish_channel, "")
            self.listener.finished.wait()
        self.redis.connection_pool.disconnect()


class ThoonkListener(threading.Thread):

    def __init__(self, thoonk, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self.lock = threading.Lock()
        self.handlers = {}
        self.thoonk = thoonk
        self.ready = threading.Event()
        self.redis = redis.StrictRedis(host=thoonk.host, port=thoonk.port, db=thoonk.db)
        self.finished = threading.Event()
        self.instance = thoonk.instance
        self._finish_channel = "listenerclose_%s" % self.instance
        self._pubsub = None
        self.daemon = True

    def finish(self):
        self.redis.publish(self._finish_channel, "")

    def run(self):
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
        self._pubsub = self.redis.pubsub()
        # subscribe to feed activities channel
        self._pubsub.subscribe((self._finish_channel, 'newfeed', 'delfeed', 'conffeed'))

        # subscribe to exist feeds retract and publish
        for feed in self.redis.smembers("feeds"):
            self._pubsub.subscribe(self.thoonk._feeds[feed].get_channels())

        self.ready.set()
        for event in self._pubsub.listen():
            type = event.pop("type")
            if event["channel"] == self._finish_channel:
                if self._pubsub.subscription_count:
                    self._pubsub.unsubscribe()
            elif type == 'message':
                self._handle_message(**event)
            elif type == 'pmessage':
                self._handle_pmessage(**event)

        self.finished.set()

    def _handle_message(self, channel, data, pattern=None):
        if channel == 'newfeed':
            #feed created event
            name, _ = data.split('\x00')
            self._pubsub.subscribe(("feed.publish:"+name, "feed.edit:"+name,
                "feed.retract:"+name, "feed.position:"+name, "job.finish:"+name))
            self.emit("create", name)

        elif channel == 'delfeed':
            #feed destroyed event
            name, _ = data.split('\x00')
            try:
                del self._feeds[name]
            except:
                pass
            self.emit("delete", name)

        elif channel == 'conffeed':
            feed, _ = data.split('\x00', 1)
            self.emit("config:"+feed, None)

        elif channel.startswith('feed.publish'):
            #feed publish event
            id, item = data.split('\x00', 1)
            self.emit("publish", channel.split(':', 1)[-1], item, id)

        elif channel.startswith('feed.edit'):
            #feed publish event
            id, item = data.split('\x00', 1)
            self.emit("edit", channel.split(':', 1)[-1], item, id)

        elif channel.startswith('feed.retract'):
            self.emit("retract", channel.split(':', 1)[-1], data)

        elif channel.startswith('feed.position'):
            id, rel_id = data.split('\x00', 1)
            self.emit("position", channel.split(':', 1)[-1], id, rel_id)

        elif channel.startswith('job.finish'):
            id, result = data.split('\x00', 1)
            self.emit("finish", channel.split(':', 1)[-1], id, result)

    def emit(self, event, *args):
        with self.lock:
            for handler in self.handlers.get(event, []):
                handler(*args)

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
        with self.lock:
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
        with self.lock:
            try:
                self.handlers[name].remove(handler)
            except (KeyError, ValueError):
                pass
