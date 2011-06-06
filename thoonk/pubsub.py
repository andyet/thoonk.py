import json
import redis
import threading
import uuid

from thoonk import feeds
from thoonk.consts import *
from thoonk.exceptions import *
from thoonk.config import ConfigCache


class Thoonk(object):

    """
    Thoonk provides a set of additional, high level datatypes with feed-like
    behaviour (feeds, queues, lists, job queues) to Redis. A default Thoonk
    instance will provide four feed types:
      feed  -- A simple list of entries sorted by publish date. May be
               either bounded or bounded in size.
      queue -- A feed that provides FIFO behaviour. Once an item is pulled
               from the queue, it is removed.
      job   -- Similar to a queue, but an item is not removed from the
               queue after it has been request until a job complete notice
               is received.
      list  -- Similar to a normal feed, except that the ordering of items
               is not limited to publish date, and can be manually adjusted.

    Thoonk.py also provides an additional pyqueue feed type which behaves
    identically to a queue, except that it pickles/unpickles Python
    datatypes automatically.

    The core Thoonk class provides infrastructure for creating and
    managing feeds.

    Attributes:
        db           -- The Redis database number.
        feeds        -- A set of known feed names.
        feedconfig   -- A cache of feed configurations.
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
        self.feedconfig = ConfigCache(self)
        self.handlers = {
                'create_notice': [],
                'delete_notice': [],
                'publish_notice': [],
                'retract_notice': []}

        self.listen_ready = threading.Event()
        self.listening = listen

        self.register_feedtype(u'feed', feeds.Feed)
        self.register_feedtype(u'queue', feeds.Queue)
        self.register_feedtype(u'job', feeds.Job)
        self.register_feedtype(u'pyqueue', feeds.PythonQueue)
        self.register_feedtype(u'list', feeds.List)

        self.FEED_CONFIG = "feed.config:%s"
        self.FEED_EDIT = "feed.edit:%s"
        self.FEED_IDS = "feed.ids:%s"
        self.FEED_ITEMS = "feed.items:%s"
        self.FEED_PUB = "feed.publish:%s"
        self.FEED_PUBS = "feed.publishes:%s"
        self.FEED_RETRACT = "feed.retract:%s"

        self.FEED_JOB_STALLED = "feed.stalled:%s"
        self.FEED_JOB_FINISHED = "feed.finished:%s:%s"
        self.FEED_JOB_RUNNING = "feed.running:%s"

        self.CONF_FEED = 'conffeed'
        self.DEL_FEED = 'delfeed'
        self.JOB_STATS = 'jobstats'
        self.NEW_FEED = 'newfeed'

        self.feed_schemas = set((self.FEED_CONFIG, self.FEED_EDIT,
                self.FEED_IDS, self.FEED_ITEMS, self.FEED_PUB,
                self.FEED_PUBS, self.FEED_RETRACT))

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
        return self.feedconfig[feed]

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

    def register_schema(self, schema):
        self.feed_schemas.add(schema)

    def register_handler(self, name, handler):
        """
        Register a function to respond to feed events.

        Event types:
            - create_notice
            - delete_notice
            - publish_notice
            - retract_notice

        Arguments:
            name    -- The name of the feed event.
            handler -- The function for handling the event.
        """
        if name not in self.handlers:
            self.handlers[name] = []
        self.handlers.append(handler)

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
        self._publish(NEWFEED, (feed, self.feedconfig.instance))
        return self[feed]

    def delete_feed(self, feed):
        """
        Delete a given feed.

        Arguments:
            feed -- The name of the feed.
        """
        deleted = False
        while not deleted:
            self.redis.watch('feeds')
            if not self.feed_exists(feed):
                return FeedDoesNotExist
            pipe = self.redis.pipeline()
            pipe.srem("feeds", feed)
            for key in [schema % feed for schema in self.feed_schemas]:
                pipe.delete(key)
                self._publish(DELFEED, (feed, self.feedconfig.instance))
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
        self.redis.set(FEEDCONFIG % feed, jconfig)
        self._publish(CONFFEED, (feed, self.feedconfig.instance))

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
        self.lredis.subscribe((NEWFEED, DELFEED, CONFFEED))

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
                elif event['channel'] == NEWFEED:
                    #feed created event
                    name, instance = event['data'].split('\x00')
                    self.feeds.add(name)
                    self.lredis.subscribe((FEEDPUB % name,
                                           FEEDRETRACT % name))
                    self.create_notice(name)
                elif event['channel'] == DELFEED:
                    #feed destroyed event
                    name, instance = event['data'].split('\x00')
                    try:
                        self.feeds.remove(name)
                    except KeyError:
                        #already removed -- probably locally
                        pass
                    self.feedconfig.invalidate(name, instance, delete=True)
                    self.delete_notice(name)
                elif event['channel'] == CONFFEED:
                    feed, instance = event['data'].split('\x00', 1)
                    self.feedconfig.invalidate(feed, instance)

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
            id -- The ID of the retracted item.
        """
        self[feed].event_retract(id)
        for handler in self.handlers['retract_notice']:
            handler(feed, id)
