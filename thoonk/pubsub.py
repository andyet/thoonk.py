import json
import redis
import threading
import uuid

from thoonk import feeds
from thoonk.consts import *
from thoonk.exceptions import *
from thoonk.config import ConfigCache


class Thoonk(object):

    def __init__(self, allfeeds=True, host='localhost', port=6379, db=0, listen=False):
        self.allfeeds = allfeeds
        self.host = host
        self.port = port
        self.db = db
        self.redis = redis.Redis(host=self.host, port=self.port, db=self.db)
        self.lredis = None
        self.handlers = {
                'create_notice': [],
                'delete_notice': [],
                'publish_notice': [],
                'retract_notice': []}
        self.feedtypes = {}
        self.feeds = set()
        self.feedconfig = ConfigCache(self)
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
        self.redis.publish(schema, "\x00".join(items))

    def __getitem__(self, feed):
        return self.feedconfig[feed]

    def __setitem__(self, feed, config):
        self.set_config(feed, config)

    def register_feedtype(self, feedtype, klass):
        self.feedtypes[feedtype] = klass
        def startclass(feed, config={}):
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
        if name not in self.handlers:
            self.handlers[name] = []
        self.handlers.append(handler)

    def create_feed(self, feed, config):
        if not self.redis.sadd("feeds", feed):
            raise FeedExists
        self.feeds.add(feed)
        self.set_config(feed, config)
        self._publish(NEWFEED, (feed, self.feedconfig.instance))
        return self[feed]

    def delete_feed(self, feed):
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

    def set_config (self, feed, config):
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
        return self.feeds

    def feed_exists(self, feed, check=False):
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
        self.redis.connection.disconnect()
        if self.listening:
            self.lredis.connection.disconnect()

    def listen(self):
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
        for handler in self.handlers['create_notice']:
            handler(feed)

    def delete_notice(self, feed):
        for handler in self.handlers['delete_notice']:
            handler(feed)

    def publish_notice(self, feed, item, id):
        self[feed].event_publish(id, item)
        for handler in self.handlers['publish_notice']:
            handler(feed, item, id)

    def retract_notice(self, feed, id):
        self[feed].event_retract(id)
        for handler in self.handlers['retract_notice']:
            handler(feed, id)
