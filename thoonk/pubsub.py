import redis
import threading
import uuid
import json
import feeds
from consts import *
from exceptions import *

class ACL(object):
    def can_publish(self, ident, feed, item, id):
        return True

    def can_create(self, ident, feed, config):
        return True

    def can_delete(self, ident, feed, config):
        return True

    def can_retract(self, ident, feed, item):
        return True

    def subscribe(self, ident, feed, id):
        return True

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

class Pubsub(object):
    def __init__(self, allfeeds=True, host='localhost', port=6379, db=0, listen=False):
        self.allfeeds = allfeeds
        self.host = host
        self.port = port
        self.db = db
        self.redis = redis.Redis(host=self.host, port=self.port, db=self.db)
        self.lredis = None
        self.interface = {}
        self.feedtypes = {}
        self.feeds = set()
        self.feedconfig = ConfigCache(self)
        self.listen_ready = threading.Event()
        self.listening = listen
        
        self.register_feedtype(u'feed', feeds.Feed)
        self.register_feedtype(u'queue', feeds.Queue)
        self.register_feedtype(u'job', feeds.Job)
        self.register_feedtype(u'pyqueue', feeds.PythonQueue)

        if listen:
            #start listener thread
            self.lthread = threading.Thread(target=self.listen)
            self.lthread.daemon = True
            self.lthread.start()
            self.listen_ready.wait()

    def __getitem__(self, feed):
        return self.feedconfig[feed]

    def register_feedtype(self, feedtype, klass):
        self.feedtypes[feedtype] = klass
        def startclass(feed, config={}):
            if self.feed_exists(feed):
                return self[feed]
            else:
                if not config.get('type', False):
                    config['type'] = feedtype
                return self.create_feed(feed, config, True)
        setattr(self, feedtype, startclass)

    def register_interface(self, interface):
        self.interface[interface.name] = interface
        interface.register(self)

    def create_feed(self, feed, config, returnfeed=False):
        if not self.redis.sadd("feeds", feed):
            raise FeedExists
        self.feeds.add(feed)
        self.set_feed_config(feed, config)
        self.redis.publish(NEWFEED, "%s\x00%s" % (feed, self.feedconfig.instance))
        if returnfeed:
            return self[feed]

    def set_feed_config (self, feed, config):
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
        self.redis.publish(CONFFEED, "%s\x00%s" % (feed, self.feedconfig.instance))

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
        #listener redis object
        self.lredis = redis.Redis(host=self.host, port=self.port, db=self.db)

        # subscribe to feed activities channel
        self.lredis.subscribe((NEWFEED, DELFEED, CONFFEED))

        #get set of feeds
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
                    self.publish_notice(event['channel'].split(':', 1)[-1], item, id)
                elif event['channel'].startswith('feed.retract'):
                    self.retract_notice(event['channel'].split(':', 1)[-1], event['data'])
                elif event['channel'] == NEWFEED:
                    #feed created event
                    name, instance = event['data'].split('\x00')
                    self.feeds.add(name)
                    #n = self[name].sp
                    self.lredis.subscribe((FEEDPUB % name, FEEDRETRACT % name))
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
        for ifname in self.interface:
            self.interface[ifname].create_notice(feed)

    def delete_notice(self, feed):
        for ifname in self.interface:
            self.interface[ifname].delete_notice(feed)

    def publish_notice(self, feed, item, id):
        self[feed].event_publish(id, item)
        for ifname in self.interface:
            self.interface[ifname].publish_notice(feed, item, id)

    def retract_notice(self, feed, id):
        self[feed].event_retract(id)
        for ifname in self.interface:
            self.interface[ifname].retract_notice(feed, id)


class Interface(object):
    name = None
    def __init__(self):
        self.pubsub = None
        self.acl = None

    def register(self, pubsub):
        self.pubsub = pubsub
        self.start()

    def start(self):
        """This should be overridden"""
        self.acl = ACL()

    def publish_notice(self, feed, item, id):
        pass

    def retract_notice(self, feed, id):
        pass

    def create_notice(self, feed):
        pass

    def delete_notice(self, feed):
        pass
    
    def finish_notice(self, feed, id, item, result):
        pass
