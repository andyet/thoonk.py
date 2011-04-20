import uuid
from consts import *
from exceptions import *
import threading
import json
import cPickle
try:
    import queue
except ImportError:
    import Queue as queue
import time

class ConfigurationField(object):
    pass


class Feed(object):

    def __init__(self, pubsub, feed, config=None):
        self.config_lock = threading.Lock()
        self.config_valid = False
        self.pubsub = pubsub
        self.redis = pubsub.redis
        self.feeds = pubsub.feeds
        self.feedconfig = pubsub.feedconfig
        self.feed = feed
        self._config = None

    def get_channels(self):
        return (FEEDPUB % self.feed, FEEDRETRACT % self.feed)

    def event_publish(self, id, value):
        pass

    def event_retract(self, id):
        pass

    def check_feed(self):
        if not self.pubsub.feed_exists(self.feed):
            raise FeedDoesNotExist

    @property
    def config(self):
        print "getting config..."
        with self.config_lock:
            self.check_feed()
            if not self.config_valid:
                self._config = json.loads(self.pubsub.redis.get(FEEDCONFIG % self.feed))
                self.config_valid = True
            return self._config

    @config.setter
    def config(self, config):
        with self.config_lock:
            self.pubsub.set_feed_config(self.feed, config)
            self.config_valid = False

    @config.deleter
    def config(self):
        with self.config_lock:
            self.check_feed()
            self.config_valid = False

    def delete_feed(self):
        self.check_feed()
        pipe = self.redis.pipeline()
        pipe.srem("feeds", self.feed)
        for key in [feed_schema % self.feed for feed_schema in (FEEDITEMS, FEEDIDS, FEEDPUB, FEEDRETRACT, FEEDCONFIG)]:
            pipe.delete(key)
        self.feeds.remove(self.feed)
        self.redis.publish(DELFEED, "%s\x00\%s" % (self.feed, self.pubsub.feedconfig.instance))

    def get_items(self):
        self.check_feed()
        return self.redis.lrange(FEEDIDS % self.feed, 0, -1)

    def get_item(self, item=None):
        self.check_feed()
        if item is None:
            self.redis.hget(FEEDITEMS % self.feed, self.redis.lindex(FEEDIDS % self.feed, 0))
        else:
            return self.redis.hget(FEEDITEMS % self.feed, item)

    def publish(self, item, id=None):
        self.check_feed()
        pipe = self.redis.pipeline()
        fpushed = False
        if id is None:
            id = uuid.uuid4().hex
            pipe.lpush(FEEDIDS % self.feed, id)
            fpushed = True
        #each condition has the same lpush, but I want to avoid
        #running the second condition if I can
        elif not self.redis.hexists(FEEDITEMS % self.feed, id):
            pipe.lpush(FEEDIDS % self.feed, id)
            fpushed = True
        pipe.hset(FEEDITEMS % self.feed, id, item)
        results = pipe.execute()
        self.redis.publish(FEEDPUB % self.feed, "%s\x00%s" % (id, item))

        # if we increased the size of the feed
        # check to see if we've exceeded the max_length
        if fpushed:
            max_length = int(self.config.get('max_length', 0))
            if max_length and int(results[0]) > max_length:
                delete_ids = self.redis.lrange(FEEDIDS % self.feed, max_length, -1)
                for delete_id in delete_ids:
                    self.retract(delete_id)
        return id

    def retract(self, id):
        self.check_feed()
        #TODO: check hexists in the pipe, add test
        pipe = self.redis.pipeline()
        if not self.redis.hexists(FEEDITEMS % self.feed, id):
            raise ItemDoesNotExist
        pipe.lrem(FEEDIDS % self.feed, id, num=1)
        pipe.hdel(FEEDITEMS % self.feed, id)
        result = pipe.execute()
        self.redis.publish(FEEDRETRACT % self.feed, id)
        return result

class Queue(Feed):

    class Empty(Exception):
        pass
    
    def publish(self, item):
        self.check_feed()
        pipe = self.redis.pipeline()
        id = uuid.uuid4().hex
        pipe.lpush(FEEDIDS % self.feed, id)
        pipe.hset(FEEDITEMS % self.feed, id, item)
        pipe.execute()
        self._publish_number()
        return id

    def put(self, item):
        """alias to publish"""
        return self.publish(item)

    def get(self, timeout=0):
        self.check_feed()
        result = self.redis.brpop(FEEDIDS % self.feed, timeout)
        if result is None:
            raise self.Empty
        id = result[1]
        value = self.redis.hget(FEEDITEMS % self.feed, id)
        self.redis.hdel(FEEDITEMS % self.feed, id)
        self._publish_number()
        return value

    def _publish_number(self):
        #indicates that the length of FEEDIDS has changed
        #self.redis.publish(FEEDPUB % self.feed, "__size__\x00%d" % self.redis.llen(FEEDIDS % self.feed))
        pass

class PythonQueue(Queue):
    def publish(self, item):
        return Queue.publish(self, cPickle.dumps(item))

    def get(self, timeout=0):
        return cPickle.loads(Queue.get(self, timeout))

class Job(Queue):

    class JobDoesNotExist(Exception):
        pass

    class JobNotPending(Exception):
        pass

    def __init__(self, *args, **kwargs):
        Queue.__init__(self, *args, **kwargs)
    
    def get_channels(self):
        return (FEEDPUB % self.feed, FEEDRETRACT % self.feed)

    def event_stalled(self, id, value):
        pass

    def _check_running(self, id):
        return self.redis.sismember(FEEDJOBRUNNING % self.feed, id)

    def put(self, item):
        return self.publish(item)

    def get(self, timeout=0):
        self.check_feed()

        id = self.redis.brpop(FEEDIDS % self.feed, timeout)[1]
        if id is None:
            raise self.Empty
        self.redis.hset(FEEDJOBRUNNING % self.feed, id, time.time())
        value = self.redis.hget(FEEDITEMS % self.feed, id)
        self._publish_number()
        return id, value

    def finish(self, id, value=None, result=False):
        self.check_feed()
        #TODO: should pipe
        if self.redis.hdel(FEEDJOBRUNNING % self.feed, id):
            #self._publish_number()
            if result:
                query = self.redis.hget(FEEDITEMS % self.feed, id)
                self.redis.rpush(FEEDJOBFINISHED % (self.feed, id), "%s\x00%s" % (query, value))
            self.redis.hdel(FEEDITEMS % self.feed, id)
        else:
            raise JobDoesNotExist
    
    def get_result(self, id, timeout=0):
        result = self.redis.blpop(FEEDJOBFINISHED % (self.feed, id), timeout)
        if result is None:
            raise self.empty
        return result[1].split("\x00")

    def check_result(self, id):
        return self.redis.llen(FEEDJOBFINISHED % (self.feed, id))

    def cancel(self, id):
        self.check_feed()
        if self.redis.hdel(FEEDJOBRUNNING % self.feed, id):
            pipe = self.redis.pipeline()
            pipe.lpush(FEEDIDS % self.feed, id)
            pipe.execute()
            self._publish_number()
        else:
            raise self.JobNotPending()

    def stall(self, id):
        self.check_feed()
        self._check_running(id)
        if not self.redis.hdel(FEEDJOBRUNNING % self.feed, id):
            raise self.JobDoesNotExist
        self.redis.lpush(FEEDJOBSTALLED % self.feed, id)

    def retry(self, id):
        if not self.redis.lrem(FEEDJOBSTALL % self.feed, id, num=1):
            raise self.JobDoesNotExist
        self.redis.rpush(FEEDIDS % self.feed, id)
        self._publish_number(self.feed)

