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


class Leaf(object):

    def __init__(self, pubsub, node, config=None):
        self.config_lock = threading.Lock()
        self.config_valid = False
        self.pubsub = pubsub
        self.redis = pubsub.redis
        self.nodes = pubsub.nodes
        self.nodeconfig = pubsub.nodeconfig
        self.node = node
        self._config = None

    def get_channels(self):
        return (NODEPUB % self.node, NODERETRACT % self.node)

    def event_publish(self, id, value):
        pass

    def event_retract(self, id):
        pass

    def check_node(self):
        if not self.pubsub.node_exists(self.node):
            raise NodeDoesNotExist

    @property
    def config(self):
        with self.config_lock:
            self.check_node()
            if not self.config_valid:
                self._config = json.loads(self.pubsub.redis.get(NODECONFIG % self.node))
                self.config_valid = True
            return self._config

    @config.setter
    def config(self, config):
        with self.config_lock:
            self.pubsub.set_node_config(self.node, config)
            self.config_valid = True

    @config.deleter
    def config(self):
        with self.config_lock:
            self.check_node()
            self.config_valid = False

    def delete_node(self):
        self.check_node()
        pipe = self.redis.pipeline()
        pipe.srem("nodes", self.node)
        for key in [node_schema % self.node for node_schema in (NODEITEMS, NODEIDS, NODEPUB, NODERETRACT, NODECONFIG)]:
            pipe.delete(key)
        self.nodes.remove(self.node)
        self.redis.publish(DELNODE, "%s\x00\%s\x00%s" % (self.node, self.pubsub.nodeconfig.instance, "\x00".join(self.get_channels())))

    def get_items(self):
        self.check_node()
        return self.redis.lrange(NODEIDS % self.node, 0, -1)

    def get_item(self, item=None):
        self.check_node()
        if item is None:
            self.redis.hget(NODEITEMS % self.node, self.redis.lindex(NODEIDS % self.node, 0))
        else:
            return self.redis.hget(NODEITEMS % self.node, item)

    def publish(self, item, id=None):
        self.check_node()
        pipe = self.redis.pipeline()
        if id is None:
            id = uuid.uuid4().hex
            pipe.lpush(NODEIDS % self.node, id)
        #each condition has the same lpush, but I want to avoid
        #running the second condition if I can
        elif not self.redis.hexists(NODEITEMS % self.node, id):
            pipe.lpush(NODEIDS % self.node, id)
        pipe.hset(NODEITEMS % self.node, id, item)
        pipe.execute()
        self.redis.publish(NODEPUB % self.node, "%s\x00%s" % (id, item))
        return id

    def retract(self, id):
        self.check_node()
        pipe = self.redis.pipeline()
        if not self.redis.hexists(NODEITEMS % self.node, id):
            raise ItemDoesNotExist
        pipe.lrem(NODEIDS % self.node, id, num=1)
        pipe.hdel(NODEITEMS % self.node, id)
        result = pipe.execute()
        self.redis.publish(NODERETRACT % self.node, id)
        return result

class Queue(Leaf):

    class Empty(Exception):
        pass
    
    def publish(self, item):
        self.check_node()
        pipe = self.redis.pipeline()
        id = uuid.uuid4().hex
        pipe.lpush(NODEIDS % self.node, id)
        pipe.hset(NODEITEMS % self.node, id, item)
        pipe.execute()
        self._publish_number()
        return id

    def put(self, item):
        """alias to publish"""
        return self.publish(item)

    def get(self, timeout=0):
        self.check_node()
        result = self.redis.brpop(NODEIDS % self.node, timeout)
        if result is None:
            raise self.Empty
        id = result[1]
        value = self.redis.hget(NODEITEMS % self.node, id)
        self.redis.hdel(NODEITEMS % self.node, id)
        self._publish_number()
        return value

    def _publish_number(self):
        #indicates that the length of NODEIDS has changed
        #self.redis.publish(NODEPUB % self.node, "__size__\x00%d" % self.redis.llen(NODEIDS % self.node))
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
        self.finished = set()
    
    def get_channels(self):
        return (NODEPUB % self.node, NODERETRACT % self.node, NODEJOBSTALLED % self.node)

    def event_stalled(self, id, value):
        pass

    def _check_pending(self, id):
        return self.redis.sismember(NODEJOBPENDING % self.node, id)

    def publish(self, item, reply=False):
        id = Queue.publish(self, item)
        if reply:
            self.finished.add(id)
            return id
        else:
            return id

    def put(self, item, reply=False):
        return self.publish(item, reply)

    def get(self, timeout=0):
        self.check_node()

        id = self.redis.brpop(NODEIDS % self.node, timeout)[1]
        if id is None:
            raise self.Empty
        self.redis.hset(NODEJOBPENDING % self.node, id, time.time())
        value = self.redis.hget(NODEITEMS % self.node, id)
        self._publish_number()
        return id, value

    def finish(self, id, value=None):
        self.check_node()
        #TODO: should pipe
        if self.redis.hdel(NODEJOBPENDING % self.node, id):
            #self._publish_number()
            if id in self.finished:
                query = self.redis.hget(NODEITEMS % self.node, id)
                self.redis.rpush(NODEJOBFINISHED % (self.node, id), "%s\x00%s" % (query, value))
            self.redis.hdel(NODEITEMS % self.node, id)
        else:
            raise JobDoesNotExist
    
    def get_result(self, id, timeout=0):
        result = self.redis.blpop(NODEJOBFINISHED % (self.node, id), timeout)
        if result is None:
            raise self.empty
        return result[1].split("\x00")

    def check_result(self, id):
        return self.redis.llen(NODEJOBFINISHED % (self.node, id))

    def cancel(self, id):
        self.check_node()
        if self.redis.srem(NODEJOBPENDING % self.node, id):
            pipe = self.redis.pipeline()
            pipe.rpush(NODEIDS % self.node, id)
            pipe.execute()
            self._publish_number()
        else:
            raise self.JobNotPending()

    def stall(self, id):
        self.check_node()
        self._check_pending(id)
        pipe = self.redis.pipeline()
        pipe.lrem(NODEIDS % self.node, id, num=1)
        pipe.rpush(NODEJOBSTALL % self.node, id)
        pipe.execute()

    def restore(self, id):
        self.check_node()
        self._check_stalled(self.node, id)
        pipe = self.redis.pipeline()
        pipe.lrem(NODEJOBSTALL % self.node, id, num=1)
        pipe.rpush(NODEIDS % self.node, id)
        pipe.execute()
        self._publish_number(self.node)

