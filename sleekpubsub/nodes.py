import uuid
from consts import *
from exceptions import *
import threading
import json
try:
    import queue
except ImportError:
    import Queue as queue

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

    def get(self, timeout=60):
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

class Job(Queue):

    class JobDoesNotExist(Exception):
        pass

    class JobNotPending(Exception):
        pass

    def __init__(self, *args, **kwargs):
        Queue.__init__(self, *args, **kwargs)
        self.finished = {}
    
    def get_channels(self):
        return (NODEPUB % self.node, NODERETRACT % self.node, NODEJOBSTALLED % self.node, NODEJOBFINISHED % self.node)

    def event_stalled(self, id, value):
        pass

    def event_finished(self, id, value, result):
        if id in self.finished:
            self.finished[id].put((value, result))

    def _check_pending(self, id):
        return self.redis.sismember(NODEJOBPENDING % self.node, id)

    def publish(self, item):
        id = Queue.publish(self, item)
        q = queue.Queue()
        self.finished[id] = q
        return id, q

    def get(self, timeout=0):
        self.check_node()
        id = self.redis.brpoplpush(NODEIDS % self.node, NODEJOBPENDING % self.node, timeout)
        if id is None:
            raise self.Empty
        value = self.redis.hget(NODEITEMS % self.node, id)
        self._publish_number()
        return id, value

    def finish(self, id, value):
        self.check_node()
        #TODO: should pipe
        if self.redis.lrem(NODEJOBPENDING % self.node, id, num=1):
            query = self.redis.hget(NODEITEMS % self.node, id)
            self.redis.hdel(NODEITEMS % self.node, id)
            #self._publish_number()
            self.redis.publish(NODEJOBFINISHED % self.node, "%s\x00%s\x00%s" % (id, query, value))
        else:
            raise JobDoesNotExist

    def cancel(self, id):
        self.check_node()
        if id in self.finished:
            del self.finished[id]
        if self.redis.lrem(NODEJOBPENDING % self.node, id, num=1):
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
        pipe.rpush(NODEIDS % self.node, id)
        pipe.execute()

    def restore(self, id):
        self.check_node()
        self.finished[id] = queue.Queue()
        self._check_stalled(self.node, id)
        pipe = self.redis.pipeline()
        pipe.lrem(NODEJOBSTALL % self.node, id, num=1)
        pipe.rpush(NODEIDS % self.node, id)
        pipe.execute()
        self._publish_number(self.node)

