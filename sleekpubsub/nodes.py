import uuid
from consts import *
from exceptions import *
import threading
import json

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
        pipe.delete([node_schema % self.node for node_schema in (NODEITEMS, NODEIDS, NODEPUB, NODERETRACT, NODECONFIG)])
        pipe.execute()
        self.nodes.remove(self.node)
        self.redis.publish(DELNODE, self.node)

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
        self.redis.publish(NODEPUB % self.node, "__size__\x00%d" % self.redis.llen(NODEIDS % self.node))

class Job(Queue):

    def get(self, timeout=60):
        self.check_node()
        result = self.redis.brpop(NODEIDS % self.node, timeout)
        if result is None:
            raise self.Empty
        id = result[1]
        value = self.redis.hget(NODEITEMS % self.node, id)
        self._publish_number()
        return id, value

    def finish(self, id, value):
        self.check_node()
        #TODO: should pipe
        self._check_pending(self.node, id)
        self.redis.lrem(NODEIDS % self.node, id, num=1)
        self.redis.hdel(NODEITEMS % self.node, id)
        self._publish_number(self.node)
        self.redis.publish(NODEJOBFINISHED % self.node, "%s\x00%s" % (id, value))

    def cancel(self, id):
        self.check_node()
        self._check_pending(self.node, id)
        pipe = self.redis.pipeline()
        pipe.lrem(NODEJOBPENDING % self.node, id, num=1)
        pipe.rpush(NODEIDS % self.node, id)
        pipe.execute()
        self._publish_number(self.node)

    def stall(self, id):
        self.check_node()
        self._check_pending(self.node, id)
        pipe = self.redis.pipeline()
        pipe.lrem(NODEIDS % self.node, id, num=1)
        pipe.rpush(NODEIDS % self.node, id)
        pipe.execute()

    def restore(self, id):
        self.check_node()
        self._check_stalled(self.node, id)
        pipe = self.redis.pipeline()
        pipe.lrem(NODEJOBSTALL % self.node, id, num=1)
        pipe.rpush(NODEIDS % self.node, id)
        pipe.execute()
        self._publish_number(self.node)

