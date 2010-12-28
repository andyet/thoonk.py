import uuid
from consts import *

class LeafNode(object):
    def __init__(self, pubsub):
        self.pubsub = pubsub
        self.redis = pubsub.redis
        self.nodes = pubsub.nodes
        self.nodeconfig = pubsub.nodeconfig

    def delete_node(self, node):
        pipe = self.redis.pipeline()
        pipe.srem("nodes", node)
        pipe.delete([node_schema % node for node_schema in (NODEITEMS, NODEIDS, NODEPUB, NODERETRACT, NODECONFIG)])
        pipe.execute()
        self.nodes.remove(node)
        self.redis.publish(DELNODE, node)

    def get_items(self, node):
        return self.redis.lrange(NODEIDS % node, 0, -1)

    def get_item(self, node, item=None):
        if item is None:
            self.redis.hget(NODEITEMS % node, self.redis.lindex(NODEIDS % node, 0))
        else:
            return self.redis.hget(NODEITEMS % node, item)

    def publish(self, node, item, id=None):
        pipe = self.redis.pipeline()
        if id is None:
            id = uuid.uuid4().hex
            pipe.lpush(NODEIDS % node, id)
        #each condition has the same lpush, but I want to avoid
        #running the second condition if I can
        elif not self.redis.hexists(NODEITEMS % node, id):
            pipe.lpush(self.NODEIDS % node, id)
        pipe.hset(NODEITEMS % node, id, item)
        pipe.execute()
        self.redis.publish(NODEPUB % node, "%s@%s" % (id, item))

    def retract(self, node, id):
        pipe = self.redis.pipeline()
        if not self.redis.hexists(NODEITEMS % node, id):
            raise ItemDoesNotExist
        pipe.lrem(NODEIDS % node, id, num=1)
        pipe.hdel(NODEITEMS % node, id)
        result = pipe.execute()
        self.redis.publish(NODERETRACT % node, id)
        return result
