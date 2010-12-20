import redis
import threading
import uuid

NODEIDS = "node.ids:%s"
NODEITEMS = "node.items:%s"
NODEPUB = "node.publish:%s"
NODERETRACT = "node.retract:%s"
NEWNODE = "newnode"
DELNODE = "delnode"

class NodeExists(Exception):
    pass

class NotAllowed(Exception):
    pass

class NodeDoesNotExist(Exception):
    pass

class ItemDoesNotExist(Exception):
    pass

class ACL(object):
    def can_publish(self, ident, node, item, id):
        return True

    def can_create(self, ident, node, config):
        return True

    def can_delete(self, ident, node, config):
        return True

    def can_retract(self, ident, node, item):
        return True

    def subscribe(self, ident, node, id):
        return True

class Pubsub(object):
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.interface = {}


        #start listener thread
        self.lthread = threading.Thread(target = self.listen)
        self.lthread.daemon = True
        self.lthread.start()

    def register_interface(self, interface):
        self.interface[interface.name] = interface
        interface.register(self)

    def create_node(self, name, config):
        if not self.redis.sadd("nodes", name):
            raise NodeExists
        self.nodes.add(name)
        self.redis.publish(NEWNODE, name)

    def delete_node(self, name):
        pipe = self.redis.pipeline()
        pipe.srem("nodes", name)
        pipe.delete([node_schema % name for node_schema in (NODEITEMS, NODEIDS, NODEPUB, NODERETRACT)])
        if not pipe.execute()[0]:
            raise NodeDoesNotExist
        self.nodes.remove(name)
        self.redis.publish(DELNODE, name)
    
    def publish(self, node, item, id=None):
        if not self.node_exists(node):
            raise NodeDoesNotExist
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
        if not self.node_exists(node):
            raise NodeDoesNotExist
        pipe = self.redis.pipeline()
        if not self.redis.hexists(NODEITEMS % node, id):
            raise ItemDoesNotExist
        pipe.lrem(NODEIDS % node, id, num=1)
        pipe.hdel(NODEITEMS % node, id)
        print pipe.execute()
        self.redis.publish(NODERETRACT % node, id)

    def node_exists(self, node):
        return self.redis.sismember('nodes', node)

    def listen(self):
        #get set of nodes
        self.nodes = self.redis.smembers('nodes')

        #listener redis object
        lredis = redis.Redis(host='localhost', port=6379, db=0)

        if self.nodes:
            # subscribe to exist nodes retract and publish
            lredis.subscribe([NODEPUB % node for node in self.nodes])
            lredis.subscribe([NODERETRACT % node for node in self.nodes])
        # subscribe to node activities channel
        lredis.subscribe((NEWNODE, DELNODE))

        for event in lredis.listen():
            if event['type'] == 'message':
                if event['channel'].startswith('node.publish'):
                    #node publish event
                    id, item = event['data'].split('@', 1)
                    self.publish_notice(event['channel'].split(':', 1)[-1], item, id)
                elif event['channel'].startswith('node.retract'):
                    self.retract_notice(event['channel'].split(':', 1)[-1], event['data'])
                elif event['channel'] == NEWNODE:
                    #node created event
                    print "new node: %s" % event['data']
                    self.nodes.add(event['data'])
                    lredis.subscribe([NODEPUB % event['data']])
                    lredis.subscribe([NODERETRACT % event['data']])
                    self.create_notice(event['data'])
                elif event['channel'] == DELNODE:
                    #node destroyed event
                    try:
                        self.nodes.remove(event['data'])
                    except KeyError:
                        #already removed -- probably locally
                        pass
                    lredis.unsubscribe([NODEPUB % event['data']])
                    lredis.unsubscribe([NODERETRACT % event['data']])
                    self.delete_notice(event['data'])
    
    def create_notice(self, node):
        for ifname in self.interface:
            self.interface[ifname].create_notice(node)

    def delete_notice(self, node):
        for ifname in self.interface:
            self.interface[ifname].delete_notice(node)

    def publish_notice(self, node, item, id):
        for ifname in self.interface:
            self.interface[ifname].publish_notice(node, item, id)

    def retract_notice(self, node, id):
        for ifname in self.interface:
            self.interface[ifname].retract_notice(node, id)

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

    def publish_notice(self, node, item, id):
        pass

    def retract_notice(self, node, id):
        pass

    def create_notice(self, node):
        pass

    def delete_notice(self, node):
        pass
