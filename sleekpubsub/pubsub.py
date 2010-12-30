import redis
import threading
import uuid
import json
from node import LeafNode
from consts import *

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

class ConfigCache(object):
    def __init__(self, pubsub):
        self._nodes = {}
        self.pubsub = pubsub
        self.lock = threading.Lock()
        self.instance = uuid.uuid4().hex

    def __getitem__(self, node):
        if node in self._nodes:
            return self._nodes[node]
        else:
            with self.lock:
                if not self.pubsub.node_exists(node):
                    raise NodeDoesNotExist
                self._nodes[node] = json.loads(self.pubsub.redis.get(NODECONFIG % node))
                return self._nodes[node]

    def __setitem__(self, node, config):
        with self.lock:
            if not self.pubsub.node_exists(node):
                raise NodeDoesNotExist
            if type(config) == dict:
                if u'type' not in config:
                    config[u'type'] = u'leaf'
                jconfig = json.dumps(config)
                dconfig = config
            else:
                dconfig = json.loads(config)
                if u'type' not in dconfig:
                    dconfig[u'type'] = u'leaf'
                jconfig = json.dumps(dconfig)
            self._nodes[node] = dconfig
            self.pubsub.redis.set(NODECONFIG % node, jconfig)
            self.pubsub.redis.publish(CONFNODE, "%s\x00%s" % (node, self.instance))

    def invalidate(self, data):
        node, instance = data.split("\x00")
        if instance != self.instance:
            with self.lock:
                if node in self._nodes:
                    del self._nodes[node]

class NodeMap(object):
    def __init__(self, pubsub):
        self._pubsub = pubsub

    def __getitem__(self, node):
        if not self._pubsub.node_exists(node):
            raise NodeDoesNotExist
        return self._pubsub.nodetypes[self._pubsub.nodeconfig[node].get(u'type', u'leaf')]


class Pubsub(object):
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.interface = {}
        self.nodetypes = {}
        self.nodes = set()
        self.nodeconfig = ConfigCache(self)
        self.nodemap = NodeMap(self)
        
        self.register_nodetype(u'leaf', LeafNode)

        #start listener thread
        self.lthread = threading.Thread(target = self.listen)
        self.lthread.daemon = True
        self.lthread.start()

    def register_nodetype(self, nodetype, klass):
        self.nodetypes[nodetype] = klass(self)

    def register_interface(self, interface):
        self.interface[interface.name] = interface
        interface.register(self)

    def create_node(self, node, config):
        if not self.redis.sadd("nodes", node):
            raise NodeExists
        self.nodes.add(node)
        self.redis.publish(NEWNODE, node)
        self.update_nodeconfig(node, config)

    def update_nodeconfig(self, node, config):
        if not self.node_exists(node):
            raise NodeDoesNotExist
        self.nodeconfig[node] = config

    def delete_node(self, node):
        self.nodemap[node].delete_node(node)

    def get_nodeconfig(self, node):
        if not self.node_exists(node):
            raise NodeDoesNotExist
        return self.nodeconfig[node]

    def get_nodes(self):
        return self.nodes
    
    def get_items(self, node):
        return self.nodemap[node].get_items(node)

    def get_item(self, node, item=None):
        return self.nodemap[node].get_item(node, item)

    def publish(self, node, item, id=None):
        return self.nodemap[node].publish(node, item, id)

    def retract(self, node, id):
        return self.nodemap[node].retract(node, id)

    def node_exists(self, node):
        return node in self.nodes
        #extra sure way.... worth it?
        #return self.redis.sismember('nodes', node)

    def listen(self):
        #listener redis object
        lredis = redis.Redis(host='localhost', port=6379, db=0)

        # subscribe to node activities channel
        lredis.subscribe((NEWNODE, DELNODE, CONFNODE))

        #get set of nodes
        self.nodes.update(self.redis.smembers('nodes'))
        if self.nodes:
            # subscribe to exist nodes retract and publish
            lredis.subscribe([NODEPUB % node for node in self.nodes])
            lredis.subscribe([NODERETRACT % node for node in self.nodes])

        for event in lredis.listen():
            if event['type'] == 'message':
                if event['channel'].startswith('node.publish'):
                    #node publish event
                    id, item = event['data'].split('\x00', 1)
                    self.publish_notice(event['channel'].split(':', 1)[-1], item, id)
                elif event['channel'].startswith('node.retract'):
                    self.retract_notice(event['channel'].split(':', 1)[-1], event['data'])
                elif event['channel'] == NEWNODE:
                    #node created event
                    self.nodes.add(event['data'])
                    lredis.subscribe([NODEPUB % event['data']])
                    lredis.subscribe([NODERETRACT % event['data']])
                    lredis.subscribe([NODEJOBSTALLED % event['data']])
                    lredis.subscribe([NODEJOBFINISHED % event['data']])
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
                    lredis.unsubscribe([NODEJOBSTALLED % event['data']])
                    lredis.unsubscribe([NODEJOBFINISHED % event['data']])
                    self.nodeconfig.invalidate(event['data'])
                    self.delete_notice(event['data'])
                elif event['channel'] == CONFNODE:
                    self.nodeconfig.invalidate(event['data'])
    
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
