class LeafNode(object):
    def __init__(self, pubsub):
        self.pubsub = pubsub
        self.redis = pubsub.redis

