import cPickle

from thoonk.exceptions import *
from thoonk.feeds import Queue


class PythonQueue(Queue):

    def publish(self, item, priority=None):
        return self.put(item, priority)

    def put(self, item, priority=None):
        item = cPickle.dumps(item)
        return Queue.put(self, item, priority)

    def get(self, timeout=0):
        value = Queue.get(self, timeout)
        return cPickle.loads(value)
