import cPickle

from thoonk.consts import *
from thoonk.exceptions import *
from thoonk.feeds import Queue


class PythonQueue(Queue):

    def publish(self, item):
        return Queue.publish(self, cPickle.dumps(item))

    def get(self, timeout=0):
        return cPickle.loads(Queue.get(self, timeout))
