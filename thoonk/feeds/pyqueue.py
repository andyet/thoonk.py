import cPickle

from thoonk.exceptions import *
from thoonk.feeds import Queue


class PythonQueue(Queue):

    """
    A Thoonk.py addition, the PythonQueue class behaves the
    same as a normal Thoonk queue, except it pickles/unpickles
    items as needed.

    Thoonk.py Implementation API:
        put -- Add a Python object to the queue.
        get -- Retrieve a Python object from the queue.
    """

    def put(self, item, priority=None):
        """
        Add a new item to the queue.

        The item will be pickled before insertion into the queue.

        (Same as self.publish())

        Arguments:
            item     -- The content to add to the queue.
            priority -- Optional priority; if equal to self.HIGH then
                        the item will be inserted at the head of the
                        queue instead of the end.
        """
        item = cPickle.dumps(item)
        return Queue.put(self, item, priority)

    def get(self, timeout=0):
        """
        Retrieve the next item from the queue.

        Raises a self.Empty exception if the request times out.

        The item will be unpickled before returning.

        Arguments:
            timeout -- Optional time in seconds to wait before
                       raising an exception.
        """
        value = Queue.get(self, timeout)
        return cPickle.loads(value)
