"""
    Written by Nathan Fritz and Lance Stout. Copyright 2011 by &yet, LLC.
    Released under the terms of the MIT License
"""

import uuid

from thoonk.exceptions import Empty
from thoonk.feeds import Feed

class Queue(Feed):

    """
    A Thoonk queue is a typical FIFO structure, but with an
    optional priority override for inserting to the head
    of the queue.

    Thoonk Standard API:
        publish -- Alias for put()
        put     -- Add an item to the queue, with optional priority.
        get     -- Retrieve the next item from the queue.
    """

    def publish(self, item, priority=False):
        """
        Add a new item to the queue.

        (Same as self.put())

        Arguments:
            item     -- The content to add to the queue.
            priority -- Optional priority; if equal to True then
                        the item will be inserted at the head of the
                        queue instead of the end.
        """
        self.put(item, priority)

    def put(self, item, priority=False):
        """
        Add a new item to the queue.

        (Same as self.publish())

        Arguments:
            item     -- The content to add to the queue (string).
            priority -- Optional priority; if equal to True then
                        the item will be inserted at the head of the
                        queue instead of the end.
        """
        id = uuid.uuid4().hex
        pipe = self.redis.pipeline()

        if priority:
            pipe.rpush(self.feed_ids, id)
            pipe.hset(self.feed_items, id, item)
            pipe.incr(self.feed_publishes % self.feed)
        else:
            pipe.lpush(self.feed_ids, id)
            pipe.hset(self.feed_items, id, item)
            pipe.incr(self.feed_publishes)

        pipe.execute()
        return id

    def get(self, timeout=0):
        """
        Retrieve the next item from the queue.

        Raises an Empty exception if the request times out.

        Arguments:
            timeout -- Optional time in seconds to wait before
                       raising an exception.
        """
        result = self.redis.brpop(self.feed_ids, timeout)
        if result is None:
            raise Empty

        id = result[1]
        pipe = self.redis.pipeline()
        pipe.hget(self.feed_items, id)
        pipe.hdel(self.feed_items, id)
        results = pipe.execute()

        return results[0]

    def get_ids(self):
        """Return the set of IDs used by jobs in the queue."""
        return self.redis.lrange(self.feed_ids, 0, -1)
