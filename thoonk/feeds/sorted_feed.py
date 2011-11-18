"""
    Written by Nathan Fritz and Lance Stout. Copyright 2011 by &yet, LLC.
    Released under the terms of the MIT License
"""

from thoonk.feeds import Feed

class SortedFeed(Feed):

    """
    A Thoonk sorted feed is a manually ordered collection of items.

    Redis Keys Used:
        feed.idincr:[feed]  -- A counter for ID values.
        feed.publish:[feed] -- A channel for publishing item position
                               change events.

    Thoonk.py Implementation API:
        get_channels -- Return the standard pubsub channels for this feed.
        get_schemas  -- Return the set of Redis keys used by this feed.

    Thoonk Standard API:
        append    -- Append an item to the end of the feed.
        edit      -- Edit an item in-place.
        get_all   -- Return all items in the feed.
        get_ids   -- Return the IDs of all items in the feed.
        get_item  -- Return a single item from the feed given its ID.
        prepend   -- Add an item to the beginning of the feed.
        retract   -- Remove an item from the feed.
        publish   -- Add an item to the end of the feed.
        publish_after  -- Add an item immediately before an existing item.
        publish_before -- Add an item immediately after an existing item.
    """

    def __init__(self, thoonk, feed):
        """
        Create a new SortedFeed object for a given Thoonk feed.

        Note: More than one SortedFeed objects may be create for the same
              Thoonk feed, and creating a SortedFeed object does not
              automatically generate the Thoonk feed itself.

        Arguments:
            thoonk -- The main Thoonk object.
            feed   -- The name of the feed.
            config -- Optional dictionary of configuration values.

        """
        Feed.__init__(self, thoonk, feed)

        self.feed_id_incr = 'feed.idincr:%s' % feed
        self.feed_position = 'feed.position:%s' % feed

    def get_channels(self):
        """
        Return the Redis key channels for publishing and retracting items.
        """
        return (self.feed_publish, self.feed_retract, self.feed_position)

    def get_schemas(self):
        """Return the set of Redis keys used exclusively by this feed."""
        schema = set((self.feed_id_incr,))
        return schema.union(Feed.get_schemas(self))

    def append(self, item):
        """
        Add an item to the end of the feed.

        (Same as publish)

        Arguments:
            item -- The item contents to add.
        """
        return self.publish(item)

    def prepend(self, item):
        """
        Add an item to the beginning of the feed.

        Arguments:
            item -- The item contents to add.
        """
        id = self.redis.incr(self.feed_id_incr)
        pipe = self.redis.pipeline()
        pipe.lpush(self.feed_ids, id)
        pipe.incr(self.feed_publishes)
        pipe.hset(self.feed_items, id, item)
        self.thoonk._publish(self.feed_publish, (str(id), item), pipe)
        self.thoonk._publish(self.feed_position, (str(id), 'begin:'), pipe)
        pipe.execute()
        return id

    def __insert(self, item, rel_id, method):
        """
        Insert an item into the feed, either before or after an
        existing item.

        Arguments:
            item   -- The item contents to insert.
            rel_id -- The ID of an existing item.
            method -- Either 'BEFORE' or 'AFTER', and indicates
                      where the item will be inserted in relation
                      to rel_id.
        """
        id = self.redis.incr(self.feed_id_incr)
        if method == 'BEFORE':
            pos_rel_id = ':%s' % rel_id
        else:
            pos_rel_id = '%s:' % rel_id
        
        def _insert(pipe):
            if not pipe.hexists(self.feed_items, rel_id):
                return # raise exception?
            pipe.multi()
            pipe.linsert(self.feed_ids, method, rel_id, id)
            pipe.hset(self.feed_items, id, item)
            self.thoonk._publish(self.feed_publish, (str(id), item), pipe)
            self.thoonk._publish(self.feed_position, (str(id), pos_rel_id), pipe)
        
        self.redis.transaction(_insert, self.feed_items)
        return id

    def publish(self, item):
        """
        Add an item to the end of the feed.

        (Same as append)

        Arguments:
            item -- The item contens to add.
        """
        id = self.redis.incr(self.feed_id_incr)
        pipe = self.redis.pipeline()
        pipe.rpush(self.feed_ids, id)
        pipe.incr(self.feed_publishes)
        pipe.hset(self.feed_items, id, item)
        self.thoonk._publish(self.feed_publish, (str(id), item), pipe)
        self.thoonk._publish(self.feed_position, (str(id), ':end'), pipe)
        pipe.execute()
        return id

    def edit(self, id, item):
        """
        Modify an item in-place in the feed.

        Arguments:
            id   -- The ID value of the item to edit.
            item -- The new contents of the item.
        """
        def _edit(pipe):
            if not pipe.hexists(self.feed_items, id):
                return # raise exception?
            pipe.multi()
            pipe.hset(self.feed_items, id, item)
            pipe.incr(self.feed_publishes)
            pipe.publish(self.feed_publish, '%s\x00%s' % (id, item))
        
        self.redis.transaction(_edit, self.feed_items)

    def publish_before(self, before_id, item):
        """
        Add an item immediately before an existing item.

        Arguments:
            before_id -- ID of the item to insert before.
            item      -- The item contents to add.
        """
        return self.__insert(item, before_id, 'BEFORE')

    def publish_after(self, after_id, item):
        """
        Add an item immediately after an existing item.

        Arguments:
            after_id -- ID of the item to insert after.
            item     -- The item contents to add.
        """
        return self.__insert(item, after_id, 'AFTER')

    def move(self, rel_position, id):
        """
        Move an existing item to before or after an existing item.

        Specifying the new location for the item is done by:

            :42    -- Move before existing item ID 42.
            42:    -- Move after existing item ID 42.
            begin: -- Move to beginning of the feed.
            :end   -- Move to the end of the feed.

        Arguments:
            rel_position -- A formatted ID to move before/after.
            id           -- The ID of the item to move.
        """
        if rel_position[0] == ':':
            dir = 'BEFORE'
            rel_id = rel_position[1:]
        elif rel_position[-1] == ':':
            dir = 'AFTER'
            rel_id = rel_position[:-1]
        else:
            raise ValueError('Relative ID formatted incorrectly')
        
        def _move(pipe):
            if not pipe.hexists(self.feed_items, id):
                return
            if rel_id not in ['begin', 'end'] and \
               not pipe.hexists(self.feed_items, rel_id):
                return
            pipe.multi()
            pipe.lrem(self.feed_ids, 1, id)
            if rel_id == 'begin':
                pipe.lpush(self.feed_ids, id)
            elif rel_id == 'end':
                pipe.rpush(self.feed_ids, id)
            else:
                pipe.linsert(self.feed_ids, dir, rel_id, id)

            pipe.publish(self.feed_position,
                         '%s\x00%s' % (id, rel_position))
        
        self.redis.transaction(_move, self.feed_items)

    def move_before(self, rel_id, id):
        """
        Move an existing item to before an existing item.

        Arguments:
            rel_id -- An existing item ID.
            id     -- The ID of the item to move.
        """
        self.move(':%s' % rel_id, id)

    def move_after(self, rel_id, id):
        """
        Move an existing item to after an existing item.

        Arguments:
            rel_id -- An existing item ID.
            id     -- The ID of the item to move.
        """
        self.move('%s:' % rel_id, id)

    def move_first(self, id):
        """
        Move an existing item to the start of the feed.

        Arguments:
            id     -- The ID of the item to move.
        """
        self.move('begin:', id)

    def move_last(self, id):
        """
        Move an existing item to the end of the feed.

        Arguments:
            id     -- The ID of the item to move.
        """
        self.move(':end', id)

    def retract(self, id):
        """
        Remove an item from the feed.

        Arguments:
            id -- The ID value of the item to remove.
        """
        def _retract(pipe):
            if pipe.hexists(self.feed_items, id):
                pipe.multi()
                pipe.lrem(self.feed_ids, 1, id)
                pipe.hdel(self.feed_items, id)
                pipe.publish(self.feed_retract, id)
        
        self.redis.transaction(_retract, self.feed_items)

    def get_ids(self):
        """Return the set of IDs used by items in the feed."""
        return self.redis.lrange(self.feed_ids, 0, -1)

    def get_item(self, id):
        """
        Retrieve a single item from the feed.

        Arguments:
            id -- The ID of the item to retrieve.
        """
        return self.redis.hget(self.feed_items, id)

    def get_items(self):
        """Return all items from the feed."""
        return self.redis.hgetall(self.feed_items)
