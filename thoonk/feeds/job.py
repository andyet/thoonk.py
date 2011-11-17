"""
    Written by Nathan Fritz and Lance Stout. Copyright 2011 by &yet, LLC.
    Released under the terms of the MIT License
"""

import time
import uuid
import redis

from thoonk.exceptions import *
from thoonk.feeds import Queue
from thoonk.feeds.queue import Empty


class JobDoesNotExist(Exception):
    pass


class JobNotPending(Exception):
    pass


class Job(Queue):

    """
    A Thoonk Job is a queue which does not completely remove items
    from the queue until a task completion notice is received.

    Job Item Lifecycle:
        - A job is created using self.put() with the data for the job.
        - The job is moved to a claimed state when a worker retrieves
          the job data from the queue.
        - The worker performs any processing required, and calls
          self.finish() with the job's result data.
        - The job is marked as finished and removed from the queue.

    Alternative: Job Cancellation
        - After a worker has claimed a job, it calls self.cancel() with
          the job's ID, possibly because of an error or lack of required
          resources.
        - The job is moved from a claimed state back to the queue.

    Alternative: Job Stalling
        - A call to self.stall() with the job ID is made.
        - The job is moved out of the queue and into a stalled state. While
          stalled, the job will not be dispatched.
        - A call to self.retry() with the job ID is made.
        - The job is moved out of the stalled state and back into the queue.

    Alternative: Job Deletion
        - A call to self.retract() with the job ID is made.
        - The job item is completely removed from the queue and any
          other job states.

    Redis Keys Used:
        feed.published:[feed] -- A time sorted set of queued jobs.
        feed.cancelled:[feed] -- A hash table of cancelled jobs.
        feed.claimed:[feed]   -- A hash table of claimed jobs.
        feed.stalled:[feed]   -- A hash table of stalled jobs.
        feed.running:[feed]   -- A hash table of running jobs.
        feed.finished:[feed]\x00[id] -- Temporary queue for receiving job
                                        result data.

    Thoonk.py Implementation API:
        get_schemas   -- Return the set of Redis keys used by this feed.

    Thoonk Standard API:
        cancel      -- Move a job from a claimed state back into the queue.
        finish      -- Mark a job as completed and store the results.
        get         -- Retrieve the next job from the queue.
        get_ids     -- Return IDs of all jobs in the queue.
        get_result  -- Retrieve the result of a job.
        maintenance -- Perform periodic house cleaning.
        put         -- Add a new job to the queue.
        retract     -- Completely remove a job from use.
        retry       -- Resume execution of a stalled job.
        stall       -- Pause execution of a queued job.
    """

    def __init__(self, thoonk, feed, config=None):
        """
        Create a new Job queue object for a given Thoonk feed.

        Note: More than one Job queue objects may be create for
              the same Thoonk feed, and creating a Job queue object
              does not automatically generate the Thoonk feed itself.

        Arguments:
            thoonk -- The main Thoonk object.
            feed   -- The name of the feed.
            config -- Optional dictionary of configuration values.
        """
        Queue.__init__(self, thoonk, feed, config=None)

        self.feed_publishes = 'feed.publishes:%s' % feed
        self.feed_cancelled = 'feed.cancelled:%s' % feed
        self.feed_retried = 'feed.retried:%s' % feed
        self.feed_finished = 'feed.finished:%s' % feed
        self.feed_job_claimed = 'feed.claimed:%s' % feed
        self.feed_job_stalled = 'feed.stalled:%s' % feed
        self.feed_job_finished = 'feed.finished:%s\x00%s' % (feed, '%s')
        self.feed_job_running = 'feed.running:%s' % feed

    def get_channels(self):
        return (self.feed_publishes, self.feed_job_claimed, self.feed_job_stalled,
            self.feed_finished, self.feed_cancelled, self.feed_retried)

    def get_schemas(self):
        """Return the set of Redis keys used exclusively by this feed."""
        schema = set((self.feed_job_claimed,
                      self.feed_job_stalled,
                      self.feed_job_running,
                      self.feed_publishes,
                      self.feed_cancelled))

        for id in self.get_ids():
            schema.add(self.feed_job_finished % id)
        
        return schema.union(Queue.get_schemas(self))

    def get_ids(self):
        """Return the set of IDs used by jobs in the queue."""
        return self.redis.hkeys(self.feed_items)

    def retract(self, id):
        """
        Completely remove a job from use.

        Arguments:
            id -- The ID of the job to remove.
        """
        def _retract(pipe):
            if pipe.hexists(self.feed_items, id):
                pipe.multi()
                pipe.hdel(self.feed_items, id)
                pipe.hdel(self.feed_cancelled, id)
                pipe.zrem(self.feed_publishes, id)
                pipe.srem(self.feed_job_stalled, id)
                pipe.zrem(self.feed_job_claimed, id)
                pipe.lrem(self.feed_ids, 1, id)
                pipe.delete(self.feed_job_finished % id)
        
        self.redis.transaction(_retract, self.feed_items)

    def put(self, item, priority=False):
        """
        Add a new job to the queue.

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
        else:
            pipe.lpush(self.feed_ids, id)
            pipe.incr(self.feed_publishes)
        pipe.hset(self.feed_items, id, item)
        pipe.zadd(self.feed_publishes, **{id: time.time()})

        results = pipe.execute()

        if results[-1]:
            # If zadd was successful
            self.thoonk._publish(self.feed_publishes, (id, item))
        else:
            self.thoonk._publish(self.feed_edit, (id, item))

        return id

    def get(self, timeout=0):
        """
        Retrieve the next job from the queue.

        Raises an Empty exception if the request times out.

        Arguments:
            timeout -- Optional time in seconds to wait before
                       raising an exception.
        
        Returns:
            id      -- The id of the job
            job     -- The job content
            cancelled -- The number of times the job has been cancelled
        """
        id = self.redis.brpop(self.feed_ids, timeout)
        if id is None:
            raise Empty
        id = id[1]

        pipe = self.redis.pipeline()
        pipe.zadd(self.feed_job_claimed, **{id: time.time()})
        pipe.hget(self.feed_items, id)
        pipe.hget(self.feed_cancelled, id)
        result = pipe.execute()
        
        self.thoonk._publish(self.feed_job_claimed, (id,))

        return id, result[1], 0 if result[2] is None else int(result[2])

    def finish(self, id, item=None, result=False, timeout=None):
        """
        Mark a job as completed, and store any results.

        Arguments:
            id      -- The ID of the completed job.
            item    -- The result data from the job.
            result  -- Flag indicating that result data should be stored.
                       Defaults to False.
            timeout -- Time in seconds to keep the result data. The default
                       is to store data indefinitely until retrieved.
        """
        def _finish(pipe):
            if pipe.zrank(self.feed_job_claimed, id) is None:
                return # raise exception?
            #query = pipe.hget(self.feed_items, id)
            pipe.multi()
            pipe.zrem(self.feed_job_claimed, id)
            pipe.hdel(self.feed_cancelled, id)
            if result:
                pipe.lpush(self.feed_job_finished % id, item)
                if timeout is not None:
                    pipe.expire(self.feed_job_finished % id, timeout)
            pipe.hdel(self.feed_items, id)
            self.thoonk._publish(self.feed_finished, 
                (id, item if result else ""), pipe)
        
        self.redis.transaction(_finish, self.feed_job_claimed)

    def get_result(self, id, timeout=0):
        """
        Retrieve the result of a given job.

        Arguments:
            id      -- The ID of the job to check for results.
            timeout -- Time in seconds to wait for results to arrive.
                       Default is to block indefinitely.
        """
        result = self.redis.brpop(self.feed_job_finished % id, timeout)
        if result is not None:
            return result

    def cancel(self, id):
        """
        Move a claimed job back to the queue.

        Arguments:
            id -- The ID of the job to cancel.
        """
        def _cancel(pipe):
            if self.redis.zrank(self.feed_job_claimed, id) is None:
                return # raise exception?
            pipe.multi()
            pipe.hincrby(self.feed_cancelled, id, 1)
            pipe.lpush(self.feed_ids, id)
            pipe.zrem(self.feed_job_claimed, id)
            self.thoonk._publish(self.feed_cancelled, (id,), pipe)
        
        self.redis.transaction(_cancel, self.feed_job_claimed)

    def stall(self, id):
        """
        Move a job out of the queue in order to pause processing.

        While stalled, a job will not be dispatched to requesting workers.

        Arguments:
            id -- The ID of the job to pause.
        """
        def _stall(pipe):
            if pipe.zrank(self.feed_job_claimed, id) is None:
                return # raise exception?
            pipe.multi()
            pipe.zrem(self.feed_job_claimed, id)
            pipe.hdel(self.feed_cancelled, id)
            pipe.sadd(self.feed_job_stalled, id)
            pipe.zrem(self.feed_publishes, id)
            self.thoonk._publish(self.feed_job_stalled, (id,), pipe)
        
        self.redis.transaction(_stall, self.feed_job_claimed)

    def retry(self, id):
        """
        Move a job from a stalled state back into the job queue.

        Arguments:
            id -- The ID of the job to resume.
        """
        def _retry(pipe):
            if pipe.sismember(self.feed_job_stalled, id) is None:
                return # raise exception?
            pipe.multi()
            pipe.srem(self.feed_job_stalled, id)
            pipe.lpush(self.feed_ids, id)
            pipe.zadd(self.feed_publishes, **{id: time.time()})
            self.thoonk._publish(self.feed_retried, (id,), pipe)
        
        results = self.redis.transaction(_retry, self.feed_job_stalled)
        if not results[0]:
            return # raise exception?

    def maintenance(self):
        """
        Perform periodic house cleaning.

        Fix any inconsistencies such as jobs that are not in any state, etc,
        that can be caused by software crashes and other unexpected events.

        Expected use is to create a maintenance thread for periodically
        calling this method.
        """
        pipe = self.redis.pipeline()
        pipe.hkeys(self.feed_items)
        pipe.lrange(self.feed_ids)
        pipe.zrange(self.feed_job_claimed, 0, -1)
        pipe.stall = pipe.smembers(self.feed_job_stalled)

        keys, avail, claim, stall = pipe.execute()

        unaccounted = [key for key in keys if (key not in avail and \
                                               key not in claim and \
                                               key not in stall)]
        for key in unaccounted:
            self.redis.lpush(self.feed_ids, key)
