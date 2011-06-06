import time
import uuid

from thoonk.exceptions import *
from thoonk.feeds import Queue


class JobDoesNotExist(Exception):
    pass


class JobNotPending(Exception):
    pass


class Job(Queue):

    def __init__(self, thoonk, feed, config=None):
        Queue.__init__(self, thoonk, feed, config=None)

        self.feed_published = 'feed.published:%s' % feed
        self.feed_cancelled = 'feed.cancelled:%s' % feed
        self.feed_job_claimed = 'feed.claimed:%s' % feed
        self.feed_job_stalled = 'feed.stalled:%s' % feed
        self.feed_job_finished = 'feed.finished:%s\x00%s' % (feed, '%s')
        self.feed_job_running = 'feed.running:%s' % feed

    def get_schemas(self):
        schema = set((self.feed_job_claimed,
                      self.feed_job_stalled,
                      self.feed_job_running,
                      self.feed_published,
                      self.feed_cancelled))

        for id in self.get_ids():
            schema.add(self.feed_job_finished % id)

        return schema.union(Queue.get_schemas(self))

    def get_channels(self):
        return (self.feed_publish, self.feed_retract)

    def event_stalled(self, id, value):
        pass

    def get_ids(self):
        return self.redis.hkeys(self.feed_items)

    def retract(self, id):
        while True:
            self.redis.watch(self.feed_items)
            if self.redis.hexists(self.feed_items, id):
                pipe = self.redis.pipeline()
                pipe.hdel(self.feed_items, id)
                pipe.hdel(self.feed_cancelled, id)
                pipe.zrem(self.feed_published, id)
                pipe.srem(self.feed_job_stalled, id)
                pipe.zrem(self.feed_job_claimed, id)
                pipe.lrem(self.feed_ids, 1, id)
                pipe.delete(self.feed_job_finished % id)
                try:
                    pipe.execute()
                    return
                except redis.exceptions.WatchError:
                    pass
            else:
                self.redis.unwatch()
                break

    def put(self, item, priority=None):
        if priority is None:
            priority = self.NORMAL

        id = uuid.uuid4().hex
        pipe = self.redis.pipeline()

        if priority == self.HIGH:
            pipe.rpush(self.feed_ids, id)
            pipe.hset(self.feed_items, id, item)
            pipe.zadd(self.feed_publishes, id, time.time())
        else:
            pipe.lpush(self.feed_ids, id)
            pipe.incr(self.feed_publishes)
            pipe.hset(self.feed_items, id, item)
            pipe.zadd(self.feed_published, id, time.time())

        results = pipe.execute()
        return id

    def get(self, timeout=0):
        id = self.redis.brpop(self.feed_ids, timeout)
        if id is None:
            return # raise exception?
        id = id[1]

        pipe = self.redis.pipeline()
        pipe.zadd(self.feed_job_claimed, id, time.time())
        pipe.hget(self.feed_items, id)
        result = pipe.execute()
        return id, result[1]

    def finish(self, id, item=None, result=False, timeout=None):
        while True:
            self.redis.watch(self.feed_job_claimed)
            if self.redis.zrank(self.feed_job_claimed, id) is None:
                self.redis.unwatch()
                return # raise exception?

            query = self.redis.hget(self.feed_items, id)

            pipe = self.redis.pipeline()
            pipe.zrem(self.feed_job_claimed, id)
            pipe.hdel(self.feed_cancelled, id)
            if result:
                pipe.lpush(self.feed_job_finished % id, item)
                if timeout is not None:
                    pipe.expire(self.feed_job_finished % id, timeout)
            pipe.hdel(self.feed_items, id)
            try:
                result = pipe.execute()
                break
            except redis.exceptions.WatchError:
                pass

    def get_result(self, id, timeout=0):
        result = self.redis.brpop(self.feed_job_finished % id, timeout)
        if result is not None:
            return result

    def cancel(self, id):
        while True:
            self.redis.watch(self.feed_job_claimed)
            if self.redis.zrank(self.feed_job_claimed, id) is None:
                self.redis.unwatch()
                return # raise exception?

            pipe = self.redis.pipeline()
            pipe.hincrby(self.feed_cancelled, id, 1)
            pipe.lpush(self.feed_ids, id)
            pipe.zrem(self.feed_job_claimed, id)
            try:
                pipe.execute()
                break
            except redis.exceptions.WatchError:
                pass

    def stall(self, id):
        while True:
            self.redis.watch(self.feed_job_claimed)
            if self.redis.zrank(self.feed_job_claimed, id) is None:
                self.redis.unwatch()
                return # raise exception?

            pipe = self.redis.pipeline()
            pipe.zrem(self.feed_job_claimed, id)
            pipe.hdel(self.feed_cancelled, id)
            pipe.sadd(self.feed_job_stalled, id)
            pipe.zrem(self.feed_published, id)
            try:
                pipe.execute()
                break
            except redis.exceptions.WatchError:
                pass

    def retry(self, id):
        while True:
            self.redis.watch(self.feed_job_stalled)
            if self.redis.sismember(self.feed_job_stalled, id) is None:
                self.redis.unwatch()
                return # raise exception?

            pipe = self.redis.pipeline()
            pipe.srem(self.feed_job_stalled, id)
            pipe.lpush(self.feed_ids, id)
            pipe.zadd(self.feed_published, time.time(), id)
            try:
                results = pipe.execute()
                if not results[0]:
                    return # raise exception?
                break
            except redis.exceptions.WatchError:
                pass

    def maintenance(self):
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
