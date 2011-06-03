import time
import uuid

from thoonk.consts import *
from thoonk.exceptions import *
from thoonk.feeds import Queue


class JobDoesNotExist(Exception):
    pass


class JobNotPending(Exception):
    pass


class Job(Queue):

    def get_channels(self):
        return (FEEDPUB % self.feed, FEEDRETRACT % self.feed)

    def event_stalled(self, id, value):
        pass

    def get_ids(self):
        return self.redis.hkeys(FEEDITEMS % self.feed)

    def retract(self, id):
        while True:
            self.redis.watch(FEEDITEMS % self.feed)
            if self.redis.hexists(FEEDITEMS % self.feed, id):
                pipe = self.redis.pipeline()
                pipe.hdel(FEEDITEMS % self.feed, id)
                pipe.hdel(FEEDCANCELLED % self.feed, id)
                pipe.zrem(FEEDPUBD % self.feed, id)
                pipe.srem(FEEDJOBSTALLED % self.feed, id)
                pipe.zrem(FEEDJOBCLAIMED % self.feed, id)
                pipe.lrem(FEEDIDS % self.feed, 1, id)
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
            pipe.rpush(FEEDIDS % self.feed, id)
            pipe.hset(FEEDITEMS % self.feed, id, item)
            pipe.zadd(FEEDPUBS % self.feed, id, time.time())
        else:
            pipe.lpush(FEEDIDS % self.feed, id)
            pipe.incr(FEEDPUBS % self.feed)
            pipe.hset(FEEDITEMS % self.feed, id, item)
            pipe.zadd(FEEDPUBD % self.feed, id, time.time())

        results = pipe.execute()
        return id

    def get(self, timeout=0):
        id = self.redis.brpop(FEEDIDS % self.feed, timeout)
        if id is None:
            return # raise exception?
        id = id[1]

        pipe = self.redis.pipeline()
        pipe.zadd(FEEDJOBCLAIMED % self.feed, id, time.time())
        pipe.hget(FEEDITEMS % self.feed, id)
        result = pipe.execute()
        return id, result[1]

    def finish(self, id, item=None, result=False, timeout=None):
        while True:
            self.redis.watch(FEEDJOBCLAIMED % self.feed)
            if self.redis.zrank(FEEDJOBCLAIMED % self.feed, id) is None:
                self.redis.unwatch()
                return # raise exception?

            query = self.redis.hget(FEEDITEMS % self.feed, id)

            pipe = self.redis.pipeline()
            pipe.zrem(FEEDJOBCLAIMED % self.feed, id)
            pipe.hdel(FEEDCANCELLED % self.feed, id)
            if result:
                pipe.lpush(FEEDJOBFINISHED % (self.feed, id), '%s\x00%s' % (query, item))
                if timeout is not None:
                    pipe.expire(FEEDJOBFINISHED % (self.feed, id), timeout)
            pipe.hdel(FEEDITEMS % self.feed, id)
            try:
                result = pipe.execute()
                break
            except redis.exceptions.WatchError:
                pass

    def get_result(self, id, timeout=0):
        result = self.redis.brpop(FEEDJOBFINISHED % (self.feed, id), timeout)
        if result is not None:
            return result[1].split('\x00')

    def cancel(self, id):
        while True:
            self.redis.watch(FEEDJOBCLAIMED % self.feed)
            if self.redis.zrank(FEEDJOBCLAIMED % self.feed, id) is None:
                self.redis.unwatch()
                return # raise exception?

            pipe = self.redis.pipeline()
            pipe.hincrby(FEEDCANCELLED % self.feed, id, 1)
            pipe.lpush(FEEDIDS % self.feed, id)
            pipe.zrem(FEEDJOBCLAIMED % self.feed, id)
            try:
                pipe.execute()
                break
            except redis.exceptions.WatchError:
                pass

    def stall(self, id):
        while True:
            self.redis.watch(FEEDJOBCLAIMED % self.feed)
            if self.redis.zrank(FEEDJOBCLAIMED % self.feed, id) is None:
                self.redis.unwatch()
                return # raise exception?

            pipe = self.redis.pipeline()
            pipe.zrem(FEEDJOBCLAIMED % self.feed, id)
            pipe.hdel(FEEDCANCELLED % self.feed, id)
            pipe.sadd(FEEDJOBSTALLED % self.feed, id)
            pipe.zrem(FEEDPUBD % self.feed, id)
            try:
                pipe.execute()
                break
            except redis.exceptions.WatchError:
                pass

    def retry(self, id):
        while True:
            self.redis.watch(FEEDJOBSTALLED % self.feed)
            if self.redis.zrange(FEEDJOBSTALLED % self.feed, id) is None:
                self.redis.unwatch()
                return # raise exception?

            pipe = self.redis.pipeline()
            pipe.srem(FEEDJOBSTALLED % self.feed, id)
            pipe.lpush(FEEDIDS % self.feed, id)
            pipe.zadd(FEEDPUBD % self.feed, time.time(), id)
            try:
                results = pipe.execute()
                if not results[0]:
                    return # raise exception?
                break
            except redis.exceptions.WatchError:
                pass

    def maintenance(self):
        pipe = self.redis.pipeline()
        pipe.hkeys(FEEDITEMS % self.feed)
        pipe.lrange(FEEDIDS % self.feed)
        pipe.zrange(FEEDJOBCLAIMED % self.feed, 0, -1)
        pipe.stall = pipe.smembers(FEEDJOBSTALLED % self.feed)

        keys, avail, claim, stall = pipe.execute()

        unaccounted = [key for key in keys if (key not in avail and \
                                               key not in claim and \
                                               key not in stall)]
        for key in unaccounted:
            self.redis.lpush(FEEDIDS % self.feed, key)
