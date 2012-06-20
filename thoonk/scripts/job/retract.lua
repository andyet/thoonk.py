-- ARGV: name, id
local name, id = unpack(ARGV);
if redis.call('hdel', 'job.items:'..name, id) == 0 then
    return {'Job not found!', id}
end
redis.call('hdel', 'job.cancelled:'..name, id)
redis.call('zrem', 'job.published:'..name, id)
redis.call('srem', 'job.stalled:'..name, id)
redis.call('zrem', 'job.claimed:'..name, id)
redis.call('lrem', 'job.ids:'..name, 1, id)
return {false, id}
