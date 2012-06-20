-- ARGV: name, id, result
local name, id, result = unpack(ARGV);

if redis.call('zrem', 'job.claimed:'..name, id) == 0 then
    return {'Job not claimed', false};
end
redis.call('hdel', 'job.cancelled:'..name, id)
redis.call('zrem', 'job.published:'..name, id)
redis.call('incr', 'job.finishes:'..name)
local publish_payload;
if result then
    publish_payload = id.."\0"..result
else
    publish_payload = id
end
redis.call('publish', 'event.job.finish:'..name, publish_payload)
redis.call('hdel', 'job.items:'..name, id)
if result then
    return {false, id, result};
else
    return {false, id};
end
