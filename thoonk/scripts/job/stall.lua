-- ARGV: name, id
local name, id = unpack(ARGV);
if redis.call('zrem', 'job.claimed:'..name, id) == 0 then
    return {'Job not claimed', id};
end
redis.call('hdel', 'job.cancelled:'..name, id);
redis.call('sadd', 'job.stalled:'..name, id);
redis.call('zrem', 'job.published'..name, id);
return {false, id}
