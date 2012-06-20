-- ARGV: name, id, time
local name, id, time = unpack(ARGV);
if redis.call('srem', 'job.stalled:'..name, id) == 0 then
    return {'Job not stalled', id};
end
redis.call('lpush', 'job.ids:'..name, id);
redis.call('zadd', 'job.published:'..name, time, id);
return {false, id}
