-- ARGV: name, id, item, time, priority
local name, id, item, time, priority = unpack(ARGV)
if priority ~= nil then
    redis.call('rpush', 'job.ids:'..name, id);
else
    redis.call('lpush', 'job.ids:'..name, id);
end
redis.call('incr', 'job.publishes:'..name);
redis.call('hset', 'job.items:'..name, id, item);
redis.call('zadd', 'job.published:'..name, time, id);
-- return {false, ARGV[3], ARGV[2]};
return {false, item, id}