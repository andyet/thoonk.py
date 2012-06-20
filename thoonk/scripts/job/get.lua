-- ARGV: name, id, time
local name, id, time = unpack(ARGV);
local r1 = redis.call('zadd', 'job.claimed:'..name, time, id);
local r2 = redis.call('hget', 'job.items:'..name, id);
return {r1, r2, id}
