-- ARGV: name, id
local name, id = unpack(ARGV)
if redis.call('zrem', 'job.claimed:'..name, id) == 0 then
    return {'No job found!', id}
end
redis.call('hincrby', 'job.cancelled:'..name, id, 1)
redis.call('lpush', 'job.ids:'..name, id)
return {false, id}
