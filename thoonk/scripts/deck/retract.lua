--name, id
exists = redis.call('LREM', 'feed.ids:'..ARGV[1], 1, ARGV[2])
if(not exists) then return false end
redis.call('HDEL', 'feed.items:'..ARGV[1], ARGV[2])
redis.call('PUBLISH', 'feed.retract:'..ARGV[1], ARGV[2])
return ARGV[2]
