exists = redis.call('HEXISTS', 'feed.items:'..ARGV[1], ARGV[2])
if(not exists) then return false end
redis.call('HSET', 'feed.items:'..ARGV[1], ARGV[2], ARGV[3])
redis.call('INCR', 'feed.publishes:'..ARGV[1])
redis.call('PUBLISH', 'feed.position:'..ARGV[1], ARGV[2]..'\0'..ARGV[3])
return ARGV[2]
