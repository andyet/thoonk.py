--name, id, item, relative, relative_id
if ARGV[2] == null then
    ARGV[2] = redis.call('INCR', 'feed.idincr:'..ARGV[1])
end
exists = redis.call('LREM', 'feed.ids:'..ARGV[1], 1, ARGV[2])
if ARGV[4] == 'BEGIN' then
    redis.call('LPUSH', 'feed.ids:'..ARGV[1], ARGV[2])
elseif ARGV[4] == 'BEFORE' then
    redis.call('LPUSH', 'feed.ids:'..ARGV[1], ARGV[2])
elseif ARGV[4] == 'AFTER' then
    redis.call('LPUSH', 'feed.ids:'..ARGV[1], ARGV[2])
else -- == END
    redis.call('RPUSH', 'feed.ids:'..ARGV[1], ARGV[2])
end 
redis.call('HSET', 'feed.items:'..ARGV[1], ARGV[2], ARGV[3])
redis.call('INCR', 'feed.publishes:'..ARGV[1])
redis.call('PUBLISH', 'feed.position:'..ARGV[1], ARGV[2]..'\0'..ARGV[3])
if ARGV[4] == 'BEGIN' then
    redis.call('PUBLISH', 'feed.position:'..ARGV[1], ARGV[2]..'\0'..'begin:')
elseif ARGV[4] == 'BEFORE' then
    redis.call('PUBLISH', 'feed.position:'..ARGV[1], ARGV[2]..'\0'..':'..ARGV[5])
elseif ARGV[4] == 'AFTER' then
    redis.call('PUBLISH', 'feed.position:'..ARGV[1], ARGV[2]..'\0'..ARGV[5]..':')
else -- == END
    redis.call('PUBLISH', 'feed.position:'..ARGV[1], ARGV[2]..'\0'..':end')
end 
