--name, id
local canretract = redis.call('zrank', 'feed.ids:'..ARGV[1], ARGV[2])
if canretract then
    redis.call('zrem', 'feed.ids:'..ARGV[1], ARGV[2]);
    redis.call('hdel', 'feed.items:'..ARGV[1], ARGV[2]);
    redis.call('publish', 'event.feed.retract:'..ARGV[1], ARGV[2]);
end
return {false, canretract};
