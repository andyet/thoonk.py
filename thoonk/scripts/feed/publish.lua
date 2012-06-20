--name, id, item, time
local max = redis.call('hget', 'feed.config:'..ARGV[1], 'max_length');
if max then
    local delete_ids = redis.call('zrange', 'feed.ids:'..ARGV[1], 0, '-'..max);
    if #delete_ids ~= 0 then
        redis.call('zrem', 'feed.ids:'..ARGV[1], unpack(delete_ids));
        redis.call('hdel', 'feed.items:'..ARGV[1], unpack(delete_ids));
        for idx, id in ipairs(delete_ids) do
            redis.call('publish', 'event.feed.retract:'..ARGV[1], id);
        end
    end
end
local isnew = redis.call('zadd', 'feed.ids:'..ARGV[1], ARGV[4], ARGV[2]);
redis.call('incr', 'feed.publishes:'..ARGV[1]);
redis.call('hset', 'feed.items:'..ARGV[1], ARGV[2], ARGV[3]);
if isnew then
    redis.call('publish', 'event.feed.publish:'..ARGV[1], ARGV[2]..'\0'..ARGV[3]);
else
    redis.call('publish', 'event.feed.edit:'..ARGV[1], ARGV[2]..'\0'..ARGV[3]);
end
return {false, ARGV[2]};
