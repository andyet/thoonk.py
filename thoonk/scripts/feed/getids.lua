--name
local results = redis.call('ZRANGE', 'feed.ids:'..ARGV[1], 0, -1);
return {false, results};
