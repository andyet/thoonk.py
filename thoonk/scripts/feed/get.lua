local result = redis.call('hget', 'feed.items:'..ARGV[1], ARGV[2]);
return {false, result};
