--name, id
return redis.call('HGET', 'feed.items:'..ARGV[1], ARGV[2])
