--name
return redis.call('LLEN', 'feed.ids:'..ARGV[1])
