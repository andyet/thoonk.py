if not redis.call('zrank', 'feed.ids:'..ARGV[1], ARGV[2]) then
    return {false, false};
else
    return {false, true};
end
