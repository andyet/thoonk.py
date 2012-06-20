--name, config
local name = ARGV[1];
local config = cjson.decode(ARGV[2]);

if redis.call('SISMEMBER', 'feeds', name) ~= 0 then
    for key, value in pairs(config) do
        redis.call('HSET', 'feed.config:'..name, key, value);
    end
    return {false};
end
return {"Feed doesn't exist"};
