--name, config
local name = ARGV[1];
local config = cjson.decode(ARGV[2]);

if redis.call('SISMEMBER', 'jobs', name) ~= 0 then
    for key, value in pairs(config) do
        redis.call('HSET', 'job.config:'..name, key, value);
    end
    return {false};
end
return {"Job doesn't exist"};
