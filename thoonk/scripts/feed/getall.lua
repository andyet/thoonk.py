--name
local ids = redis.call('zrange', 'feed.ids:'..ARGV[1], 0, -1)
local result = {}
for i, id in ipairs(ids) do
    local value = redis.call('hget', 'feed.items:'..ARGV[1], id)
    table.insert(result, {id=id, item=value})
end
return {false, cjson.encode(result)}
