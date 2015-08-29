local val = redis.call('hget', _:k, _:field);
local ex  = redis.call('hexists', _:k, _:field);
-- local len = redis.call('hstrlen', _:k, _:field); -- Needs Redis 3.2+
local len = 0;
if val then
   len = string.len(val);
end
local sha = "";
if len > 40 then -- Longer than SHA hash
   sha = redis.sha1hex(val);
end
return {val, ex, sha};
