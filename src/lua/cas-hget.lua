local val = redis.call('hget', _:k, _:field);
local ex  = redis.call('hexists', _:k, _:field);
local len = redis.call('hstrlen', _:k, _:field);
local sha = "";
if len > 40 then -- Longer than SHA hash
   sha = redis.sha1hex(val);
end
return {val, ex, sha};
