local val = redis.call('get', _:k);
local ex  = redis.call('exists', _:k);
local len = redis.call('strlen', _:k);
local sha = "";
if len > 40 then -- Longer than SHA hash
   sha = redis.sha1hex(val);
end
return {val, ex, sha};
