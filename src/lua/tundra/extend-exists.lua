local result = {};
local ttl_ms = tonumber(ARGV[1]);
for i,k in pairs(KEYS) do
   if ttl_ms > 0 and redis.call('pttl', k) > 0 then
      result[i] = redis.call('pexpire', k, ttl_ms);
   else
      result[i] = redis.call('exists', k);
   end
end
return result;
