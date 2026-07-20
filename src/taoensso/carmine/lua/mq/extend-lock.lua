-- Extend a lease only while its fencing token remains current.

-- Must precede this script's first write so that the `TIME` call below is
-- permitted on servers with `lua-replicate-commands no` (Redis 6 and older).
-- A no-op returning true on Redis >= 7, where effects replication is the
-- only mode. Don't remove.
redis.replicate_commands();

local t   = redis.call('TIME'); -- Server clock, avoids client clock skew
local now = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000); -- Epoch msecs

local token = redis.call('hget', _:qk-lock-tokens, _:mid);
local current_expiry = tonumber(redis.call('hget', _:qk-locks, _:mid));
if ((not token) or (token ~= _:lease-token) or (not current_expiry)) then
   return {'stale'};
end

local requested_expiry = now + tonumber(_:lock-ms);
local new_expiry = math.max(current_expiry, requested_expiry);
redis.call('hset', _:qk-locks, _:mid, new_expiry);
return {'extended', new_expiry};
