-- Sets message backoff expiry relative to server clock

-- Must precede this script's first write so that the `TIME` call below is
-- permitted on servers with `lua-replicate-commands no` (Redis 6 and older).
-- A no-op returning true on Redis >= 7, where effects replication is the
-- only mode. Don't remove.
redis.replicate_commands();

local t   = redis.call('TIME'); -- Server clock, avoids client clock skew
local now = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000); -- Epoch msecs
return redis.call('hset', _:qk-backoffs, _:mid, now + tonumber(_:backoff-ms));
