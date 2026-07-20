-- Atomically finalize a handler result only while its lease is current.
-- The token is a fresh random value per dequeue and is cleared whenever a
-- lease is superseded (re-dequeue, producer update, clear), so token
-- equality alone proves ownership. NB an expiry-equality check would
-- wrongly fence an owner whose own `extend-lock` reply was lost/racing.

-- Must precede this script's first write so that the `TIME` call below is
-- permitted on servers with `lua-replicate-commands no` (Redis 6 and older).
-- A no-op returning true on Redis >= 7, where effects replication is the
-- only mode. Don't remove.
redis.replicate_commands();

local t   = redis.call('TIME'); -- Server clock, avoids client clock skew
local now = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000); -- Epoch msecs

local token = redis.call('hget', _:qk-lock-tokens, _:mid);
local lock_expiry = redis.call('hget', _:qk-locks, _:mid);
if ((not token) or (token ~= _:lease-token) or (not lock_expiry)) then
   return {'stale'};
end

local backoff_ms = tonumber(_:backoff-ms);
if (backoff_ms >= 0) then
   redis.call('hset', _:qk-backoffs, _:mid, now + backoff_ms);
end

if (_:done? == '1') then
   redis.call('sadd', _:qk-done, _:mid);
end

redis.call('hdel', _:qk-locks,       _:mid);
redis.call('hdel', _:qk-lock-tokens, _:mid);
return {'finalized'};
