-- Atomically cancel any pending handling and/or requeue of given mid.
-- Returns {status, is_bo, is_rq, cancelled} with the mid's prior status.

-- Must precede this script's first write so that the `TIME` call below is
-- permitted on servers with `lua-replicate-commands no` (Redis 6 and older).
-- A no-op returning true on Redis >= 7, where effects replication is the
-- only mode. Don't remove.
redis.replicate_commands();

local mid = _:mid;
local t   = redis.call('TIME'); -- Server clock, avoids client clock skew
local now = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000); -- Epoch msecs

--[[include:msg-status-fn]]
local status, is_bo, is_rq = msg_status(mid, now);

local cancelled = false;

if is_rq then
   -- {done, *bo, +rq}, {locked, _bo, +rq} -> cancel requeue
   redis.call('hdel', _:qk-messages-rq,   mid);
   redis.call('hdel', _:qk-lock-times-rq, mid);
   redis.call('srem', _:qk-requeue,       mid);
   cancelled = true;
end

if ((status == 'queued') or (status == 'locked')) then
   -- {queued, *bo, _rq}, {locked, _bo, *rq} -> mark as done for GC.
   -- Clearing the lock + fencing token means that a handler currently (or
   -- previously) holding the mid can no longer finalize or extend, so can't
   -- cause a retry. Any dedupe backoff of an mid already done is left as is.
   redis.call('sadd', _:qk-done,        mid);
   redis.call('hdel', _:qk-locks,       mid);
   redis.call('hdel', _:qk-lock-tokens, mid);
   redis.call('hdel', _:qk-backoffs,    mid);
   cancelled = true;
end

return {status, is_bo, is_rq, cancelled};
