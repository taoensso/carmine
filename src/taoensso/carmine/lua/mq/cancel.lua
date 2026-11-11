-- Atomically cancel any pending handling and/or requeue of given mid.
-- Returns {status, is_bo, is_rq, cancelled} with the mid's prior status.

-- Must precede this script's first write so that the `TIME` call below is
-- permitted on servers with `lua-replicate-commands no` (Redis 6 and older).
-- A no-op returning true on Redis >= 7, where effects replication is the
-- only mode. Don't remove.
redis.replicate_commands();

-- From msg_status.lua ---------------------------------------------------------
local mid = _:mid;
local t   = redis.call('TIME'); -- Server clock, avoids client clock skew
local now = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000); -- Epoch msecs

local status  = nil; -- base status e/o nil, done, queued, locked
local is_bo = false; -- backoff flag for: done, queued
local is_rq = false; -- requeue flag for: done, locked
-- 8x cases: nil, done(bo/rq), queued(bo), locked(rq)
-- Describe with: {status, $bo, $rq} with $ prefixes e/o: _, +, -, *

if (redis.call('hexists', _:qk-messages, mid) == 1) then
   local exp_lock = tonumber(redis.call('hget', _:qk-locks,    mid)) or 0;
   local exp_bo   = tonumber(redis.call('hget', _:qk-backoffs, mid)) or 0;

   is_bo = (now < exp_bo);
   is_rq = (redis.call('sismember', _:qk-requeue,     mid) == 1) or -- Deprecated
           (redis.call('hexists',   _:qk-messages-rq, mid) == 1);

   if (redis.call('sismember', _:qk-done, mid) == 1) then status = 'done';
   elseif (now < exp_lock)                           then status = 'locked';
   else                                                   status = 'queued'; end
else
   status = 'nx';
end
--------------------------------------------------------------------------------

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
