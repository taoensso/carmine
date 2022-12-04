-- Careful! Logic here is subtle, see mq-diagram.svg for assistance.
local mid = _:mid;
local now = tonumber(_:now);

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

return {status, is_bo, is_rq};
