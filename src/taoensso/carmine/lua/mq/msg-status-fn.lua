-- Shared by the scripts that need an mid's status, spliced in at load time.
-- Careful! Logic here is subtle, see mq-architecture.svg for assistance.
--
-- Returns status, is_bo, is_rq, exp_lock, exp_bo with:
--   status - base status e/o 'nx', 'done', 'queued', 'locked'
--   is_bo  - backoff flag for: done, queued
--   is_rq  - requeue flag for: done, locked
--
-- 8x cases: nx, done(bo/rq), queued(bo), locked(rq)
-- Describe with: {status, $bo, $rq} with $ prefixes e/o: _, +, -, *
local function msg_status(mid, now)
   if (redis.call('hexists', _:qk-messages, mid) == 0) then
      return 'nx', false, false, 0, 0;
   end

   local exp_lock = tonumber(redis.call('hget', _:qk-locks,    mid)) or 0;
   local exp_bo   = tonumber(redis.call('hget', _:qk-backoffs, mid)) or 0;

   local is_bo = (now < exp_bo);
   local is_rq = (redis.call('sismember', _:qk-requeue,     mid) == 1) or -- Deprecated
                 (redis.call('hexists',   _:qk-messages-rq, mid) == 1);

   local status;
   if (redis.call('sismember', _:qk-done, mid) == 1) then status = 'done';
   elseif (now < exp_lock)                           then status = 'locked';
   else                                                   status = 'queued'; end

   return status, is_bo, is_rq, exp_lock, exp_bo;
end
