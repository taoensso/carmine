-- From msg_status.lua ---------------------------------------------------------
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
--------------------------------------------------------------------------------
-- Return {action, error}

local interrupt_sleep = function ()
   if     redis.call('rpoplpush', _:qk-isleep-a, _:qk-isleep-b) then
   elseif redis.call('rpoplpush', _:qk-isleep-b, _:qk-isleep-a) then
   else   redis.call('lpush',     _:qk-isleep-a, '_'); end -- Init
end

local reset_in_queue = function()
   redis.call('hset',   _:qk-messages, _:mid, _:mcnt);
   redis.call('hsetnx', _:qk-udts,     _:mid, now);

   local lock_ms = tonumber(_:lock-ms);
   if   (lock_ms ~= -1) then
      redis.call('hset', _:qk-lock-times, _:mid, lock_ms);
   else
      redis.call('hdel', _:qk-lock-times, _:mid);
   end
end

local reset_in_requeue = function()
   redis.call('hset', _:qk-messages-rq, _:mid, _:mcnt);

   local lock_ms = tonumber(_:lock-ms);
   if   (lock_ms ~= -1) then
      redis.call('hset', _:qk-lock-times-rq, _:mid, lock_ms);
   else
      redis.call('hdel', _:qk-lock-times-rq, _:mid);
   end
end

local can_upd = (_:can-upd? == '1');
local can_rq  = (_:can-rq?  == '1');

local ensure_update_in_requeue = function()
   if is_rq then
      if can_upd then
	 reset_in_requeue();
	 return {'updated'};
      else
	 return {false, 'already-queued'};
      end
   else
      reset_in_requeue();
      return {'added'};
   end
end

if (status == 'nx') then
   -- {nil, _bo, _rq} -> add to queue

    -- Ensure that mid-circle is initialized
   if redis.call('exists', _:qk-mid-circle) ~= 1 then
      redis.call('lpush',  _:qk-mid-circle, 'end-of-circle');
   end

   -- Set the initial backoff if requested
   local init_bo = tonumber(_:init-bo);
   if (init_bo ~= 0) then
      redis.call('hset',  _:qk-backoffs, _:mid, now + init_bo);
      redis.call('lpush', _:qk-mid-circle, _:mid); -- -> Maintenance queue
   else
      redis.call('lpush', _:qk-mids-ready, _:mid); -- -> Priority queue
   end

   reset_in_queue();
   interrupt_sleep();
   return {'added'};

elseif (status == 'queued') then
   if can_upd then
      -- {queued, *bo, _rq} -> update in queue
      reset_in_queue();
      return {'updated'};
   else
      return {false, 'already-queued'};
   end
elseif (status == 'locked') then
   if can_rq then
      -- {locked, _bo, *rq} -> ensure/update in requeue
      return ensure_update_in_requeue();
   else
      return {false, 'locked'};
   end
elseif (status == 'done') then
   if is_bo then
      if can_rq then
	 -- {done, +bo, *rq} -> ensure/update in requeue
	 return ensure_update_in_requeue();
      else
	 return {false, 'backoff'};
      end
   else
      -- {done, -bo, *rq} -> ensure/update in requeue
      -- (We're appropriating the requeue mechanism here)

      redis.call('lpush', _:qk-mids-ready, _:mid); -- -> Priority queue

      interrupt_sleep();
      return ensure_update_in_requeue();
   end
end

return {false, 'unexpected'};
