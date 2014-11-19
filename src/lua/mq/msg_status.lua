-- Careful, logic here's subtle! See state diagram for assistance.

local mid         = _:mid;
local now         = tonumber(_:now);
local lock_exp    = tonumber(redis.call('hget', _:qk-locks,    mid)) or 0;
local backoff_exp = tonumber(redis.call('hget', _:qk-backoffs, mid)) or 0;
local state       = nil;

if redis.call('hexists', _:qk-messages, mid) == 1 then
   if redis.call('sismember', _:qk-done, mid) == 1 then
      if (now < backoff_exp) then
         if redis.call('sismember', _:qk-requeue, mid) == 1 then
            state = 'done-with-requeue';
         else
            state = 'done-with-backoff';
         end
      else
         state = 'done-awaiting-gc';
      end
   else
      if (now < lock_exp) then
         if redis.call('sismember', _:qk-requeue, mid) == 1 then
            state = 'locked-with-requeue';
         else
            state = 'locked';
         end
      elseif (now < backoff_exp) then
         state = 'queued-with-backoff';
      else
         state = 'queued';
      end
   end
end

return state;
