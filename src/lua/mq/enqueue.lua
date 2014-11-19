-- From msg_status.lua ---------------------------------------------------------
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

-- return state;
--------------------------------------------------------------------------------

if (state == 'done-awaiting-gc') or
   ((state == 'done-with-backoff') and (_:allow-requeue? == 'true'))
then
   redis.call('hdel', _:qk-nattempts, _:mid);
   redis.call('srem', _:qk-done,      _:mid);
   return {_:mid};
end

if (state == 'locked') and (_:allow-requeue? == 'true') and
   (redis.call('sismember', _:qk-requeue, _:mid) ~= 1)
then
   redis.call('sadd', _:qk-requeue, _:mid);
   return {_:mid};
end

if state == nil then
   redis.call('hset', _:qk-messages, _:mid, _:mcontent);

   -- lpushnx end-of-circle marker to ensure an initialized mid-circle
   if redis.call('exists', _:qk-mid-circle) ~= 1 then
      redis.call('lpush', _:qk-mid-circle, 'end-of-circle');
   end

   redis.call('lpush', _:qk-mid-circle, _:mid);
   return {_:mid};
else
   return state; -- Reject
end
