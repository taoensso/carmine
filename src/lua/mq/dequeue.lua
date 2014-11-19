if redis.call('exists', _:qk-eoq-backoff) == 1 then
   return 'eoq-backoff';
end

-- TODO Waiting for Lua brpoplpush support to get us long polling
local mid = redis.call('rpoplpush', _:qk-mid-circle, _:qk-mid-circle);
local now = tonumber(_:now);

if (not mid) or (mid == 'end-of-circle') then -- Uninit'd or eoq

   -- Calculate eoq_backoff_ms
   local ndry_runs = tonumber(redis.call('get', _:qk-ndry-runs) or 0);
   local eoq_ms_tab = {_:eoq-ms0, _:eoq-ms1, _:eoq-ms2, _:eoq-ms3, _:eoq-ms4};
   local eoq_backoff_ms = tonumber(eoq_ms_tab[math.min(5, (ndry_runs + 1))]);

   -- Set queue-wide polling backoff flag
   redis.call('psetex', _:qk-eoq-backoff, eoq_backoff_ms, 'true');
   redis.call('incr',   _:qk-ndry-runs);

   return 'eoq-backoff';
end

-- From msg_status.lua ---------------------------------------------------------
-- local mid      = _:mid;
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

if (state == 'locked') or
   (state == 'locked-with-requeue') or
   (state == 'queued-with-backoff') or
   (state == 'done-with-backoff') then
   return nil;
end

redis.call('set', _:qk-ndry-runs, 0); -- Doing useful work

if (state == 'done-awaiting-gc') then
   redis.call('hdel',  _:qk-messages,   mid);
   redis.call('hdel',  _:qk-locks,      mid);
   redis.call('hdel',  _:qk-backoffs,   mid);
   redis.call('hdel',  _:qk-nattempts,  mid);
   redis.call('ltrim', _:qk-mid-circle, 1, -1);
   redis.call('srem',  _:qk-done,       mid);
   return nil;
end

redis.call('hset', _:qk-locks, mid, now + tonumber(_:lock-ms)); -- Acquire
local mcontent  = redis.call('hget',    _:qk-messages,  mid);
local nattempts = redis.call('hincrby', _:qk-nattempts, mid, 1);
return {mid, mcontent, nattempts};
