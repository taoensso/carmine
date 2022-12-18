-- Return e/o {'sleep' ...}, {'skip' ...}, {'handle' ...}, {'unexpected' ...}

-- Prioritize mids from ready list
local mid_src = nil;
local mid     = nil;
mid = redis.call('rpoplpush', _:qk-mids-ready, _:qk-mid-circle);
if mid then
   mid_src = 'ready';
else
   mid = redis.call('rpoplpush', _:qk-mid-circle, _:qk-mid-circle);
   if mid then
      mid_src = 'circle';
   end
end

if ((not mid) or (mid == 'end-of-circle')) then -- Uninit'd or eoq

   -- Calculate eoq_backoff_ms
   local ndry_runs = tonumber(redis.call('get', _:qk-ndry-runs)) or 0;
   local eoq_ms_tab = {_:eoq-bo1, _:eoq-bo2, _:eoq-bo3, _:eoq-bo4, _:eoq-bo5};
   local eoq_backoff_ms = tonumber(eoq_ms_tab[math.min(5, (ndry_runs + 1))]);
   redis.call('incr', _:qk-ndry-runs);

   local isleep_on = nil;
   if (redis.call('llen', _:qk-isleep-b) > 0) then isleep_on = 'b'; else isleep_on = 'a'; end

   return {'sleep', 'end-of-circle', isleep_on, eoq_backoff_ms};
end

-- From msg_status.lua ---------------------------------------------------------
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

if (status == 'nx') then
   if (mid_src == 'circle') then
      redis.call('ltrim', _:qk-mid-circle, 1, -1); -- Remove from circle
      return {'skip', 'did-trim'};
   else
      return {'skip', 'unexpected'};
   end
elseif  (status == 'locked')            then return {'skip', 'locked'};
elseif ((status == 'queued') and is_bo) then return {'skip', 'queued-with-backoff'};
elseif ((status == 'done')   and is_bo) then return {'skip', 'done-with-backoff'};
end

redis.call('set', _:qk-ndry-runs, 0); -- Doing useful work

if (status == 'done') then
   if is_rq then
      -- {done, -bo, +rq} -> requeue now
      local mcontent =
	 redis.call('hget', _:qk-messages-rq, mid) or
	 redis.call('hget', _:qk-messages,    mid); -- Deprecated (for qk-requeue)

      local lock_ms =
	 tonumber(redis.call('hget', _:qk-lock-times-rq, mid)) or
	 tonumber(_:default-lock-ms);

      redis.call('hset', _:qk-messages, mid, mcontent);
      redis.call('hset', _:qk-udts,     mid, now);

      if lock_ms then
	 redis.call('hset', _:qk-lock-times, mid, lock_ms);
      else
	 redis.call('hdel', _:qk-lock-times, mid);
      end

      redis.call('hdel', _:qk-messages-rq,   mid);
      redis.call('hdel', _:qk-lock-times-rq, mid);
      redis.call('hdel', _:qk-nattempts,     mid);
      redis.call('srem', _:qk-done,          mid);
      redis.call('srem', _:qk-requeue,       mid);
      return {'skip', 'did-requeue'};
   else
      -- {done, -bo, -rq} -> full GC now
      redis.call('hdel',  _:qk-messages,      mid);
      redis.call('hdel',  _:qk-messages-rq,   mid);
      redis.call('hdel',  _:qk-lock-times,    mid);
      redis.call('hdel',  _:qk-lock-times-rq, mid);
      redis.call('hdel',  _:qk-udts,          mid);
      redis.call('hdel',  _:qk-locks,         mid);
      redis.call('hdel',  _:qk-backoffs,      mid);
      redis.call('hdel',  _:qk-nattempts,     mid);
      redis.call('srem',  _:qk-done,          mid);
      redis.call('srem',  _:qk-requeue,       mid);

      if (mid_src == 'circle') then
	 redis.call('ltrim', _:qk-mid-circle, 1, -1); -- Remove from circle
      end

      return {'skip', 'did-gc'};
   end
elseif (status == 'queued') then
   -- {queued, -bo, _rq} -> handle now
   local lock_ms =
      tonumber(redis.call('hget', _:qk-lock-times, mid)) or
      tonumber(_:default-lock-ms);

                     redis.call('hset',    _:qk-locks,     mid, now + lock_ms); -- Acquire
   local mcontent  = redis.call('hget',    _:qk-messages,  mid);
   local udt       = redis.call('hget',    _:qk-udts,      mid);
   local nattempts = redis.call('hincrby', _:qk-nattempts, mid, 1);

   return {'handle', mid, mcontent, nattempts, lock_ms, tonumber(udt)};
end

return {'unexpected'};
