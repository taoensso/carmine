-- Return e/o {'sleep' ...}, {'skip' ...}, {'handle' ...}, {'unexpected' ...}

-- Must precede this script's first write so that the `TIME` call below is
-- permitted on servers with `lua-replicate-commands no` (Redis 6 and older).
-- A no-op returning true on Redis >= 7, where effects replication is the
-- only mode. Don't remove.
redis.replicate_commands();

local t   = redis.call('TIME'); -- Server clock, avoids client clock skew
local now = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000); -- Epoch msecs

-- Processes given mid and returns e/o {'skip' ...}, {'handle' ...}, {'unexpected' ...}
-- With `defer`, a circle mid that's ready for handling is instead moved to the
-- back of the ready list.
local function process(mid, mid_src, defer)

   -- From msg_status.lua ------------------------------------------------------
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
   -----------------------------------------------------------------------------

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

         redis.call('hdel',  _:qk-messages-rq,   mid);
         redis.call('hdel',  _:qk-lock-times-rq, mid);
         redis.call('hdel',  _:qk-nattempts,     mid);
         redis.call('hdel',  _:qk-lock-tokens,   mid);
         redis.call('srem',  _:qk-done,          mid);
         redis.call('srem',  _:qk-requeue,       mid);

         redis.call('lpush', _:qk-mids-ready,    mid); -- -> Priority queue (>once okay)

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
         redis.call('hdel',  _:qk-lock-tokens,   mid);
         redis.call('srem',  _:qk-done,          mid);
         redis.call('srem',  _:qk-requeue,       mid);

         if (mid_src == 'circle') then
         redis.call('ltrim', _:qk-mid-circle, 1, -1); -- Remove from circle
         end

         return {'skip', 'did-gc'};
      end
   elseif (status == 'queued') then
      if defer then
         -- {queued, -bo, _rq} -> wait in turn behind the mids already ready
         redis.call('ltrim', _:qk-mid-circle, 1, -1); -- Remove from circle
         redis.call('lpush', _:qk-mids-ready, mid);
         return {'skip', 'did-defer'};
      end

      -- {queued, -bo, _rq} -> handle now
      local lock_ms =
         tonumber(redis.call('hget', _:qk-lock-times, mid)) or
         tonumber(_:default-lock-ms);

      local lock_expiry = now + lock_ms;
                        redis.call('hset',    _:qk-locks,       mid, lock_expiry); -- Acquire
                        redis.call('hset',    _:qk-lock-tokens, mid, _:lease-token);
      local mcontent  = redis.call('hget',    _:qk-messages,  mid);
      local udt       = redis.call('hget',    _:qk-udts,      mid);
      local nattempts = redis.call('hincrby', _:qk-nattempts, mid, 1);

      return {'handle', mid, mcontent, nattempts, lock_ms, tonumber(udt)};
   end

   return {'unexpected'};
end

-- Process up to max_steps mids, stopping at first non-skip reply.
--
-- Mids from the ready list are prioritized, but only after first using a few
-- spare steps (up to one lap) for circle maintenance: GC, requeues, etc. This
-- way the circle keeps progressing even while new mids keep arriving.
--
-- For fairness, circle mids found ready for handling (retries, expired locks)
-- during these steps join the back of the ready list rather than jump ahead
-- of it. So neither can starve the other.
local max_steps = math.max(1, tonumber(_:max-steps) or 1);
local max_circle_steps = math.min(4, (max_steps - 1)); -- Before a ready mid
local neocs     = 0;     -- Num times reached end of circle while mids are ready
local lapped    = false; -- Completed a circle lap while mids are ready?
local reply     = nil;

for step = 1, max_steps do
   local mid     = nil;
   local mid_src = nil;
   local ready   = (redis.call('llen', _:qk-mids-ready) > 0);

   if (ready and (lapped or (step > max_circle_steps))) then
      mid = redis.call('rpoplpush', _:qk-mids-ready, _:qk-mid-circle);
      mid_src = 'ready';
   else
      mid = redis.call('rpoplpush', _:qk-mid-circle, _:qk-mid-circle);
      mid_src = 'circle';
   end

   if ((not mid) or (mid == 'end-of-circle')) then -- Uninit'd or eoq
      if ready then
         -- Don't sleep, continue with circle until back here, then ready mids
         neocs  = neocs + 1;
         lapped = ((not mid) or (neocs >= 2));
      else
         -- Calculate eoq_backoff_ms
         local ndry_runs = tonumber(redis.call('get', _:qk-ndry-runs)) or 0;
         local eoq_ms_tab = {_:eoq-bo1, _:eoq-bo2, _:eoq-bo3, _:eoq-bo4, _:eoq-bo5};
         local eoq_backoff_ms = tonumber(eoq_ms_tab[math.min(5, (ndry_runs + 1))]);
         redis.call('incr', _:qk-ndry-runs);

         -- Discard signals already accounted for by this atomic poll. Any later
         -- enqueue leaves one persistent sentinel on A or B for the blocking client.
         redis.call('del', _:qk-isleep-a, _:qk-isleep-b);

         return {'sleep', 'end-of-circle', 'a', eoq_backoff_ms};
      end
   else
      reply = process(mid, mid_src, (ready and (mid_src == 'circle')));
      if (reply[1] ~= 'skip') then return reply; end
   end
end

return reply;
