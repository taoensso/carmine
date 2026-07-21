local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000);
local maintenance_limit = math.max(1, tonumber(_:maintenance-limit));

-- Timed-role cleanup is otherwise invisible when this same round ultimately
-- returns idle or claims unrelated healthy work. Keep exact, bounded tallies
-- on every reply, including errors after an earlier cleanup already committed.
local cleanup_orphan_count = 0;
local cleanup_corrupt_meta_count = 0;
local cleanup_corrupt_payload_count = 0;
local cleanup_corrupt_index_count = 0;
local cleanup_counts = function()
  return {
    tostring(cleanup_orphan_count),
    tostring(cleanup_corrupt_meta_count),
    tostring(cleanup_corrupt_payload_count),
    tostring(cleanup_corrupt_index_count)
  };
end
local tally_cleanup = function(reason)
  if reason == 'orphan' then
    cleanup_orphan_count = cleanup_orphan_count + 1;
  elseif reason == 'corrupt-meta' then
    cleanup_corrupt_meta_count = cleanup_corrupt_meta_count + 1;
  elseif reason == 'corrupt-payload' then
    cleanup_corrupt_payload_count = cleanup_corrupt_payload_count + 1;
  elseif reason == 'corrupt-index' then
    cleanup_corrupt_index_count = cleanup_corrupt_index_count + 1;
  end
end

if redis.call('exists', _:qk-config) ~= 1 then
  return {'error', 'uninitialized', cleanup_counts()};
end

local retry_base_ms = tonumber(redis.call('hget', _:qk-config, 'retry_base_ms'));
local retry_max_ms  = tonumber(redis.call('hget', _:qk-config, 'retry_max_ms'));
local lease_ms      = tonumber(redis.call('hget', _:qk-config, 'lease_ms'));
local retry_jitter  = redis.call('hget', _:qk-config, 'retry_jitter');

-- Same policy as settle.lua's handler-requested retries, incl. full jitter:
-- expired leases often expire in batches (e.g. worker crash), exactly when
-- jitter matters most. Seed is deterministic (no math.random) so replicas
-- and script replays agree.
local retry_delay = function(attempt, jitter_seed)
  local delay = math.min(retry_max_ms,
    retry_base_ms * (2 ^ math.min(62, math.max(0, attempt - 1))));
  if delay > 0 and retry_jitter == 'full' then
    local digest = redis.sha1hex(jitter_seed .. ':' .. tostring(attempt));
    local sample = tonumber(string.sub(digest, 1, 13), 16);
    delay = math.floor((sample / 4503599627370496) * (delay + 1));
  end
  return delay;
end

local cleanup_orphan = function(mid)
  local _, successor_seq, seq_error =
    mq_prepare_successor_promotion(mid, now);
  if seq_error then return false, seq_error; end
  mq_cleanup_active(mid);
  mq_promote_successor(mid, now, successor_seq);
  return true, nil;
end

-- Reap a bounded number of expired leases. Attempts count claims, so an
-- exhausted expired claim follows its per-message terminal policy.
local expired = redis.call('zrangebyscore', _:qk-leased, '-inf', now,
  'limit', 0, maintenance_limit);
for _, mid in ipairs(expired) do
  local packed = redis.call('hget', _:qk-meta, mid);
  local payload = redis.call('hget', _:qk-payloads, mid);
  -- The expired random lease token is also the deterministic jitter seed. A
  -- missing token already fences the old handler, so use the MID as a stable
  -- fallback instead of deleting otherwise complete, valid active work.
  local old_token = redis.call('hget', _:qk-lease-tokens, mid) or mid;
  local meta = mq_unpack_active(packed);
  local cleanup_reason = nil;
  if (not packed) or (not payload) then
    cleanup_reason = 'orphan';
  elseif not mq_validate_payload_envelope(payload) then
    cleanup_reason = 'corrupt-payload';
  elseif (not meta) or meta[3] < 1 then
    cleanup_reason = 'corrupt-meta';
  end
  if cleanup_reason then
    local _, seq_error = cleanup_orphan(mid);
    if seq_error then return {'error', seq_error, cleanup_counts()}; end
    tally_cleanup(cleanup_reason);
  else
    if meta[3] >= meta[4] then
      local _, successor_seq, seq_error =
        mq_prepare_successor_promotion(mid, now);
      if seq_error then return {'error', seq_error, cleanup_counts()}; end
      mq_exhaust(mid, meta, payload, 'lease-expired', now);
      mq_promote_successor(mid, now, successor_seq);
    else
      local delay_ms = retry_delay(meta[3], old_token);
      local ready_seq = nil;
      if delay_ms == 0 then
        local seq_error = nil;
        ready_seq, seq_error = mq_reserve_ready_seq();
        if not ready_seq then
          return {'error', seq_error, cleanup_counts()};
        end
      end
      -- Expiry maintenance has selected this fenced lease as authoritative.
      -- Remove any stray producer indexes before applying its retry policy, so
      -- they cannot bypass a positive backoff or create a second ready role.
      local stray_index_count =
        redis.call('zrem', _:qk-ready-high, mid) +
        redis.call('zrem', _:qk-ready-normal, mid) +
        redis.call('zrem', _:qk-ready-low, mid) +
        redis.call('zrem', _:qk-scheduled, mid);
      if stray_index_count > 0 then tally_cleanup('corrupt-index'); end
      redis.call('zrem', _:qk-leased, mid);
      redis.call('hdel', _:qk-lease-tokens, mid);
      if delay_ms == 0 then
        mq_ready(mid, meta[2], ready_seq);
      else
        redis.call('zadd', _:qk-scheduled, now + delay_ms, mid);
        -- Existing blockers cannot see this newly-created retry deadline until
        -- one of them recomputes queue state. Leave a coalesced advisory baton
        -- even when the current caller dies immediately after this script.
        mq_signal();
      end
    end
  end
end

-- Promote a bounded number of due scheduled messages.
local due = redis.call('zrangebyscore', _:qk-scheduled, '-inf', now,
  'limit', 0, maintenance_limit);
for _, mid in ipairs(due) do
  -- A lease index or token may belong to an in-flight handler. Preserve that
  -- role and remove only the stray scheduled occurrence; destructive generic
  -- containment could otherwise erase live application work.
  if redis.call('zscore', _:qk-leased, mid) or
     redis.call('hexists', _:qk-lease-tokens, mid) == 1 then
    redis.call('zrem', _:qk-scheduled, mid);
    tally_cleanup('corrupt-index');
  else
    local packed = redis.call('hget', _:qk-meta, mid);
    local payload = redis.call('hget', _:qk-payloads, mid);
    local meta = mq_unpack_active(packed);
    local cleanup_reason = nil;
    if (not packed) or (not payload) then
      cleanup_reason = 'orphan';
    elseif not mq_validate_payload_envelope(payload) then
      cleanup_reason = 'corrupt-payload';
    elseif not meta then
      cleanup_reason = 'corrupt-meta';
    end
    if not cleanup_reason then
      local ready_seq, seq_error = mq_reserve_ready_seq();
      if not ready_seq then
        return {'error', seq_error, cleanup_counts()};
      end
      redis.call('zrem', _:qk-scheduled, mid);
      mq_ready(mid, meta[2], ready_seq);
    else
      local _, seq_error = cleanup_orphan(mid);
      if seq_error then return {'error', seq_error, cleanup_counts()}; end
      tally_cleanup(cleanup_reason);
    end
  end
end

local ready_keys = {_:qk-ready-high, _:qk-ready-normal, _:qk-ready-low};
local mid = nil;
local selected_ready_key = nil;
for _, ready_key in ipairs(ready_keys) do
  if not mid then
    local head = redis.call('zrange', ready_key, 0, 0);
    if head[1] then
      mid = head[1];
      selected_ready_key = ready_key;
    end
  end
end

if mid then
  -- A lease index or token can belong to a handler that is still running even
  -- when out-of-band damage left a duplicate ready occurrence. Remove only the
  -- selected ready member and preserve the possibly authoritative in-flight
  -- role. A later claim can similarly drain another stray ready band.
  if redis.call('zscore', _:qk-leased, mid) or
     redis.call('hexists', _:qk-lease-tokens, mid) == 1 then
    redis.call('zrem', selected_ready_key, mid);
    return {'skip', 'corrupt-index', cleanup_counts()};
  end
  local packed = redis.call('hget', _:qk-meta, mid);
  local payload = redis.call('hget', _:qk-payloads, mid);
  if (not packed) or (not payload) then
    local _, seq_error = cleanup_orphan(mid);
    if seq_error then return {'error', seq_error, cleanup_counts()}; end
    return {'skip', 'orphan', cleanup_counts()};
  end
  if not mq_validate_payload_envelope(payload) then
    local _, seq_error = cleanup_orphan(mid);
    if seq_error then return {'error', seq_error, cleanup_counts()}; end
    return {'skip', 'corrupt-payload', cleanup_counts()};
  end

  local meta = mq_unpack_active(packed);
  if not meta then
    local _, seq_error = cleanup_orphan(mid);
    if seq_error then return {'error', seq_error, cleanup_counts()}; end
    return {'skip', 'corrupt-meta', cleanup_counts()};
  end
  if meta[3] >= meta[4] then
    -- A ready record at its attempt limit is inconsistent. Apply its validated
    -- terminal policy without creating an out-of-range next attempt.
    local _, successor_seq, seq_error =
      mq_prepare_successor_promotion(mid, now);
    if seq_error then return {'error', seq_error, cleanup_counts()}; end
    mq_exhaust(mid, meta, payload, 'attempt-limit', now);
    mq_promote_successor(mid, now, successor_seq);
    return {'skip', 'corrupt-meta', cleanup_counts()};
  end
  -- Consume a possibly stale advisory token only after validating the claim.
  -- Always recreate one below: besides continuing ready bursts, the baton
  -- makes an already-blocked peer recompute the lease deadline just created.
  mq_consume_signal();
  redis.call('zrem', selected_ready_key, mid);
  meta[3] = meta[3] + 1;
  -- A validated per-message override (meta[8]) replaces the queue default for
  -- this grant; zero is excluded by validation, so Lua truthiness is safe.
  local effective_lease_ms = meta[8] or lease_ms;
  local lease_expiry = now + effective_lease_ms;
  -- Densify legacy 7-field records on rewrite so stored metas converge to
  -- the current 8-field form.
  meta[8] = meta[8] or false;
  redis.call('hset', _:qk-meta, mid, cmsgpack.pack(meta));
  redis.call('hset', _:qk-lease-tokens, mid, _:lease-token);
  redis.call('zadd', _:qk-leased, lease_expiry, mid);
  mq_signal();
  return {'handle', mid, payload, tostring(meta[3]), _:lease-token,
    tostring(lease_expiry), tostring(meta[5]), tostring(meta[2]), tostring(now),
    cleanup_counts()};
end

local next_at = nil;
local next_scheduled = redis.call('zrange', _:qk-scheduled, 0, 0, 'withscores');
local next_leased = redis.call('zrange', _:qk-leased, 0, 0, 'withscores');
-- Corrupt future heads remain available to diagnostics, but must not escape
-- as fractional/infinite JVM sleep deadlines. Every already-due head maps to
-- now so bounded maintenance keeps advancing through negative/-inf damage.
-- A future in-domain fractional head wakes at its ceiling: normal due
-- maintenance then consumes/repairs it, so it cannot mask later healthy work
-- in the same timed role.
local wake_deadline = function(raw_score)
  if not raw_score then return nil; end
  local score = tonumber(raw_score);
  if type(score) ~= 'number' or score ~= score or score > mq_max_timestamp then
    return nil;
  end
  if score <= now then return now; end
  return math.ceil(score);
end
local scheduled_at = wake_deadline(next_scheduled[2]);
local leased_at = wake_deadline(next_leased[2]);
if scheduled_at then next_at = scheduled_at; end
if leased_at and ((not next_at) or leased_at < next_at) then
  next_at = leased_at;
end
-- Do not create or consume a healthy baton on an idle round, but remove an
-- out-of-band wrongtype so the JVM's advisory BLPOP can resume normally.
mq_repair_signal_type();
return {'idle', next_at and tostring(next_at) or false, tostring(now), cleanup_counts()};
