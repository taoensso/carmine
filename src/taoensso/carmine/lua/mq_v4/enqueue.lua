local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000);
local mid = _:mid;

if redis.call('exists', _:qk-config) ~= 1 then
  return {'error', 'uninitialized'};
end
if not mq_validate_payload_envelope(_:payload) then
  return {'error', 'invalid-payload'};
end

local ready_key = function(priority)
  if priority == 0 then return _:qk-ready-high;
  elseif priority == 2 then return _:qk-ready-low;
  else return _:qk-ready-normal; end
end

local remove_active_indexes = function()
  redis.call('zrem', _:qk-ready-high, mid);
  redis.call('zrem', _:qk-ready-normal, mid);
  redis.call('zrem', _:qk-ready-low, mid);
  redis.call('zrem', _:qk-scheduled, mid);
end

local place_active = function(priority, available_at, ready_seq)
  remove_active_indexes();
  if available_at > now then
    redis.call('zadd', _:qk-scheduled, available_at, mid);
  else
    redis.call('zadd', ready_key(priority), ready_seq, mid);
  end
  mq_signal();
end

local active_payload = redis.call('hget', _:qk-payloads, mid);
local active_packed = redis.call('hget', _:qk-meta, mid);
local dead_score = redis.call('zscore', _:qk-dead, mid);
local dead_payload = redis.call('hget', _:qk-dead-payloads, mid);
local failure_packed = redis.call('hget', _:qk-failures, mid);
local dead_parts = (dead_score and 1 or 0) + (dead_payload and 1 or 0) +
  (failure_packed and 1 or 0);
if dead_parts ~= 0 and (dead_parts ~= 3 or
    not mq_validate_payload_envelope(dead_payload) or
    not mq_unpack_failure(failure_packed, dead_score)) then
  return {'error', 'corrupt-dead'};
end
local prior_dead = dead_parts == 3;
local duplicate_policy = _:on-duplicate;
local revision_mode = redis.call('hget', _:qk-config, 'revision_mode') or 'none';
-- NB `X and false or Y` always yields Y in Lua; a plain `or` correctly
-- yields `false` (not nil) when unset, keeping packed metas dense arrays.
local revision = tonumber(_:revision) or false;
-- Generation semantics: every write below records this call's own override
-- (false when unset), never an inherited one. Each coalescing enqueue fully
-- re-describes its message; omitting `:lease-ms` means the queue default.
local lease_ms_override = false;
if _:lease-set == '1' then lease_ms_override = tonumber(_:lease-ms) or false; end

if active_payload or active_packed then
  if (not active_payload) or (not active_packed) then return {'error', 'corrupt-active'}; end
  if not mq_validate_payload_envelope(active_payload) then return {'error', 'corrupt-active'}; end
  local meta = mq_unpack_active(active_packed);
  if not meta then
    return {'error', 'corrupt-meta'};
  end

  -- Duplicate handling must not report success for active work that workers
  -- cannot safely claim. Validate the complete index/token role before every
  -- no-write return or coalescing mutation.
  local ready_high_score = redis.call('zscore', _:qk-ready-high, mid);
  local ready_normal_score = redis.call('zscore', _:qk-ready-normal, mid);
  local ready_low_score = redis.call('zscore', _:qk-ready-low, mid);
  local scheduled_score = redis.call('zscore', _:qk-scheduled, mid);
  local leased_score = redis.call('zscore', _:qk-leased, mid);
  local lease_token = redis.call('hget', _:qk-lease-tokens, mid);
  local active_index_count =
    (ready_high_score and 1 or 0) + (ready_normal_score and 1 or 0) +
    (ready_low_score and 1 or 0) + (scheduled_score and 1 or 0) +
    (leased_score and 1 or 0);
  if active_index_count ~= 1 or
     ((leased_score and not lease_token) or
      ((not leased_score) and lease_token)) then
    return {'error', 'corrupt-index'};
  end
  if leased_score then
    if not mq_integer_between(tonumber(leased_score), 0, mq_max_timestamp) or
       meta[3] < 1 then
      return {'error', 'corrupt-index'};
    end
  elseif scheduled_score then
    if not mq_integer_between(tonumber(scheduled_score), 0, mq_max_timestamp) or
       meta[3] >= meta[4] then
      return {'error', 'corrupt-index'};
    end
  else
    local ready_score = ready_high_score or ready_normal_score or ready_low_score;
    local ready_priority = ready_high_score and 0 or
      (ready_normal_score and 1 or 2);
    if not mq_integer_between(tonumber(ready_score), 1, mq_max_revision) or
       ready_priority ~= meta[2] or meta[3] >= meta[4] then
      return {'error', 'corrupt-index'};
    end
  end

  local successor_payload = redis.call('hget', _:qk-successor-payloads, mid);
  local successor_packed = redis.call('hget', _:qk-successor-meta, mid);
  local successor_meta = nil;
  if successor_payload or successor_packed then
    if (not successor_payload) or (not successor_packed) then
      return {'error', 'corrupt-successor'};
    end
    if not mq_validate_payload_envelope(successor_payload) then
      return {'error', 'corrupt-successor'};
    end
    successor_meta = mq_unpack_successor(successor_packed);
    if not successor_meta then return {'error', 'corrupt-successor'}; end
  end
  if successor_meta and meta[3] == 0 then
    -- An unattempted active generation must be authoritative. Refuse to erase
    -- or ignore an impossible newer role until an operator repairs it.
    return {'error', 'corrupt-successor'};
  end

  if duplicate_policy == 'reject' then
    if active_payload == _:payload then return {'existing'};
    else return {'conflict'}; end
  end

  local attempt = meta[3];
  local target_payload = active_payload;
  local target_meta = meta;
  local successor = false;
  local successor_record = false;
  if attempt > 0 then
    successor = true;
    target_payload = successor_payload;
    if target_payload then
      target_meta = successor_meta;
      successor_record = true;
    else
      target_payload = nil;
      target_meta = {1, meta[2], meta[4], now,
        meta[6] or redis.call('hget', _:qk-config, 'on_exhaustion_default'),
        meta[7] or false, now};
    end
  end

  -- Do not use Lua's `successor and X or Y` idiom here: legacy metadata may
  -- intentionally represent a missing revision as false, which would then
  -- fall through to successor available_at (target_meta[7]).
  local current_revision = nil;
  if successor then current_revision = target_meta[6];
  else current_revision = target_meta[7]; end
  if revision_mode == 'required' then
    current_revision = tonumber(current_revision);
    if current_revision and revision < current_revision then return {'stale-revision'}; end
    if current_revision and revision == current_revision then
      -- Preserve enqueue idempotency after the active generation was claimed:
      -- with no successor record, the equal-revision comparison target is the
      -- active payload (target_payload is nil in the synthesized case).
      local compare_payload = target_payload;
      if successor and (not successor_record) then compare_payload = active_payload; end
      if compare_payload == _:payload then return {'existing'};
      else return {'revision-conflict'}; end
    end
  elseif target_payload == _:payload and
      _:priority-set == '0' and _:delay-set == '0' and
      _:max-attempts-set == '0' and _:on-exhaustion-set == '0' and
      _:lease-set == '0' then
    return {'existing'};
  end

  if successor then
    local priority = (_:priority-set == '1') and tonumber(_:priority) or target_meta[2];
    local max_attempts = (_:max-attempts-set == '1') and tonumber(_:max-attempts) or target_meta[3];
    local on_exhaustion = (_:on-exhaustion-set == '1') and _:on-exhaustion or
      (target_meta[5] or redis.call('hget', _:qk-config, 'on_exhaustion_default'));
    local available_at = (_:delay-set == '1') and (now + tonumber(_:delay-ms)) or
      (target_meta[7] or now);
    local successor_meta = {1, priority, max_attempts, now, on_exhaustion,
      revision, available_at, lease_ms_override};
    redis.call('hset', _:qk-successor-payloads, mid, _:payload);
    redis.call('hset', _:qk-successor-meta, mid, cmsgpack.pack(successor_meta));
    return {'coalesced-successor', tostring(now), tostring(available_at), prior_dead and '1' or '0'};
  else
    local priority = (_:priority-set == '1') and tonumber(_:priority) or meta[2];
    local max_attempts = (_:max-attempts-set == '1') and tonumber(_:max-attempts) or meta[4];
    local on_exhaustion = (_:on-exhaustion-set == '1') and _:on-exhaustion or
      (meta[6] or redis.call('hget', _:qk-config, 'on_exhaustion_default'));
    local available_at = nil;
    if _:delay-set == '1' then
      available_at = now + tonumber(_:delay-ms);
    else
      local scheduled_score = redis.call('zscore', _:qk-scheduled, mid);
      if scheduled_score then
        available_at = mq_integer_between(tonumber(scheduled_score), 0, mq_max_timestamp);
        if not available_at then return {'error', 'corrupt-index'}; end
      end
    end
    local ready_seq = nil;
    if ((_:delay-set == '1') or (_:priority-set == '1')) and
       (available_at or now) <= now then
      local seq_error = nil;
      ready_seq, seq_error = mq_reserve_ready_seq();
      if not ready_seq then return {'error', seq_error}; end
    end
    meta = {1, priority, 0, max_attempts, now, on_exhaustion, revision,
      lease_ms_override};
    redis.call('hset', _:qk-payloads, mid, _:payload);
    redis.call('hset', _:qk-meta, mid, cmsgpack.pack(meta));
    -- Defensive invariant repair: attempt-0 active work is authoritative and
    -- must never coexist with an older successor.
    redis.call('hdel', _:qk-successor-payloads, mid);
    redis.call('hdel', _:qk-successor-meta, mid);
    if (_:delay-set == '1') or (_:priority-set == '1') then
      place_active(priority, available_at or now, ready_seq);
    else
      mq_signal();
    end
    return {'coalesced', tostring(now), tostring(available_at or now), prior_dead and '1' or '0'};
  end
end

local priority = tonumber(_:priority);
local max_attempts = tonumber(_:max-attempts);
local delay_ms = math.max(0, tonumber(_:delay-ms));
local available_at = now + delay_ms;
local meta = {1, priority, 0, max_attempts, now, _:on-exhaustion, revision,
  lease_ms_override};
local ready_seq = nil;
if available_at <= now then
  local seq_error = nil;
  ready_seq, seq_error = mq_reserve_ready_seq();
  if not ready_seq then return {'error', seq_error}; end
end

-- Clean impossible active orphans before accepting this MID. A retained dead
-- role is intentionally independent and may coexist with the fresh active.
remove_active_indexes();
redis.call('zrem', _:qk-leased, mid);
redis.call('hdel', _:qk-lease-tokens, mid);
redis.call('hdel', _:qk-meta, mid);
redis.call('hdel', _:qk-payloads, mid);
redis.call('hdel', _:qk-successor-meta, mid);
redis.call('hdel', _:qk-successor-payloads, mid);

redis.call('hset', _:qk-payloads, mid, _:payload);
redis.call('hset', _:qk-meta, mid, cmsgpack.pack(meta));
place_active(priority, available_at, ready_seq);

return {'added', tostring(now), tostring(available_at), prior_dead and '1' or '0'};
