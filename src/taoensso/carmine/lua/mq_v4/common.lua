local mq_signal = function()
  local ok, length = pcall(redis.call, 'llen', _:qk-signal);
  if not ok then
    -- This key is queue-owned and purely advisory. Repair only its type, and
    -- keep all signal failures isolated from the durable transition.
    pcall(redis.call, 'del', _:qk-signal);
    pcall(redis.call, 'lpush', _:qk-signal, '1');
  elseif length == 0 then
    pcall(redis.call, 'lpush', _:qk-signal, '1');
  end
end

local mq_consume_signal = function()
  local ok = pcall(redis.call, 'lpop', _:qk-signal);
  if not ok then
    -- The next mq_signal call recreates the single advisory baton. Polling is
    -- always the fallback if even this narrowly-scoped repair fails.
    pcall(redis.call, 'del', _:qk-signal);
  end
end

local mq_repair_signal_type = function()
  local ok = pcall(redis.call, 'llen', _:qk-signal);
  if not ok then pcall(redis.call, 'del', _:qk-signal); end
end

local mq_canonical_seq = function(raw)
  if type(raw) ~= 'string' or string.len(raw) == 0 then return false; end
  if raw == '0' then return true; end
  local first = string.byte(raw, 1);
  if first < 49 or first > 57 then return false; end
  for idx = 2, string.len(raw) do
    local digit = string.byte(raw, idx);
    if digit < 48 or digit > 57 then return false; end
  end
  return true;
end

-- Reserve a Lua-exact FIFO score before any mutation that will produce ready
-- work. Inspect the raw Redis representation first: converting an out-of-range
-- decimal through Lua's double would lose the distinction at 2^53.
local mq_reserve_ready_seq = function()
  local ok, raw = pcall(redis.call, 'get', _:qk-seq);
  if not ok then return nil, 'corrupt-seq'; end
  if raw then
    if not mq_canonical_seq(raw) then return nil, 'corrupt-seq'; end
    local maximum = '9007199254740991';
    local length = string.len(raw);
    if length > string.len(maximum) or
       (length == string.len(maximum) and raw >= maximum) then
      return nil, 'seq-exhausted';
    end
  end
  local incremented, seq = pcall(redis.call, 'incr', _:qk-seq);
  if not incremented then return nil, 'corrupt-seq'; end
  return seq, nil;
end

local mq_ready = function(mid, priority, ready_seq)
  local ready_key = _:qk-ready-normal;
  if priority == 0 then ready_key = _:qk-ready-high;
  elseif priority == 2 then ready_key = _:qk-ready-low; end
  redis.call('zadd', ready_key, ready_seq, mid);
  mq_signal();
end

local mq_cleanup_active = function(mid)
  redis.call('zrem', _:qk-ready-high, mid);
  redis.call('zrem', _:qk-ready-normal, mid);
  redis.call('zrem', _:qk-ready-low, mid);
  redis.call('zrem', _:qk-scheduled, mid);
  redis.call('zrem', _:qk-leased, mid);
  redis.call('hdel', _:qk-lease-tokens, mid);
  redis.call('hdel', _:qk-payloads, mid);
  redis.call('hdel', _:qk-meta, mid);
end

local mq_read_successor = function(mid)
  local payload = redis.call('hget', _:qk-successor-payloads, mid);
  local packed = redis.call('hget', _:qk-successor-meta, mid);
  if (not payload) and (not packed) then return 'absent', nil, nil; end
  if (not payload) or (not packed) then
    return 'corrupt', nil, nil;
  end
  if not mq_validate_payload_envelope(payload) then return 'corrupt', nil, nil; end

  local successor = mq_unpack_successor(packed);
  if not successor then return 'corrupt', nil, nil; end
  return 'valid', payload, successor;
end

local mq_cleanup_successor = function(mid)
  redis.call('hdel', _:qk-successor-payloads, mid);
  redis.call('hdel', _:qk-successor-meta, mid);
end

-- Returns successor state, a pre-reserved ready score when one is needed, and
-- an optional sequence error. Call this before removing the active generation.
local mq_prepare_successor_promotion = function(mid, now)
  local state, _, successor = mq_read_successor(mid);
  if state ~= 'valid' then return state, nil, nil; end
  local available_at = successor[7] or now;
  if available_at <= now then
    local ready_seq, seq_error = mq_reserve_ready_seq();
    if not ready_seq then return state, nil, seq_error; end
    return state, ready_seq, nil;
  end
  return state, nil, nil;
end

local mq_promote_successor = function(mid, now, ready_seq)
  local state, payload, successor = mq_read_successor(mid);
  if state ~= 'valid' then
    if state == 'corrupt' then
      -- Explicit corruption cleanup; never construct active metadata from a
      -- partially valid newer generation.
      mq_cleanup_successor(mid);
    end
    return false;
  end

  local priority = successor[2];
  local max_attempts = successor[3];
  local enqueued_at = successor[4];
  local on_exhaustion = successor[5] or
    redis.call('hget', _:qk-config, 'on_exhaustion_default');
  local revision = successor[6] or false;
  local available_at = successor[7] or now;
  local lease_ms_override = successor[8] or false;
  local meta = {1, priority, 0, max_attempts, enqueued_at, on_exhaustion,
    revision, lease_ms_override};

  redis.call('hset', _:qk-payloads, mid, payload);
  redis.call('hset', _:qk-meta, mid, cmsgpack.pack(meta));
  mq_cleanup_successor(mid);

  if available_at > now then
    redis.call('zadd', _:qk-scheduled, available_at, mid);
    mq_signal(); -- Recompute the nearest deadline if this is earlier.
  else
    mq_ready(mid, priority, ready_seq);
  end
  return true;
end

local mq_dead_letter = function(mid, meta, payload, reason, now)
  -- The lease override survives the dead role so a redrive restores it: a
  -- long-running message must not silently revert to the queue default lease.
  local failure = {
    1, reason, now, meta[3], meta[2], meta[4], meta[5],
    meta[6] or redis.call('hget', _:qk-config, 'on_exhaustion_default'),
    meta[7] or false, meta[8] or false
  };
  mq_cleanup_active(mid);
  redis.call('zadd', _:qk-dead, now, mid);
  redis.call('hset', _:qk-dead-payloads, mid, payload);
  redis.call('hset', _:qk-failures, mid, cmsgpack.pack(failure));
end

local mq_exhaust = function(mid, meta, payload, reason, now)
  local policy = meta[6] or redis.call('hget', _:qk-config, 'on_exhaustion_default');
  if policy == 'discard' then
    mq_cleanup_active(mid);
    return 'discarded';
  else
    mq_dead_letter(mid, meta, payload, reason, now);
    return 'dead';
  end
end
