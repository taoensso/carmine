local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000);
local mid = _:mid;

if redis.call('hget', _:qk-lease-tokens, mid) ~= _:lease-token then
  return {'stale'};
end

local packed = redis.call('hget', _:qk-meta, mid);
local payload = redis.call('hget', _:qk-payloads, mid);
if (not packed) or (not payload) then return {'stale'}; end
if not mq_validate_payload_envelope(payload) then return {'error', 'corrupt-payload'}; end
local meta = mq_unpack_active(packed);
if (not meta) or meta[3] < 1 then
  return {'error', 'corrupt-meta'};
end

-- Every terminal path removes the active generation before promotion. Validate
-- a present successor first so malformed newer work cannot be silently erased.
local successor_state = mq_read_successor(mid);
if successor_state == 'corrupt' then return {'error', 'corrupt-successor'}; end

local successor_seq = nil;
local finish = function(action, timestamp)
  local promoted = mq_promote_successor(mid, now, successor_seq);
  return {action, timestamp, promoted and '1' or '0'};
end

local action = _:action;
if action ~= 'ack' and action ~= 'discard' and
   action ~= 'dead' and action ~= 'retry' then
  return {'error', 'unexpected-action'};
end

if action ~= 'retry' or meta[3] >= meta[4] then
  local seq_error = nil;
  successor_state, successor_seq, seq_error =
    mq_prepare_successor_promotion(mid, now);
  if successor_state == 'corrupt' then
    return {'error', 'corrupt-successor'};
  end
  if seq_error then return {'error', seq_error}; end
end

if action == 'ack' then
  mq_cleanup_active(mid);
  return finish('acked', false);
elseif action == 'discard' then
  mq_cleanup_active(mid);
  return finish('discarded', false);
elseif action == 'dead' then
  mq_dead_letter(mid, meta, payload, _:reason, now);
  return finish('dead', tostring(now));
elseif action == 'retry' then
  if meta[3] >= meta[4] then
    local exhausted_action = mq_exhaust(mid, meta, payload, _:reason, now);
    return finish(exhausted_action, tostring(now));
  end

  local delay_ms = tonumber(_:delay-ms);
  if delay_ms < 0 then
    local retry_base_ms = tonumber(redis.call('hget', _:qk-config, 'retry_base_ms'));
    local retry_max_ms  = tonumber(redis.call('hget', _:qk-config, 'retry_max_ms'));
    delay_ms = math.min(retry_max_ms,
      retry_base_ms * (2 ^ math.min(62, math.max(0, tonumber(meta[3]) - 1))));

    if delay_ms > 0 and redis.call('hget', _:qk-config, 'retry_jitter') == 'full' then
      -- Derive a deterministic uniform sample from this random lease token.
      -- Avoid math.random so replicas and script replays remain deterministic.
      local digest = redis.sha1hex(_:lease-token .. ':' .. tostring(meta[3]));
      local sample = tonumber(string.sub(digest, 1, 13), 16);
      delay_ms = math.floor((sample / 4503599627370496) * (delay_ms + 1));
    end
  end

  local ready_seq = nil;
  if delay_ms == 0 then
    local seq_error = nil;
    ready_seq, seq_error = mq_reserve_ready_seq();
    if not ready_seq then return {'error', seq_error}; end
  end

  redis.call('zrem', _:qk-leased, mid);
  redis.call('hdel', _:qk-lease-tokens, mid);
  if delay_ms > 0 then
    redis.call('zadd', _:qk-scheduled, now + delay_ms, mid);
    mq_signal();
  else
    mq_ready(mid, meta[2], ready_seq);
  end
  return {'retried', tostring(now + delay_ms), '0', tostring(delay_ms)};
end
