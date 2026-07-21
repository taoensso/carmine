local mid = _:mid;
local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000);
if redis.call('hget', _:qk-lease-tokens, mid) ~= _:lease-token then
  return {'stale'};
end

local packed = redis.call('hget', _:qk-meta, mid);
local payload = redis.call('hget', _:qk-payloads, mid);
if (not packed) or (not payload) then return {'stale'}; end
if not mq_validate_payload_envelope(payload) then return {'corrupt'}; end
local meta = mq_unpack_active(packed);
if (not meta) or meta[3] < 1 then return {'corrupt'}; end

local successor_state = mq_read_successor(mid);
if successor_state ~= 'absent' then
  if successor_state == 'valid' then
    -- No handler saw the released active. The already-requested successor is
    -- newer, so promote it directly instead of creating attempt-0 active work
    -- beside an older successor and inverting future coalescing order.
    local _, successor_seq, seq_error =
      mq_prepare_successor_promotion(mid, now);
    if seq_error then return {'error', seq_error}; end
    mq_cleanup_active(mid);
    mq_promote_successor(mid, now, successor_seq);
    return {'released-successor'};
  end
  return {'corrupt'};
end

-- A worker that stops between claim and handler invocation must give the
-- message back without consuming an attempt: no delivery was actually made.
local ready_seq, seq_error = mq_reserve_ready_seq();
if not ready_seq then return {'error', seq_error}; end
meta[3] = meta[3] - 1;
-- Densify legacy 7-field records on rewrite so stored metas converge to
-- the current 8-field form.
meta[8] = meta[8] or false;
redis.call('hset', _:qk-meta, mid, cmsgpack.pack(meta));
redis.call('zrem', _:qk-leased, mid);
redis.call('hdel', _:qk-lease-tokens, mid);
mq_ready(mid, meta[2], ready_seq);
return {'released'};
