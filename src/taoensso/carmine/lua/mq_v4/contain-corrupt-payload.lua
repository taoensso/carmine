local mid = _:mid;
if redis.call('hget', _:qk-lease-tokens, mid) ~= _:lease-token then
  return {'stale'};
end
if redis.call('hget', _:qk-payloads, mid) ~= _:expected-payload then
  return {'stale'};
end

-- The JVM has unambiguously identified the payload reply as Carmine's own
-- Nippy thaw failure. Fence containment to this exact claimed generation, and
-- reserve any successor FIFO score before removing it.
local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) +
  math.floor(tonumber(now_parts[2]) / 1000);
local _, successor_seq, seq_error = mq_prepare_successor_promotion(mid, now);
if seq_error then return {'error', seq_error}; end
mq_cleanup_active(mid);
local promoted = mq_promote_successor(mid, now, successor_seq);
return {'contained', promoted and '1' or '0'};
