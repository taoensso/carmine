local mid = _:mid;
local dead_score = redis.call('zscore', _:qk-dead, mid);
local packed = redis.call('hget', _:qk-failures, mid);
local payload = redis.call('hget', _:qk-dead-payloads, mid);
local present = (dead_score and 1 or 0) + (packed and 1 or 0) + (payload and 1 or 0);
if present == 0 then return {'not-dead'}; end
if present ~= 3 then return {'corrupt'}; end
if not mq_validate_payload_envelope(payload) then return {'corrupt'}; end
local failure = mq_unpack_failure(packed, dead_score);
if not failure then return {'corrupt'}; end

if redis.call('hexists', _:qk-payloads, mid) == 1 or
   redis.call('hexists', _:qk-meta, mid) == 1 then return {'active-exists'}; end

-- Redrive has a JVM codec preflight that Lua cannot perform. Return binary-
-- marked copies of the exact durable fields alongside the decoded candidate;
-- commit compares all three byte-for-byte, so an out-of-band change can never
-- cause a different payload or failure generation to be moved.
if _:mode == 'inspect' then
  local binary_marker = string.char(0, 60);
  return {'candidate', payload, binary_marker .. payload,
    binary_marker .. packed, dead_score};
end
if _:mode ~= 'commit' then return {'corrupt'}; end
if _:expected-payload ~= payload or _:expected-packed ~= packed or
   _:expected-score ~= dead_score then return {'changed'}; end

local ready_seq, seq_error = mq_reserve_ready_seq();
if not ready_seq then return {seq_error}; end

local meta = {1, failure[5], 0, failure[6], failure[7],
  failure[8] or 'dead', failure[9] or false, failure[10] or false};

-- Successors and active indexes without active hashes are impossible. Remove
-- all of them only after the complete dead record and sequence reservation
-- have passed preflight, so every refused redrive is byte-for-byte read-only.
redis.call('hdel', _:qk-successor-payloads, mid);
redis.call('hdel', _:qk-successor-meta, mid);
redis.call('zrem', _:qk-ready-high, mid);
redis.call('zrem', _:qk-ready-normal, mid);
redis.call('zrem', _:qk-ready-low, mid);
redis.call('zrem', _:qk-scheduled, mid);
redis.call('zrem', _:qk-leased, mid);
redis.call('hdel', _:qk-lease-tokens, mid);
redis.call('hset', _:qk-payloads, mid, payload);
redis.call('hset', _:qk-meta, mid, cmsgpack.pack(meta));
redis.call('zrem', _:qk-dead, mid);
redis.call('hdel', _:qk-failures, mid);
redis.call('hdel', _:qk-dead-payloads, mid);
mq_ready(mid, meta[2], ready_seq);
return {'redriven'};
