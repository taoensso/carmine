local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000);
local mid = _:mid;
local packed = redis.call('hget', _:qk-meta, mid);
local payload = redis.call('hget', _:qk-payloads, mid);
local active_parts = (packed and 1 or 0) + (payload and 1 or 0);

local successor_payload = redis.call('hget', _:qk-successor-payloads, mid);
local successor_packed = redis.call('hget', _:qk-successor-meta, mid);
local successor_parts = (successor_payload and 1 or 0) + (successor_packed and 1 or 0);
local successor_meta = nil;
if successor_parts == 1 then return {'corrupt'}; end
if successor_parts == 2 then
  if not mq_validate_payload_envelope(successor_payload) then return {'corrupt'}; end
  successor_meta = mq_unpack_successor(successor_packed);
  if not successor_meta then return {'corrupt'}; end
end

local dead_score = redis.call('zscore', _:qk-dead, mid);
local failure_packed = redis.call('hget', _:qk-failures, mid);
local dead_payload = redis.call('hget', _:qk-dead-payloads, mid);
local dead_parts = (dead_score and 1 or 0) + (failure_packed and 1 or 0) +
  (dead_payload and 1 or 0);
local failure = nil;
if dead_parts ~= 0 then
  if dead_parts ~= 3 then return {'corrupt'}; end
  if not mq_validate_payload_envelope(dead_payload) then return {'corrupt'}; end
  failure = mq_unpack_failure(failure_packed, dead_score);
  if not failure then return {'corrupt'}; end
end

-- Read active indexes even when both active hashes are absent. A bare index is
-- still part of a partial active role and must not be hidden by an otherwise
-- valid retained-dead role (or reported as completely absent).
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

if active_parts ~= 2 then
  if active_parts == 1 or successor_parts ~= 0 or
      active_index_count ~= 0 or lease_token then
    return {'corrupt'};
  end
  if failure then
    return {'dead', tostring(failure[4]), tostring(failure[6]), tostring(failure[3]),
      '0', '1', tostring(failure[5]), tostring(failure[7])};
  end
  return {'absent'};
end

local meta = mq_unpack_active(packed);
if not mq_validate_payload_envelope(payload) then return {'corrupt'}; end
if not meta or (successor_meta and meta[3] == 0) then return {'corrupt'}; end
-- Every active record must occupy exactly one index, and a fencing token must
-- exist iff that sole index is leased. Check these cross-key invariants before
-- interpreting any one index as authoritative.
if active_index_count ~= 1 or
   ((leased_score and not lease_token) or
    ((not leased_score) and lease_token)) then
  return {'corrupt'};
end
local suffix = {successor_meta and '1' or '0', failure and '1' or '0'};
local lease_expiry = leased_score;
if lease_expiry then
  lease_expiry = mq_integer_between(tonumber(lease_expiry), 0, mq_max_timestamp);
  if not lease_expiry or meta[3] < 1 then return {'corrupt'}; end
  local state = (lease_expiry <= now) and 'lease-expired' or 'leased';
  return {state, tostring(meta[3]), tostring(meta[4]), tostring(lease_expiry),
    suffix[1], suffix[2], tostring(meta[2]), tostring(meta[5])};
end
local available_at = scheduled_score;
if available_at then
  available_at = mq_integer_between(tonumber(available_at), 0, mq_max_timestamp);
  if not available_at or meta[3] >= meta[4] then return {'corrupt'}; end
  return {'scheduled', tostring(meta[3]), tostring(meta[4]), tostring(available_at),
    suffix[1], suffix[2], tostring(meta[2]), tostring(meta[5])};
end
local ready_score = nil;
local ready_priority = nil;
if ready_high_score then
  ready_score = ready_high_score;
  ready_priority = 0;
elseif ready_normal_score then
  ready_score = ready_normal_score;
  ready_priority = 1;
elseif ready_low_score then
  ready_score = ready_low_score;
  ready_priority = 2;
end
if ready_score then
  ready_score = mq_integer_between(tonumber(ready_score), 1, mq_max_revision);
  if not ready_score or ready_priority ~= meta[2] or meta[3] >= meta[4] then
    return {'corrupt'};
  end
  return {'ready', tostring(meta[3]), tostring(meta[4]), false,
    suffix[1], suffix[2], tostring(meta[2]), tostring(meta[5])};
end
return {'corrupt'};
