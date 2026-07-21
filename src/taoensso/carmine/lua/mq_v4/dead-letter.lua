local mid = _:mid;
local dead_score = redis.call('zscore', _:qk-dead, mid);
local packed = redis.call('hget', _:qk-failures, mid);
local payload = redis.call('hget', _:qk-dead-payloads, mid);
local present = (dead_score and 1 or 0) + (packed and 1 or 0) + (payload and 1 or 0);
if present == 0 then return {'absent'}; end
if present ~= 3 then return {'corrupt', 'incomplete-record'}; end
if not mq_validate_payload_envelope(payload) then
  return {'corrupt', 'invalid-payload-envelope'};
end
local failure = mq_unpack_failure(packed, dead_score);
if not failure then
  return {'corrupt', 'invalid-failure-meta'};
end
return {'dead', payload, tostring(failure[3]), tostring(failure[4]), failure[2],
  tostring(failure[5]), tostring(failure[6]), tostring(failure[7])};
