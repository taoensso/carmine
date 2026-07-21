if redis.call('hget', _:qk-lease-tokens, _:mid) ~= _:lease-token then
  return {'stale'};
end

local current_expiry_raw = redis.call('zscore', _:qk-leased, _:mid);
if not current_expiry_raw then return {'stale'}; end
local current_expiry = tonumber(current_expiry_raw);
-- Preserve the existing safe repair of -inf while rejecting scores that
-- would escape the durable timestamp domain or leak an unparsable expiry.
if current_expiry == -math.huge then current_expiry = 0; end
current_expiry = mq_integer_between(current_expiry, 0, mq_max_timestamp);
if not current_expiry then return {'corrupt', 'lease-expiry'}; end
-- Each extension re-reads the active metadata so a per-message lease override
-- (meta[8]) governs every renewal, not only the original grant. A missing or
-- invalid record refuses extension: renewing a lease over damaged durable
-- state would only delay its containment. A leased generation must also have
-- consumed an attempt, matching the settle/release/reaper role checks.
local meta = mq_unpack_active(redis.call('hget', _:qk-meta, _:mid));
if (not meta) or meta[3] < 1 then return {'corrupt', 'corrupt-meta'}; end
local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000);
local lease_ms = meta[8] or
  tonumber(redis.call('hget', _:qk-config, 'lease_ms'));
local new_expiry = math.max(current_expiry, now + lease_ms);
redis.call('zadd', _:qk-leased, 'xx', new_expiry, _:mid);
return {'extended', tostring(new_expiry)};
