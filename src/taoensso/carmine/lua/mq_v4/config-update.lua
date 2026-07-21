-- Atomically retunes durable queue configuration.
-- Follows ensure.lua's preflight-before-write discipline: the complete
-- current config is read and validated first, every refusal is read-only,
-- and one trailing variadic HSET commits all effective changes at once.
-- Config changes create no ready work, so no wake signal is published.

if redis.call('exists', _:qk-config) ~= 1 then
  return {'missing-config'};
end

local hgetall_ok, raw_config = pcall(redis.call, 'hgetall', _:qk-config);
if not hgetall_ok then return {'corrupt-config', 'config-type'}; end
local current = {};
for idx = 1, #raw_config, 2 do
  current[raw_config[idx]] = raw_config[idx + 1];
end

-- Durable numeric fields must be canonical non-negative integer strings:
-- tonumber alone accepts spellings like '1e3' or '1000.0' whose raw bytes
-- would then survive the write and break strict client parsing afterward.
local canonical_config_int = function(raw, minimum, maximum)
  if type(raw) ~= 'string' or string.len(raw) == 0 then return nil; end
  if raw ~= '0' then
    local first = string.byte(raw, 1);
    if first < 49 or first > 57 then return nil; end
    for idx = 2, string.len(raw) do
      local digit = string.byte(raw, idx);
      if digit < 48 or digit > 57 then return nil; end
    end
  end
  return mq_integer_between(tonumber(raw), minimum, maximum);
end

if current['schema'] ~= _:schema then
  return {'corrupt-config', 'schema'};
end
if canonical_config_int(current['lease_ms'], 1, mq_max_duration) == nil then
  return {'corrupt-config', 'lease_ms'};
end
if canonical_config_int(current['max_attempts'], 1, mq_max_attempts) == nil then
  return {'corrupt-config', 'max_attempts'};
end
if canonical_config_int(current['retry_base_ms'], 0, mq_max_duration) == nil then
  return {'corrupt-config', 'retry_base_ms'};
end
if canonical_config_int(current['retry_max_ms'], 0, mq_max_duration) == nil then
  return {'corrupt-config', 'retry_max_ms'};
end
-- Additive fields may legitimately be absent on queues created by older
-- previews; present values must still parse.
local jitter = current['retry_jitter'];
if jitter ~= nil and jitter ~= 'none' and jitter ~= 'full' then
  return {'corrupt-config', 'retry_jitter'};
end
local exhaustion = current['on_exhaustion_default'];
if exhaustion ~= nil and exhaustion ~= 'dead' and exhaustion ~= 'discard' then
  return {'corrupt-config', 'on_exhaustion_default'};
end
local duplicate = current['on_duplicate_default'];
if duplicate ~= nil and duplicate ~= 'reject' and duplicate ~= 'coalesce' then
  return {'corrupt-config', 'on_duplicate_default'};
end
local revision_mode = current['revision_mode'];
if revision_mode ~= nil and revision_mode ~= 'none' and
   revision_mode ~= 'required' then
  return {'corrupt-config', 'revision_mode'};
end

-- Validate the complete post-update window even when this update supplies no
-- retry fields. Durable state can be damaged outside Carmine; an unrelated
-- update must not commit over a semantically invalid configuration.
local merged_base = tonumber(current['retry_base_ms']);
if _:retry-base-set == '1' then merged_base = tonumber(_:retry-base-ms); end
local merged_max = tonumber(current['retry_max_ms']);
if _:retry-max-set == '1' then merged_max = tonumber(_:retry-max-ms); end
if merged_base > merged_max then
  return {'retry-window-inverted',
    tostring(merged_base), tostring(merged_max)};
end

local changed = {};
local hset_args = {};
local merged = {};
for field, value in pairs(current) do merged[field] = value; end
local consider = function(field, set_flag, new_value)
  if set_flag == '1' and current[field] ~= new_value then
    table.insert(changed, field);
    table.insert(changed, current[field] or false);
    table.insert(changed, new_value);
    table.insert(hset_args, field);
    table.insert(hset_args, new_value);
    merged[field] = new_value;
  end
end
consider('lease_ms',              _:lease-set,         _:lease-ms);
consider('max_attempts',          _:max-attempts-set,  _:max-attempts);
consider('retry_base_ms',         _:retry-base-set,    _:retry-base-ms);
consider('retry_max_ms',          _:retry-max-set,     _:retry-max-ms);
consider('retry_jitter',          _:retry-jitter-set,  _:retry-jitter);
consider('on_exhaustion_default', _:on-exhaustion-set, _:on-exhaustion);

local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000);

local action = 'unchanged';
if #hset_args > 0 then
  action = 'updated';
  redis.call('hset', _:qk-config, unpack(hset_args));
end

-- Return the complete post-write config so the caller can rebuild its handle
-- from durable truth, followed by {field, old, new} triples for each change.
local reply = {action, tostring(now),
  merged['lease_ms'], merged['max_attempts'],
  merged['retry_base_ms'], merged['retry_max_ms'],
  merged['retry_jitter'] or false,
  merged['on_exhaustion_default'] or false,
  merged['on_duplicate_default'] or false,
  merged['revision_mode'] or false};
for _, part in ipairs(changed) do table.insert(reply, part); end
return reply;
