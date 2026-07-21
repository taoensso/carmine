local redis_version = redis.REDIS_VERSION;
local redis_version_num = tonumber(redis.REDIS_VERSION_NUM);
if (not redis_version) or (not redis_version_num) then
  -- Redis exposes the constants from v7 onward. INFO only improves the error
  -- detail on older servers; a narrowly-scoped ACL may deny it, so never let
  -- that obscure the definitive unsupported-version result.
  local server_info = redis.pcall('info', 'server');
  if type(server_info) == 'string' then
    redis_version = redis_version or string.match(server_info, 'redis_version:([^\r\n]+)');
    if (not redis_version_num) and redis_version then
      local major, minor, patch = string.match(redis_version, '^(%d+)%.(%d+)%.(%d+)');
      if major then
        redis_version_num = (tonumber(major) * 65536) +
          (tonumber(minor) * 256) + tonumber(patch);
      end
    end
  end
end

local required_version_num = tonumber(_:required-redis-version-num);
if (not redis_version_num) or redis_version_num < required_version_num then
  return {'unsupported-version', redis_version or 'unknown',
    tostring(redis_version_num or -1), _:required-redis-version,
    tostring(required_version_num)};
end

local expected = {
  schema         = _:schema,
  lease_ms       = _:lease-ms,
  max_attempts   = _:max-attempts,
  retry_base_ms  = _:retry-base-ms,
  retry_max_ms   = _:retry-max-ms
};

local additive = {
  retry_jitter         = _:retry-jitter,
  on_exhaustion_default = _:on-exhaustion-default,
  on_duplicate_default  = _:on-duplicate-default,
  revision_mode         = _:revision-mode
};

if redis.call('exists', _:qk-config) == 1 then
  for field, value in pairs(expected) do
    if redis.call('hget', _:qk-config, field) ~= tostring(value) then
      return {'mismatch', field, redis.call('hget', _:qk-config, field), tostring(value)};
    end
  end
  -- V4 is still preview, but preserve queues created before these durable
  -- defaults existed. The first upgraded handle establishes missing values;
  -- all subsequent handles must agree.
  for field, value in pairs(additive) do
    local current = redis.call('hget', _:qk-config, field);
    if current and current ~= tostring(value) then
      return {'mismatch', field, current, tostring(value)};
    end
  end
  -- Only fill omissions after the complete preflight succeeds. This makes a
  -- mismatch read-only regardless of Lua table iteration order.
  for field, value in pairs(additive) do
    redis.call('hsetnx', _:qk-config, field, value);
  end
  return {'existing', redis_version, tostring(redis_version_num)};
end

-- A supported clear deliberately retains config, and a move restores config
-- last. Therefore any other queue key without config means initialization was
-- interrupted or durable state was damaged. Refuse to establish a possibly
-- different policy over those artifacts. All declared keys share the queue's
-- Cluster hash tag, so the multi-key EXISTS remains single-slot.
local artifact_keys = {};
for _, key in ipairs(KEYS) do
  if key ~= _:qk-config then table.insert(artifact_keys, key); end
end
local artifact_count = redis.call('exists', unpack(artifact_keys));
if artifact_count ~= 0 then
  return {'missing-config', tostring(artifact_count)};
end

for field, value in pairs(expected) do
  redis.call('hset', _:qk-config, field, value);
end
for field, value in pairs(additive) do
  redis.call('hset', _:qk-config, field, value);
end
return {'created', redis_version, tostring(redis_version_num)};
