-- Packed MQ records are durable data, so every script must apply the same
-- validation before using their fields. Schema 1 records evolve additively:
-- the original active fields end at enqueued_at, while policy, revision,
-- successor availability, and the per-message lease override are optional for
-- records written by older previews.

local mq_max_timestamp = 99999999999999;
local mq_max_revision = 9007199254740991;
local mq_max_attempts = 1000000;
local mq_max_duration = 10000000000000;

local mq_integer_between = function(value, minimum, maximum)
  if type(value) ~= 'number' or value ~= value or
     value < minimum or value > maximum or value ~= math.floor(value) then
    return nil;
  end
  return value;
end

local mq_optional_policy_valid = function(value)
  return value == nil or value == false or value == 'dead' or value == 'discard';
end

local mq_optional_revision_valid = function(value)
  return value == nil or value == false or
    mq_integer_between(value, 0, mq_max_revision) ~= nil;
end

-- Per-message lease override durations share the public duration bound
-- (10^13 ms), not the wider timestamp bound: claim/extension arithmetic adds
-- them to epoch-ms server time and the sum must stay Lua-double exact.
local mq_optional_lease_valid = function(value)
  return value == nil or value == false or
    mq_integer_between(value, 1, mq_max_duration) ~= nil;
end

-- MQ owns one durable payload codec. Every legitimate envelope begins with
-- Carmine's NUL + `>` Nippy marker followed by Nippy's `NPY` header. A valid
-- Nippy value has at least one header/version byte and one body byte after
-- that prefix. This deliberately remains an O(1) structural check: it is not
-- a checksum and does not deserialize untrusted durable bytes.
local mq_validate_payload_envelope = function(payload)
  return type(payload) == 'string' and string.len(payload) >= 7 and
    string.byte(payload, 1) == 0 and string.byte(payload, 2) == 62 and
    string.byte(payload, 3) == 78 and string.byte(payload, 4) == 80 and
    string.byte(payload, 5) == 89;
end

local mq_validate_active = function(meta)
  if type(meta) ~= 'table' or mq_integer_between(meta[1], 1, 1) == nil then
    return false;
  end
  local priority = mq_integer_between(meta[2], 0, 2);
  local attempt = mq_integer_between(meta[3], 0, mq_max_attempts);
  local max_attempts = mq_integer_between(meta[4], 1, mq_max_attempts);
  local enqueued_at = mq_integer_between(meta[5], 0, mq_max_timestamp);
  return priority ~= nil and attempt ~= nil and max_attempts ~= nil and
    attempt <= max_attempts and enqueued_at ~= nil and
    mq_optional_policy_valid(meta[6]) and mq_optional_revision_valid(meta[7]) and
    mq_optional_lease_valid(meta[8]);
end

local mq_validate_successor = function(successor)
  if type(successor) ~= 'table' or
     mq_integer_between(successor[1], 1, 1) == nil then
    return false;
  end
  local priority = mq_integer_between(successor[2], 0, 2);
  local max_attempts = mq_integer_between(successor[3], 1, mq_max_attempts);
  local enqueued_at = mq_integer_between(successor[4], 0, mq_max_timestamp);
  local available_at = successor[7];
  return priority ~= nil and max_attempts ~= nil and enqueued_at ~= nil and
    mq_optional_policy_valid(successor[5]) and
    mq_optional_revision_valid(successor[6]) and
    (available_at == nil or available_at == false or
      mq_integer_between(available_at, 0, mq_max_timestamp) ~= nil) and
    mq_optional_lease_valid(successor[8]);
end

local mq_validate_failure = function(failure, dead_score)
  if type(failure) ~= 'table' or
     mq_integer_between(failure[1], 1, 1) == nil then
    return false;
  end
  local reason = failure[2];
  local failed_at = mq_integer_between(failure[3], 0, mq_max_timestamp);
  local attempt = mq_integer_between(failure[4], 1, mq_max_attempts);
  local priority = mq_integer_between(failure[5], 0, 2);
  local max_attempts = mq_integer_between(failure[6], 1, mq_max_attempts);
  local enqueued_at = mq_integer_between(failure[7], 0, mq_max_timestamp);
  local score = dead_score;
  if type(score) == 'string' then score = tonumber(score); end
  score = mq_integer_between(score, 0, mq_max_timestamp);
  -- The public bound is 1024 UTF-16 code units. A code unit occupies at most
  -- three UTF-8 bytes in Redis (four-byte UTF-8 sequences are surrogate
  -- pairs, i.e. two units), so 4096 bytes safely covers the durable form.
  -- Carmine reserves a leading NUL byte for RESP auto-thaw markers.
  return type(reason) == 'string' and string.len(reason) >= 1 and
    string.len(reason) <= 4096 and string.byte(reason, 1) ~= 0 and
    failed_at ~= nil and score ~= nil and
    failed_at == score and attempt ~= nil and priority ~= nil and
    max_attempts ~= nil and attempt <= max_attempts and enqueued_at ~= nil and
    mq_optional_policy_valid(failure[8]) and
    mq_optional_revision_valid(failure[9]) and
    mq_optional_lease_valid(failure[10]);
end

local mq_unpack_active = function(packed)
  if not packed then return nil; end
  local ok, meta = pcall(cmsgpack.unpack, packed);
  if not ok or not mq_validate_active(meta) then return nil; end
  return meta;
end

local mq_unpack_successor = function(packed)
  if not packed then return nil; end
  local ok, successor = pcall(cmsgpack.unpack, packed);
  if not ok or not mq_validate_successor(successor) then return nil; end
  return successor;
end

local mq_unpack_failure = function(packed, dead_score)
  if not packed then return nil; end
  local ok, failure = pcall(cmsgpack.unpack, packed);
  if not ok or not mq_validate_failure(failure, dead_score) then return nil; end
  return failure;
end
