local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000);
local nready = redis.call('zcard', _:qk-ready-high) +
  redis.call('zcard', _:qk-ready-normal) + redis.call('zcard', _:qk-ready-low);
local noverdue = redis.call('zcount', _:qk-scheduled, '-inf', now);
local nsched = redis.call('zcard', _:qk-scheduled) - noverdue;
local nexpired = redis.call('zcount', _:qk-leased, '-inf', now);
local nleased = redis.call('zcard', _:qk-leased) - nexpired;

local first_score = function(key)
  local entry = redis.call('zrange', key, 0, 0, 'withscores');
  local score = entry[2] and
    mq_integer_between(tonumber(entry[2]), 0, mq_max_timestamp) or nil;
  if score then return tostring(score); else return ''; end
end

local first_score_between = function(key, min_score, max_score)
  local entry = redis.call(
    'zrangebyscore', key, min_score, max_score, 'withscores', 'limit', 0, 1);
  local score = entry[2] and
    mq_integer_between(tonumber(entry[2]), 0, mq_max_timestamp) or nil;
  if score then return tostring(score); else return ''; end
end

local first_ready_enqueued_at = function(key, expected_priority)
  local entry = redis.call('zrange', key, 0, 0, 'withscores');
  if not entry[1] then return ''; end
  local score = entry[2] and
    mq_integer_between(tonumber(entry[2]), 1, mq_max_revision) or nil;
  if not score then return ''; end
  local packed = redis.call('hget', _:qk-meta, entry[1]);
  if not packed then return ''; end
  local meta = mq_unpack_active(packed);
  if not meta or meta[2] ~= expected_priority or meta[3] >= meta[4] then
    return '';
  end
  return tostring(meta[5]);
end

return {tostring(nready), tostring(noverdue), tostring(nsched), tostring(nleased),
  tostring(nexpired), tostring(redis.call('zcard', _:qk-dead)),
  tostring(redis.call('hlen', _:qk-successor-payloads)),
  tostring(redis.call('hlen', _:qk-payloads)), tostring(now),
  first_ready_enqueued_at(_:qk-ready-high, 0),
  first_ready_enqueued_at(_:qk-ready-normal, 1),
  first_ready_enqueued_at(_:qk-ready-low, 2),
  first_score(_:qk-scheduled), first_score(_:qk-leased), first_score(_:qk-dead),
  first_score_between(_:qk-scheduled, '-inf', now),
  first_score_between(_:qk-scheduled, '(' .. tostring(now), '+inf'),
  first_score_between(_:qk-leased, '-inf', now),
  first_score_between(_:qk-leased, '(' .. tostring(now), '+inf')};
