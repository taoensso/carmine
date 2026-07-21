local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000);
local requested = _:status;
local limit = math.max(1, tonumber(_:limit));
local include_related = _:include-related == '1';
local cursor_group = tonumber(_:cursor-group);
local cursor_score_raw = (_:cursor-score ~= '') and _:cursor-score or nil;
local cursor_score = cursor_score_raw and tonumber(cursor_score_raw) or nil;
if _:cursor-score ~= '' and not cursor_score then
  return redis.error_reply('invalid active-page cursor score');
end
local cursor_same_score_count = tonumber(_:cursor-same-score-count);
local cursor_mid = _:cursor-mid;

local groups = {};
local add_group = function(
    key, indexed_status, expected_priority, min_score, max_score)
  table.insert(groups, {
    key = key, indexed_status = indexed_status,
    expected_priority = expected_priority,
    min_score = min_score, max_score = max_score
  });
end

if requested == 'ready' then
  add_group(_:qk-ready-high,   'ready', 0, '-inf', '+inf');
  add_group(_:qk-ready-normal, 'ready', 1, '-inf', '+inf');
  add_group(_:qk-ready-low,    'ready', 2, '-inf', '+inf');
elseif requested == 'scheduled' then
  add_group(_:qk-scheduled, 'scheduled', nil, '(' .. tostring(now), '+inf');
elseif requested == 'overdue' then
  add_group(_:qk-scheduled, 'scheduled', nil, '-inf', tostring(now));
elseif requested == 'leased' then
  add_group(_:qk-leased, 'leased', nil, '(' .. tostring(now), '+inf');
elseif requested == 'lease-expired' then
  add_group(_:qk-leased, 'leased', nil, '-inf', tostring(now));
else
  return redis.error_reply('unexpected active-page status');
end

local candidates = {};
local target = limit + 1;

local append_pairs = function(
    group_index, indexed_status, expected_priority,
    pairs, prior_score, prior_count)
  local seen_score = prior_score;
  local seen_count = prior_count or 0;
  for idx = 1, #pairs, 2 do
    local mid = pairs[idx];
    local score = pairs[idx + 1];
    local numeric_score = tonumber(score);
    if seen_score and numeric_score == seen_score then
      seen_count = seen_count + 1;
    else
      seen_score = numeric_score;
      seen_count = 1;
    end
    table.insert(candidates, {
      mid = mid, score = score, same_score_count = seen_count,
      group = group_index, indexed_status = indexed_status,
      expected_priority = expected_priority
    });
    if #candidates >= target then break; end
  end
  return seen_score, seen_count;
end

for lua_group = cursor_group + 1, #groups do
  if #candidates >= target then break; end
  local group = groups[lua_group];
  local group_index = lua_group - 1;
  local remaining = target - #candidates;
  local resume = cursor_score and group_index == cursor_group;

  if resume then
    local within_lower =
      group.min_score == '-inf' or cursor_score > tonumber(string.sub(group.min_score, 2));
    local within_upper =
      group.max_score == '+inf' or cursor_score <= tonumber(group.max_score);

    if within_lower and within_upper then
      local same_score_offset = cursor_same_score_count;
      local current_score = redis.call('zscore', group.key, cursor_mid);
      if current_score and tonumber(current_score) == cursor_score then
        local cursor_rank = redis.call('zrank', group.key, cursor_mid);
        -- Redis score bounds must use the exact canonical cursor string. Lua
        -- tostring rounds some adjacent doubles (e.g. 1.0000000000000002 to
        -- 1), which can make a limit-1 traversal return its cursor again.
        local lower_count = redis.call('zcount', group.key,
          '-inf', '(' .. cursor_score_raw);
        same_score_offset = cursor_rank - lower_count + 1;
      end

      local tied = redis.call('zrangebyscore', group.key,
        cursor_score_raw, cursor_score_raw, 'withscores',
        'limit', same_score_offset, remaining);
      local last_score, last_count = append_pairs(
        group_index, group.indexed_status, group.expected_priority, tied,
        cursor_score, same_score_offset);
      remaining = target - #candidates;
      if remaining > 0 then
        local later = redis.call('zrangebyscore', group.key,
          '(' .. cursor_score_raw, group.max_score,
          'withscores', 'limit', 0, remaining);
        append_pairs(group_index, group.indexed_status,
          group.expected_priority, later, last_score, last_count);
      end
    elseif group.min_score ~= '-inf' and
        cursor_score <= tonumber(string.sub(group.min_score, 2)) then
      local pairs = redis.call('zrangebyscore', group.key,
        group.min_score, group.max_score, 'withscores', 'limit', 0, remaining);
      append_pairs(group_index, group.indexed_status,
        group.expected_priority, pairs, nil, 0);
    end
  else
    local pairs = redis.call('zrangebyscore', group.key,
      group.min_score, group.max_score, 'withscores', 'limit', 0, remaining);
    append_pairs(group_index, group.indexed_status,
      group.expected_priority, pairs, nil, 0);
  end
end

local more = #candidates > limit;
if more then table.remove(candidates); end

local next_group = false;
local next_score = false;
local next_same_score_count = false;
local next_mid = false;
if more then
  local last = candidates[#candidates];
  next_group = tostring(last.group);
  next_score = last.score;
  next_same_score_count = tostring(last.same_score_count);
  next_mid = last.mid;
end

local mids = {};
for idx, candidate in ipairs(candidates) do mids[idx] = candidate.mid; end
local packed_metas = {};
local active_scores = {};
local lease_tokens = {};
if #mids > 0 then
  packed_metas = redis.call('hmget', _:qk-meta, unpack(mids));
  -- Redis 7+ guarantees ZMSCORE. Six batched lookups enforce cross-index and
  -- token invariants for the whole bounded page (at most 250 entries), rather
  -- than issuing six per-entry lookups from Lua.
  active_scores = {
    redis.call('zmscore', _:qk-ready-high,   unpack(mids)),
    redis.call('zmscore', _:qk-ready-normal, unpack(mids)),
    redis.call('zmscore', _:qk-ready-low,    unpack(mids)),
    redis.call('zmscore', _:qk-scheduled,    unpack(mids)),
    redis.call('zmscore', _:qk-leased,       unpack(mids))
  };
  lease_tokens = redis.call('hmget', _:qk-lease-tokens, unpack(mids));
end

local items = {};
for idx, candidate in ipairs(candidates) do
  local mid = candidate.mid;
  local packed = packed_metas[idx];
  local has_payload = redis.call('hexists', _:qk-payloads, mid) == 1;
  local score_number = tonumber(candidate.score);
  -- Ready scores are the positive, exactly represented INCR sequence. Other
  -- active indexes contain non-negative, integral epoch-millisecond values.
  local score_valid = nil;
  if candidate.indexed_status == 'ready' then
    score_valid = mq_integer_between(score_number, 1, mq_max_revision);
  else
    score_valid = mq_integer_between(score_number, 0, mq_max_timestamp);
  end
  local timestamp = candidate.indexed_status == 'ready' and false or candidate.score;
  local timestamp_number = timestamp and score_valid or nil;
  local state = candidate.indexed_status;
  if state == 'leased' and timestamp_number and timestamp_number <= now then
    state = 'lease-expired';
  end

  local active_index_count = 0;
  for role = 1, 5 do
    if active_scores[role][idx] then
      active_index_count = active_index_count + 1;
    end
  end
  local has_lease_token = lease_tokens[idx] and true or false;
  local index_role_valid = active_index_count == 1 and
    ((candidate.indexed_status == 'leased') == has_lease_token);

  if not score_valid then
    items[idx] = {mid, 'corrupt', candidate.indexed_status, false};
  elseif not index_role_valid then
    items[idx] = {mid, 'corrupt', candidate.indexed_status, timestamp};
  elseif (not packed) or (not has_payload) then
    items[idx] = {mid, 'orphan', candidate.indexed_status, timestamp};
  else
    local meta = mq_unpack_active(packed);
    local role_valid = false;
    local priority_valid = false;
    if meta then
      if candidate.indexed_status == 'leased' then
        role_valid = meta[3] >= 1;
      else
        -- Ready and scheduled records still need another claim available.
        role_valid = meta[3] < meta[4];
      end
      priority_valid = candidate.indexed_status ~= 'ready' or
        meta[2] == candidate.expected_priority;
    end
    if (not meta) or (not role_valid) or (not priority_valid) then
      items[idx] = {mid, 'corrupt', candidate.indexed_status, timestamp};
    else
      local priority = meta[2];
      local attempt = meta[3];
      local max_attempts = meta[4];
      local enqueued_at = meta[5];
      local successor = false;
      local prior_dead = false;
      local related_corrupt = false;
      if include_related then
        local successor_payload = redis.call('hget', _:qk-successor-payloads, mid);
        local successor_packed = redis.call('hget', _:qk-successor-meta, mid);
        local successor_parts = (successor_payload and 1 or 0) +
          (successor_packed and 1 or 0);
        if successor_parts == 2 and mq_validate_payload_envelope(successor_payload) and
            mq_unpack_successor(successor_packed) and
            attempt > 0 then
          successor = '1';
        elseif successor_parts ~= 0 then
          related_corrupt = true;
        else
          successor = '0';
        end

        local dead_score = redis.call('zscore', _:qk-dead, mid);
        local dead_payload = redis.call('hget', _:qk-dead-payloads, mid);
        local failure_packed = redis.call('hget', _:qk-failures, mid);
        local dead_parts = (dead_score and 1 or 0) + (dead_payload and 1 or 0) +
          (failure_packed and 1 or 0);
        if dead_parts == 3 and mq_validate_payload_envelope(dead_payload) and
            mq_unpack_failure(failure_packed, dead_score) then
          prior_dead = '1';
        elseif dead_parts ~= 0 then
          related_corrupt = true;
        else
          prior_dead = '0';
        end
      end
      if related_corrupt then
        items[idx] = {mid, 'corrupt', candidate.indexed_status, timestamp};
      else
        local overdue = candidate.indexed_status == 'scheduled' and
          timestamp_number <= now and '1' or '0';
        items[idx] = {
          mid, state, candidate.indexed_status, timestamp,
          tostring(priority), tostring(attempt), tostring(max_attempts),
          tostring(enqueued_at), successor, prior_dead, overdue
        };
      end
    end
  end
end

return {
  tostring(now), more and '1' or '0', next_group, next_score,
  next_same_score_count, next_mid, items
};
