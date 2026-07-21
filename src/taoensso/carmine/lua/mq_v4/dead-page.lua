local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000);
local limit = math.max(1, tonumber(_:limit));
local cursor_score_raw = (_:cursor-score ~= '') and _:cursor-score or nil;
local cursor_score = cursor_score_raw and tonumber(cursor_score_raw) or nil;
if _:cursor-score ~= '' and not cursor_score then
  return redis.error_reply('invalid dead-page cursor score');
end
local cursor_same_score_count = tonumber(_:cursor-same-score-count);
local cursor_mid = _:cursor-mid;

local candidates = {};
local target = limit + 1;

local append_pairs = function(pairs, prior_score, prior_count)
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
      mid = mid, score = score, same_score_count = seen_count
    });
    if #candidates >= target then break; end
  end
  return seen_score, seen_count;
end

if cursor_score then
  local same_score_offset = cursor_same_score_count;
  local current_score = redis.call('zscore', _:qk-dead, cursor_mid);
  if current_score and tonumber(current_score) == cursor_score then
    local cursor_rank = redis.call('zrank', _:qk-dead, cursor_mid);
    -- Redis score bounds must use the exact canonical cursor string. Lua
    -- tostring rounds some adjacent doubles (e.g. 1.0000000000000002 to 1),
    -- which can make a limit-1 traversal return its cursor again.
    local lower_count = redis.call('zcount', _:qk-dead,
      '-inf', '(' .. cursor_score_raw);
    same_score_offset = cursor_rank - lower_count + 1;
  end

  local tied = redis.call('zrangebyscore', _:qk-dead,
    cursor_score_raw, cursor_score_raw, 'withscores',
    'limit', same_score_offset, target - #candidates);
  local last_score, last_count = append_pairs(
    tied, cursor_score, same_score_offset);
  local remaining = target - #candidates;
  if remaining > 0 then
    local later = redis.call('zrangebyscore', _:qk-dead,
      '(' .. cursor_score_raw, '+inf', 'withscores', 'limit', 0, remaining);
    append_pairs(later, last_score, last_count);
  end
else
  local pairs = redis.call('zrangebyscore', _:qk-dead,
    '-inf', '+inf', 'withscores', 'limit', 0, target);
  append_pairs(pairs, nil, 0);
end

local more = #candidates > limit;
if more then table.remove(candidates); end

local next_score = false;
local next_same_score_count = false;
local next_mid = false;
if more then
  local last = candidates[#candidates];
  next_score = last.score;
  next_same_score_count = tostring(last.same_score_count);
  next_mid = last.mid;
end

local items = {};
for idx, candidate in ipairs(candidates) do
  -- The dead index carries non-negative, integral epoch-millisecond scores.
  local score_number = tonumber(candidate.score);
  if mq_integer_between(score_number, 0, mq_max_timestamp) then
    items[idx] = {candidate.mid, candidate.score};
  else
    items[idx] = {candidate.mid, false};
  end
end

return {
  tostring(now), more and '1' or '0', next_score,
  next_same_score_count, next_mid, items
};
