local now_parts = redis.call('time');
local now = (tonumber(now_parts[1]) * 1000) + math.floor(tonumber(now_parts[2]) / 1000);
local cutoff = now - math.max(0, tonumber(_:older-than-ms));
local limit = math.max(1, tonumber(_:limit));
local mids = redis.call('zrangebyscore', _:qk-dead, '-inf', cutoff, 'limit', 0, limit + 1);
local more = #mids > limit;
local removed = math.min(#mids, limit);

for idx = 1, removed do
  local mid = mids[idx];
  redis.call('zrem', _:qk-dead, mid);
  redis.call('hdel', _:qk-failures, mid);
  redis.call('hdel', _:qk-dead-payloads, mid);
end

return {tostring(removed), more and '1' or '0', tostring(now)};
