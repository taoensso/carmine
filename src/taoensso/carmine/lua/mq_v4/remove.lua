local mid = _:mid;
local existed =
  redis.call('hexists', _:qk-payloads, mid) == 1 or
  redis.call('hexists', _:qk-meta, mid) == 1 or
  redis.call('hexists', _:qk-successor-payloads, mid) == 1 or
  redis.call('hexists', _:qk-successor-meta, mid) == 1 or
  redis.call('hexists', _:qk-dead-payloads, mid) == 1 or
  redis.call('hexists', _:qk-lease-tokens, mid) == 1 or
  redis.call('hexists', _:qk-failures, mid) == 1 or
  redis.call('zscore', _:qk-ready-high, mid) ~= false or
  redis.call('zscore', _:qk-ready-normal, mid) ~= false or
  redis.call('zscore', _:qk-ready-low, mid) ~= false or
  redis.call('zscore', _:qk-scheduled, mid) ~= false or
  redis.call('zscore', _:qk-leased, mid) ~= false or
  redis.call('zscore', _:qk-dead, mid) ~= false;
if not existed then return {'absent'}; end
redis.call('zrem', _:qk-ready-high, mid);
redis.call('zrem', _:qk-ready-normal, mid);
redis.call('zrem', _:qk-ready-low, mid);
redis.call('zrem', _:qk-scheduled, mid);
redis.call('zrem', _:qk-leased, mid);
redis.call('zrem', _:qk-dead, mid);
redis.call('hdel', _:qk-lease-tokens, mid);
redis.call('hdel', _:qk-failures, mid);
redis.call('hdel', _:qk-payloads, mid);
redis.call('hdel', _:qk-meta, mid);
redis.call('hdel', _:qk-successor-payloads, mid);
redis.call('hdel', _:qk-successor-meta, mid);
redis.call('hdel', _:qk-dead-payloads, mid);
return {'removed'};
