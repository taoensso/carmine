local removed = redis.call('del',
  _:qk-seq,
  _:qk-payloads,
  _:qk-meta,
  _:qk-successor-payloads,
  _:qk-successor-meta,
  _:qk-ready-high,
  _:qk-ready-normal,
  _:qk-ready-low,
  _:qk-scheduled,
  _:qk-leased,
  _:qk-lease-tokens,
  _:qk-dead,
  _:qk-dead-payloads,
  _:qk-failures,
  _:qk-signal);

return {'cleared', tostring(removed)};
