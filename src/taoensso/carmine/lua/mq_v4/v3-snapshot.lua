-- V3 MIDs and payloads are arbitrary stored byte strings. The caller
-- reads this entire reply in RESP raw-bulk mode and explicitly decodes each
-- value exactly once. Keep every join here, while MIDs are still identical Lua
-- strings, so JVM byte-array identity and Carmine's legacy issue-83 heuristic
-- can never affect role correlation.
--
-- Reply (schema version 2):
-- {2,
--  {one fixed-width record per base message, e/o:
--   {raw_mid, raw_msg,
--    has_messages_rq,
--    has_lock,      raw_lock_expiry,
--    has_backoff,   raw_backoff_expiry,
--    has_nattempts, raw_nattempts,
--    has_udt,       raw_udt,
--    is_done, is_requeue}},
--  {one fixed-width record per unmatched auxiliary MID, e/o:
--   {raw_mid, has_messages_rq, has_lock, has_backoff, has_nattempts,
--    has_udt, is_done, is_requeue}}}
--
-- Missing optional values use Lua false so RESP null placeholders cannot
-- shorten or shift a record. Both collections are sorted by the exact raw
-- Redis field/member, providing a content-stable ordinal without exposing the
-- packed MID in public reports. Orphan message payloads are intentionally never
-- read or returned.

local base_kvs = redis.call('hgetall', _:qk-messages);
local base = {};
local base_mids = {};

for i = 1, #base_kvs, 2 do
  local mid = base_kvs[i];
  base[#base + 1] = {mid, base_kvs[i + 1]};
  base_mids[mid] = true;
end
base_kvs = nil;

table.sort(base, function(a, b) return a[1] < b[1]; end);

for i = 1, #base do
  local mid = base[i][1];
  local msg = base[i][2];
  local lock = redis.call('hget', _:qk-locks, mid);
  local backoff = redis.call('hget', _:qk-backoffs, mid);
  local nattempts = redis.call('hget', _:qk-nattempts, mid);
  local udt = redis.call('hget', _:qk-udts, mid);

  base[i] = {
    mid, msg,
    redis.call('hexists', _:qk-messages-rq, mid),
    lock and 1 or 0, lock,
    backoff and 1 or 0, backoff,
    nattempts and 1 or 0, nattempts,
    udt and 1 or 0, udt,
    redis.call('sismember', _:qk-done, mid),
    redis.call('sismember', _:qk-requeue, mid)
  };
end

local orphan_by_mid = {};
local orphans = {};

-- Role slots after raw_mid:
-- messages-rq=2, locks=3, backoffs=4, nattempts=5, udts=6, done=7, requeue=8.
local add_orphan_role = function(mid, slot)
  if base_mids[mid] then return; end
  local orphan = orphan_by_mid[mid];
  if not orphan then
    orphan = {mid, 0, 0, 0, 0, 0, 0, 0};
    orphan_by_mid[mid] = orphan;
    orphans[#orphans + 1] = orphan;
  end
  orphan[slot] = 1;
end

local add_hash_orphans = function(key, slot)
  local mids = redis.call('hkeys', key);
  for i = 1, #mids do add_orphan_role(mids[i], slot); end
end

local add_set_orphans = function(key, slot)
  local mids = redis.call('smembers', key);
  for i = 1, #mids do add_orphan_role(mids[i], slot); end
end

add_hash_orphans(_:qk-messages-rq, 2);
add_hash_orphans(_:qk-locks,       3);
add_hash_orphans(_:qk-backoffs,    4);
add_hash_orphans(_:qk-nattempts,   5);
add_hash_orphans(_:qk-udts,        6);
add_set_orphans (_:qk-done,        7);
add_set_orphans (_:qk-requeue,     8);

table.sort(orphans, function(a, b) return a[1] < b[1]; end);

return {2, base, orphans};
