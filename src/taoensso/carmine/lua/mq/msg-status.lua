local mid = _:mid;
local t   = redis.call('TIME'); -- Server clock, avoids client clock skew
local now = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000); -- Epoch msecs

--[[include:msg-status-fn]]
local status, is_bo, is_rq = msg_status(mid, now);

return {status, is_bo, is_rq};
