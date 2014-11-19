local get_val = redis.call('get', _:k);
local get_sha = redis.sha1hex(get_val);
if (get_sha == _:old-val-sha) then
   redis.call('set', _:k, _:new-val);
   return 1;
else
   return 0;
end
