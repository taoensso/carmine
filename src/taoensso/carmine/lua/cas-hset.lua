local curr_val = redis.call('hget', _:k, _:field);

local do_swap = false;
if (_:old-?sha ~= '') then
   local curr_sha = redis.sha1hex(curr_val);
   if (curr_sha == _:old-?sha) then do_swap = true; end
else
   if (curr_val == _:old-?val) then do_swap = true; end
end

if do_swap then
   if (_:delete == '1') then
      redis.call('hdel', _:k, _:field);
   else
      redis.call('hset', _:k, _:field, _:new-val);
   end
   return 1;
else
   return 0;
end
