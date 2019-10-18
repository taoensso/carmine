local curr_val = redis.call('get', _:k);

local do_swap = false;
if (_:old-?sha ~= '') then
   local curr_sha = redis.sha1hex(curr_val);
   if (curr_sha == _:old-?sha) then do_swap = true; end
else
   if (curr_val == _:old-?val) then do_swap = true; end
end

if do_swap then
   if (_:delete == '1') then
      redis.call('del', _:k);
   else
      redis.call('set', _:k, _:new-val);
   end
   return 1;
else
   return 0;
end
