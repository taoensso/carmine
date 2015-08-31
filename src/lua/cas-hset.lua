local curr_val = redis.call('hget', _:k, _:field);

if (_:old-?sha ~= '') then
   local curr_sha = redis.sha1hex(curr_val);
   if (curr_sha == _:old-?sha) then
      redis.call('hset', _:k, _:field, _:new-val);
      return 1;
   else
      return 0;
   end
else
   if (curr_val == _:old-?val) then
      redis.call('hset', _:k, _:field, _:new-val);
      return 1;
   else
      return 0;
   end
end
