local curr_val = redis.call('hget', _:k, _:field);

if (_:old-val-?sha ~= '') then
   -- Check current value's SHA1
   local curr_sha = redis.sha1hex(curr_val);
   if (curr_sha == _:old-val-?sha) then
      redis.call('hset', _:k, _:field, _:new-val);
      return true; -- 1
   else
      return false; -- nil (was 0)
   end
else
   -- Check current value
   if (curr_val == _:old-?val) then
      redis.call('hset', _:k, _:field, _:new-val);
      return true; -- 1
   else
      return false; -- nil (was 0)
   end
end
