local get_val = redis.call('get', _:k);
if (get_val == _:old-val) then
   redis.call('set', _:k, _:new-val);
   return 1;
else
   return 0;
end
