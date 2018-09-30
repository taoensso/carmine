local hkey = KEYS[1];

if redis.call('exists', hkey) == 0 then
    redis.call('hmset', hkey, unpack(ARGV));
    return 1;
else
   local proceed = true;
   for i,x in ipairs(ARGV) do
      if (i % 2 ~= 0) then -- odd i => `x` is field
	 if redis.call('hexists', hkey, x) == 1 then
	    proceed = false;
	    break;
	 end
      end
   end

   if proceed then
      redis.call('hmset', hkey, unpack(ARGV));
      return 1;
   else
      return 0;
   end
end
