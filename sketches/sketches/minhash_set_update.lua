local count = redis.call('ZCARD', KEYS[1]);
if count == tonumber(ARGV[1]) then
  local max_score = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')[2]
  if tonumber(ARGV[2]) >= tonumber(max_score) then
    return
  else
    redis.call('ZREMRANGEBYRANK', KEYS[1], -1, -1)
  end
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[2]), tonumber(ARGV[3]))