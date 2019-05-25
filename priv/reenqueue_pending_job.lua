local group = ARGV[1]
local id = ARGV[2]
local min_idle = ARGV[3]
local stream = KEYS[1]

local result = redis.call('XCLAIM', stream, group, "reclaimer", min_idle, id)
-- [
   -- [
     -- "1557629755682-0",
     -- ["job", "{\"jid\":\"123\",\"enqueued_at\":42,\"queue\":\"test_queue\"}"]
   -- ]
 -- ]
-- ]

if result then
  redis.call('XADD', stream, '*', 'job', result[1][2][2])
  redis.call('XACK', stream, group, id)
  redis.call('XDEL', stream, id)

  return 1
else
  return 0
end
