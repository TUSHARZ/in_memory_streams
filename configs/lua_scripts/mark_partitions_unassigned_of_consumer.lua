-- local myTable={}
-- local index=1
-- local popped = redis.call('SPOP', KEYS[1], redis.call('SCARD', KEYS[1]))
-- for k, value in ipairs(popped) do
--     myTable[index] = value
--     index = index+1
--     myTable[index] = 'unassigned'
--     index = index+1
-- end
-- redis.call('HSET', KEYS[2], unpack(myTable))
-- redis.call('SPOP', KEYS[3], KEYS[1])

-- This script removes all partition_ids from a consumer, mark them unassigned and remove consumer member from group
-- KEYS : [CONSUMER_ASSIGNED_PARTITIONS::{consumer_member_id}, CONSUMER_GROUP_PARTITIONS::{group_name}, CONSUMER_GROUP::{group_name}, consumer_member_id]
local myTable={}
local index=1
local popped = redis.call('SPOP', KEYS[1], redis.call('SCARD', KEYS[1]))
for k, value in ipairs(popped) do
    myTable[index] = value
    index = index+1
    myTable[index] = 'unassigned'
    index = index+1
end
redis.call('HSET', KEYS[2], unpack(myTable)) redis.call('SREM', KEYS[3], KEYS[4])