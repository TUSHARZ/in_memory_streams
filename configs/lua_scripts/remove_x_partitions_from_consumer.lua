-- local myTable={}
-- local index=1
-- local partitions_lua_table = {}
-- for value in string.gmatch(KEYS[2],'([^,]+)') do
--     partitions_lua_table[index] = value
--     index = index+1
-- end
--
-- index=1
-- local popped = redis.call('SREM', KEYS[1], unpack(partitions_lua_table))
-- for value in string.gmatch(KEYS[2],'([^,]+)') do
--     myTable[index] = value
--     index = index+1
--     myTable[index] = 'unassigned'
--     index = index+1
--     end
--

-- This script removes x number of partitions from consumer and mark them unassigned
-- KEYS : [CONSUMER_ASSIGNED_PARTITIONS::{consumer_member_id}, CONSUMER_GROUP_PARTITIONS::{group_name}]
-- ARGS : [partition_ids to remove]
local members = ARGV
redis.call('SREM', KEYS[1], unpack(members))
local lua_table={}
local index=1
for ix,value in pairs(members) do
    lua_table[index] = value
    index = index+1
    lua_table[index] = 'unassigned'
    index = index+1
    end
redis.call('HSET', KEYS[2], unpack(lua_table))
return 1

-- local myTable={} local index=1 local partitions_lua_table = {} for value in string.gmatch(KEYS[2],'([^,]+)') do partitions_lua_table[index] = value index = index+1 end index=1 local popped = redis.call('SPOP', KEYS[1], unpack(partitions_lua_table)) for value in string.gmatch(KEYS[2],'([^,]+)') do myTable[index] = value index = index+1 myTable[index] = 'unassigned' index = index+1 end redis.call('HSET', KEYS[3], unpack(myTable))


-- local index=1 local partitions_lua_table = {} for value in string.gmatch(KEYS[1],'([^,]+)') do partitions_lua_table[index] = value index = index+1 end return partitions_lua_table
--
-- local myTable={} local index=1 local partitions_lua_table = {} for value in string.gmatch(KEYS[2],'([^,]+)') do partitions_lua_table[index] = value index = index+1 end index=1 local popped = redis.call('SREM', KEYS[1], unpack(partitions_lua_table))