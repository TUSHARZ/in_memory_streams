-- This script adds data to a topic partition and update strategy_metadata as well
-- KEYS : [{topic_name}::{partition_id}, {topic_name}, strategy_metadata, strategy_metadata_value]
-- ARGS : [topic data]
redis.call('RPUSH',KEYS[1],ARGV[1])
redis.call('HSET', KEYS[2], KEYS[3], KEYS[4])