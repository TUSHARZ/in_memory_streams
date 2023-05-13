from configs.redis_client import RedisClient
from configs.redis_consumer_group_keys import RedisConsumerGroupKeys


class RedisOffsetClient:
    """
    Interacts with redis to save offsets
    """

    redis_client = RedisClient()

    @classmethod
    def get_offset_key(cls, consumer_group_name, topic_name , partition_id):
        return f'{consumer_group_name}_{topic_name}::{partition_id}'

    @classmethod
    def commit_offset(cls, consumer_group, topic_name, partition, offset):
        # check group leader health
        if not cls.redis_client.check_key_exists(RedisConsumerGroupKeys.get_group_coordinator_health_key(group_name=consumer_group)):
            print("unable to commit, as group coordinator is down")
            return False
        cls.redis_client.set_key(cls.get_offset_key(consumer_group, topic_name, partition), offset)
        return True

    @classmethod
    def get_offset(cls, consumer_group, topic_name, partition):
        # check group leader health
        if not cls.redis_client.check_key_exists(RedisConsumerGroupKeys.get_group_coordinator_health_key(group_name=consumer_group)):
            print("unable to get offset as group coordinator is down")
            return None

        # check if existing offset is there, if not return 0
        if not cls.redis_client.check_key_exists(cls.get_offset_key(consumer_group, topic_name, partition)):
            return 0
        off = cls.redis_client.get_key(cls.get_offset_key(consumer_group, topic_name, partition))
        return int(off)

