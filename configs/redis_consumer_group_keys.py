class RedisConsumerGroupKeys:
    """
    Provides keys for redis consumer group coordinator
    """

    @classmethod
    def get_partitions_assigned_to_consumers_key(cls, consumer_member_id):
        # assigned topics to the consumers
        return f'CONSUMER_ASSIGNED_PARTITIONS::{consumer_member_id}'

    @classmethod
    def get_consumer_group_key(cls, group_name):
        # assigned consumers to the group
        return f'CONSUMER_GROUP::{group_name}'

    @classmethod
    def get_consumer_group_partitions_key(cls, group_name):
        # overall partitions in a group
        return f'CONSUMER_GROUP_PARTITIONS::{group_name}'

    @classmethod
    def get_group_coordinator_health_key(cls, group_name):
        # overall partitions in a group
        return f'{group_name}::health'
