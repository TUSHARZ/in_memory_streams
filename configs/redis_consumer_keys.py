class RedisConsumerKeys:
    """
    Provides redis consumer keys
    """

    @classmethod
    def get_consumer_health_key(cls, consumer_member_id):
        """
        consumer heart beat will be set on this keys
        :param consumer_member_id:
        :return:
        """
        return f'health::{consumer_member_id}'
