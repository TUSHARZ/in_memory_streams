from broker.broker import Broker
from broker.partition_strategy.base_partition_strategy import TopicDataPartitionStrategy
from broker.topic import Data


class RedisTopicClient(Broker):
    from configs.redis_client import RedisClient
    redis_client = RedisClient()

    @classmethod
    def get_topic_partition_key(cls, topic_name, partition_id):
        """
        Stores data for a particular topic and partition
        :param topic_name:
        :param partition_id:
        :return:
        """
        return f'{topic_name}::{partition_id}'

    @classmethod
    def get_topic_metadata_key(cls, topic_name):
        """
        stores topic metadata , like name,no_of_partitions,partition_strategy etc
        :param topic_name:
        :return:
        """
        return f'{topic_name}'

    @classmethod
    def get_topic_metadata(cls, topic_name):
        return cls.redis_client.get_hash(cls.get_topic_metadata_key(topic_name))

    @classmethod
    def attach_to_topic(cls, topic_name, consumer_group, **kwargs):
        """
        Add partition change sub to consumer group and return the partitions len
        :param topic_name:
        :param consumer_group:
        :param kwargs:
        :return:
        """
        # add channel sub , in case partitions for this topic changes, we will notify consumer group
        cls.redis_client.add_channel_sub(f'{cls.get_topic_metadata_key(topic_name)}_partition_change',
                                         kwargs.get("change_detect_func"))

        return cls.get_no_of_topic_partitions(topic_name)

    @classmethod
    def create_topic(cls, topic_name, partitions, strategy):
        """
        Create a topic in redis. Topic metadata like no of partitions, name , strategy all is saved in metadata key
        :param topic_name:
        :param partitions:
        :param strategy:
        :return:
        """
        cls.redis_client.add_multiple_to_hash(cls.get_topic_metadata_key(topic_name),
                                              {"topic_name": topic_name, "partitions": partitions,
                                               "strategy": strategy})

    @classmethod
    def get_no_of_topic_partitions(cls, topic_name):
        """
        get no of partitions from topic metadata
        :param topic_name:
        :return:
        """
        return int(cls.redis_client.get_hash_key(topic_name, "partitions"))

    @classmethod
    def add_data_to_topic(cls, topic_name, data, data_class=Data):
        """
        add data to topic. here we will use strategy metadata to determine the partition first , in which data needs
        to be added and then add the data there and also update strategy metadata.(this is required , as in case of
        Round robit data partition strategy we need to update last partition used)
        :param topic_name:
        :param data: data object
        :param data_class: :return: data class to use
        """
        # get metadata
        metadata = cls.redis_client.get_hash(cls.get_topic_metadata_key(topic_name))

        # make partition strategy object and get the next partition
        partition_strategy = TopicDataPartitionStrategy.get_partition_strategy_class(metadata.get('strategy')). \
            deserialize(metadata.get('strategy_metadata', "{}"))
        next_partition = partition_strategy.get_next_partition(int(metadata['partitions']))

        # insert data into topic and update metadata
        cls.redis_client.call_lua("add_data_to_topic", [cls.get_topic_partition_key(topic_name, next_partition),
                                                        cls.get_topic_metadata_key(topic_name),
                                                        "strategy_metadata", partition_strategy.serialize()],
                                  [data_class.serialize(data)])

    @classmethod
    def poll_data(cls, topic_name, partition, limit, offset, data_class=Data):
        """
        get range data from Redis list. use data class for deserialize
        :param topic_name:
        :param partition:
        :param limit:
        :param offset:
        :param data_class:
        :return:
        """
        return [data_class.deserialize(k) for k in
                cls.redis_client.get_range_list_data(cls.get_topic_partition_key(topic_name, partition), limit, limit +
                                                     offset - 1)]

    @classmethod
    def add_partition(cls, topic_name):
        """
        add partitions to topic metadata and publish the same to the channel , so that attached groups can rebalance.
        :param topic_name:
        :return:
        """
        print("adding_partition")
        cls.redis_client.increment_hash_value(cls.get_topic_metadata_key(topic_name), "partitions", 1)

        # publish to channel to notify consumer groups consuming from topic
        cls.redis_client.publish_to_channel(f'{cls.get_topic_metadata_key(topic_name)}_partition_change', topic_name)
