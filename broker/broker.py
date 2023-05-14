from broker.partition_strategy.base_partition_strategy import TopicDataPartitionStrategy
from broker.topic import Topic, LLStorage


class Broker:
    from collections import defaultdict
    topics = {}
    topics_subscription = defaultdict(set)

    @classmethod
    def create_topic(cls, topic_name, partitions, strategy: TopicDataPartitionStrategy, storage_type=LLStorage):
        """
        Create new topic
        :param storage_type: storage class to use
        :param topic_name: Topic name
        :param partitions: Number of partitions
        :param strategy: Data Partition Strategy
        :return:
        """
        if cls.topics.get(topic_name):
            raise Exception("topic already exists")
        cls.topics[topic_name] = Topic(topic_name, partitions, strategy, storage_type=storage_type)

    @classmethod
    def add_data_to_topic(cls, topic_name, data):
        cls.topics[topic_name].push_data(data)

    @classmethod
    def poll_data(cls, topic_name, partition, offset, limit):
        return cls.topics[topic_name].poll_data(partition, offset, limit)

    @classmethod
    def attach_to_topic(cls, topic_name, consumer_group, **kwargs):
        cls.topics_subscription[topic_name].add(consumer_group)
        return cls.get_no_of_topic_partitions(topic_name)

    @classmethod
    def get_no_of_topic_partitions(cls, topic_name):
        return len(cls.topics[topic_name].partitions)

    @classmethod
    def add_partition(cls, topic_name):
        cls.topics[topic_name].add_partition()
        cls.notify_subscribers(topic_name)

    @classmethod
    def add_subscription(cls, topic_name, consumer_group):
        cls.topics_subscription[topic_name].add(consumer_group)

    @classmethod
    def remove_subscription(cls, topic_name, consumer_group):
        cls.topics_subscription[topic_name].remove(consumer_group)

    @classmethod
    def notify_subscribers(cls, topic_name):
        from consumer.group_coordinator_client import GroupCoordinatorClient
        for consumer_group in cls.topics_subscription[topic_name]:
            GroupCoordinatorClient.topic_partitions_changed(consumer_group, topic_name)
