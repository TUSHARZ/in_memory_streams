class GroupCoordinatorClient:
    """
    This class acts as communication intermediary between consumer and consumerGroupCoordinator
    """

    group_coordinator = {}
    consumers = {}

    @classmethod
    def join_group_request(cls, group_name, consumer_id, consumer):
        """

        :param group_name:
        :param consumer_id:
        :param consumer:
        :return:
        """
        cls.consumers[consumer.id] = consumer
        cls.group_coordinator[group_name].register_consumer_member(consumer_id)

    @classmethod
    def get_assigned_partitions(cls, group_name, consumer_id):
        return cls.group_coordinator[group_name].get_assigned_partitions(consumer_id)

    @classmethod
    def register_group_coordinator(cls, name, group_coordinator):
        cls.group_coordinator[name] = group_coordinator

    @classmethod
    def notify_remove_partition(cls, consumer_id, topic_partition_ids):
        return cls.consumers[consumer_id].notify_partition_removed(topic_partition_ids=topic_partition_ids)

    @classmethod
    def revoke_partition(cls, group_name, partition_ids, consumer_id):
        cls.group_coordinator[group_name].mark_partitions_unassigned(consumer_id, partition_ids)

    @classmethod
    def send_heart_beat(cls, group_name, consumer_id):
        cls.group_coordinator[group_name].ping_heart_beat(consumer_id=consumer_id)

    @classmethod
    def stop_consumer(cls, consumer_id):
        cls.consumers[consumer_id].stop_consumer_gracefully()

    @classmethod
    def topic_partitions_changed(cls, group_name, topic_name):
        cls.group_coordinator[group_name].notify_partitions_changed(topic_name)

    @classmethod
    def get_consumer_group_offset(cls, group_name, topic_name, partition):
        return cls.group_coordinator[group_name].get_offset(topic_name, partition)

    @classmethod
    def commit_consumer_group_offset(cls, group_name, topic_name, partition, value):
        return cls.group_coordinator[group_name].commit_offset(topic_name, partition,value)

# class RedisGroupCoordinatorClient(InMemoryGroupCoordinatorClient):
#
#     group_coordinator = {}
#     consumers = {}
#
#     @classmethod
#     def join_group_request(cls, group_name, consumer_id):
#         cls.group_coordinator[group_name].register_consumer_member(consumer_id)
#
#     @classmethod
#     def get_assigned_partitions(cls, group_name, consumer_id):
#         return cls.group_coordinator[group_name].get_assigned_partitions(consumer_id)
#
#     @classmethod
#     def register_group_coordinator(cls, name, group_coordinator):
#         cls.group_coordinator[name]=group_coordinator
#
#     @classmethod
#     def notify_remove_partition(cls, consumer_id, topic_partition_id):
#         return cls.consumers[consumer_id].notify_partition_removed(topic_partition_id)
#
#     @classmethod
#     def revoke_partition(cls, group_name, partition_id,consumer_id):
#         cls.group_coordinator[group_name].mark_partition_unassigned(consumer_id,partition_id)
#
#     @classmethod
#     def send_heart_beat(cls, group_name, consumer_id):
#         cls.group_coordinator[group_name].ping_heart_beat(consumer_id=consumer_id)
#
#     @classmethod
#     def stop_consumer(cls, consumer_id):
#         cls.consumers[consumer_id].stop_consumer_gracefully()
#
#
