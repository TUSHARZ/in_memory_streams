import enum
import time
from threading import Lock
from collections import defaultdict

from configs.redis_offset_client import RedisOffsetClient
# from configs.redis_consumer_helper import RedisConsumerHelper
from partition_strategy.equal_assignment import PartitionAssignmentStrategy
from broker.topic import *
from broker.broker import Broker
from broker.redis_topic import RedisTopicClient
from consumer.group_coordinator_client import GroupCoordinatorClient
import random
from offset_manager.offset_manager import OffsetClient


class PartitionStatus(enum.Enum):
    UNASSIGNED = 'unassigned'
    ASSIGNED = 'assigned'


class PartitionTopic:

    def __init__(self, topic_name, partition):
        self.topic_name = topic_name
        self.partition = partition

    def __str__(self):
        return f'{self.topic_name}::{self.partition}'

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__str__() == other.__str__()
        return False


class GroupCoordinator:
    """
    Manages consumers assigned to the group
    constantly monitors the consumers and reassign if anything is killed
    Rebalance partitions between consumers attached to group
    """

    consumer_nodes = set()  # consumer ids of consumer nodes assigned to the group
    consumer_nodes_mapping = defaultdict(set)  # partition mappings assigned to consumer nodes
    partitions = {}  # overall topic_partitions assigned to the group
    heart_beat = {}  # heart beat tracker of all consumers

    def __init__(self, name, partition_assignment_strategy='EqualAssignmentStrategy', is_leader=False):
        """
        :param name: Consumer group name
        :param partition_assignment_strategy: Partition assignment strategy
        :param is_leader : Specify if the group coordinator is leader or not, only leader can rebalance partitions to
        avoid race conditions.
        """
        self.name = name
        self.rebalance_lock = Lock()  # this lock will help prevent race conditions during partition rebalances
        self.partition_assignment_strategy = PartitionAssignmentStrategy.get_partition_strategy(
            partition_assignment_strategy)
        self.broker_client = Broker
        self.is_leader = is_leader
        self.offsetClient = OffsetClient

        GroupCoordinatorClient.register_group_coordinator(name, self)

        if not self.is_leader:
            return

        # start check on unassigned partitions
        t1 = threading.Thread(target=self.check_unassigned_partitions, daemon=True)
        t1.name = "unassigned_partition"
        t1.start()

        # start check on consumer group health
        t2 = threading.Thread(target=self.check_consumer_health, daemon=True)
        t2.name = "consumer_healths"
        t2.start()

    def notify_consumer_members_changed(self, *args, **kwargs):
        """
        call this method if consumer members are changed.
        :param args:
        :param kwargs:
        :return:
        """
        self.rebalance_partitions()

    def register_consumer_member(self, consumer_id):
        """
        register consumer member to the group
        :param consumer_id:
        :return:
        """
        print(f"registering_consumer_member {consumer_id}")
        self.consumer_nodes.add(consumer_id)
        self.notify_consumer_members_changed()

    def mark_partitions_unassigned(self, consumer_id, topic_partition_ids):
        """
        Remove partitions from consumer and mark it unassigned
        :param topic_partition_ids: partitions to remove
        :param consumer_id:: consumer id to remove partitions from
        :return:
        """
        print(f"removing_partition_from {consumer_id} {topic_partition_ids}")
        with self.rebalance_lock:
            for topic_partition_id in topic_partition_ids:
                self.consumer_nodes_mapping[consumer_id].remove(topic_partition_id)
                self.partitions[topic_partition_id] = PartitionStatus.UNASSIGNED.value

    @classmethod
    def get_assigned_partitions(cls, consumer_id):
        """
        get assigned partitions for a consumer member
        :param consumer_id:
        :return:
        """
        return cls.consumer_nodes_mapping[consumer_id]

    def print_curr_assignment(self):
        for cnsr_id in self.consumer_nodes:
            for p in self.consumer_nodes_mapping[cnsr_id]:
                print(f'{p} in {cnsr_id}')

        # print([[f'{p.__str__()} in {cnsr_id}\n' for p in self.consumer_nodes_mapping[cnsr_id]] for cnsr_id in
        #        self.consumer_nodes])

    def add_partition_mapping(self, consumer_member_id, topic_partition):
        """
        Add partition mapping to consumer and mark it assigned.
        Here we can directly add the partition, without notifying the consumer , since consumer is getting updated
        partitions in every poll request, so this new partition will be returned in next get_partitions_assigned request.
        :param consumer_member_id:
        :param topic_partition:
        :return:
        """
        self.partitions[topic_partition] = PartitionStatus.ASSIGNED.value
        self.consumer_nodes_mapping[consumer_member_id].add(topic_partition)

    def notify_partitions_changed(self, topic_name):
        """
        Listener in case partitions in topic is changed, to assign new partitions to the nodes
        """
        for partition_id in range(self.broker_client.get_no_of_topic_partitions(topic_name)):
            curr = PartitionTopic(topic_name, partition_id)
            if curr not in self.partitions:
                self.partitions[curr] = PartitionStatus.UNASSIGNED.value
        self.rebalance_partitions()

    @classmethod
    def get_unassigned_partitions(cls):
        """
        Get unassigned partitions in the group
        :return:
        """

        return set([topic_partition_id for topic_partition_id, status in cls.partitions.items() if
                    status == PartitionStatus.UNASSIGNED.value])

    def check_unassigned_partitions(self):
        """
        Check unassigned partitions and trigger a re-balance if found any.
        Note: consumer nodes may not have any partition , if no of consumers > no of partitions
        :return:
        """
        while True:
            with self.rebalance_lock:
                unassigned = self.get_unassigned_partitions()
            if unassigned:
                print(f"Found unassigned partitions, {unassigned}")
                self.rebalance_partitions()
            time.sleep(5)

    def get_consumer_nodes_in_group(self):
        return self.consumer_nodes

    def partitions_len_in_group(self):
        return len(self.partitions)

    def notify_remove_partition(self, consumer_id, partition_ids):
        """
        Notify consumer member to remove partition.
        Note: We cannot just blindly remove a partition from consumer, since it might be that consumer is still processing
        data from that partition and has not committed offset yet. So in this case if we remove the partition and assign
        to another one, same data can be processed multiple times.
        :param consumer_id:
        :param partition_ids:
        :return:
        """
        GroupCoordinatorClient.notify_remove_partition(consumer_id,
                                                       partition_ids)

    def rebalance_partitions(self):
        """
        Re-balances the partitions between all assigned consumers
        """
        if not self.is_leader:
            print("not a leader")
            return
        if not self.partitions_len_in_group():
            return
        print("REBALANCING")
        self.rebalance_lock.acquire()
        members = self.get_consumer_nodes_in_group()
        if not members:
            print("no consumer member attached to group")
            # return
        partitions_assignment = self.partition_assignment_strategy(members).get_partitions(
            self.partitions_len_in_group())
        print(f'Partitions assignments should be {partitions_assignment}')

        for consumer_id, no_of_partitions in partitions_assignment.items():
            curr_assigned_partitions_to_node = self.get_assigned_partitions(consumer_id)
            _len_curr_assigned_partitions_to_node = len(curr_assigned_partitions_to_node)
            print(f"curr_assigned_partition_to_{consumer_id}_are_{curr_assigned_partitions_to_node}")

            # If consumer has less than required partitions, assign any unassigned partition to the consumer
            while _len_curr_assigned_partitions_to_node < no_of_partitions:
                unassigned_partitions = self.get_unassigned_partitions()
                if not unassigned_partitions:
                    print("No unassigned partitions left")
                    break
                print(f"unassigned_partitions_are_{unassigned_partitions}")
                curr_unassigned_partition = unassigned_partitions.pop()
                self.add_partition_mapping(consumer_id, curr_unassigned_partition)
                _len_curr_assigned_partitions_to_node += 1

            # If consumer has more than required partitions, remove the extra partitions so they can be distributed
            # accordingly
            while _len_curr_assigned_partitions_to_node > no_of_partitions:
                print(f"removing extra partitions from {consumer_id}")
                no_of_partitions_to_remove = _len_curr_assigned_partitions_to_node - no_of_partitions
                partitions_to_remove = random.sample(list(self.get_assigned_partitions(consumer_id)),
                                                     no_of_partitions_to_remove)
                threading.Thread(target=self.notify_remove_partition, args=(consumer_id, partitions_to_remove)).start()
                _len_curr_assigned_partitions_to_node -= no_of_partitions_to_remove

        self.rebalance_lock.release()
        self.print_curr_assignment()
        print("Rebalancing Completed")

    def notify_consumer_member_removed(self, consumer_member_id):
        """
        remove_consumer_member from the group ie. mark it partitions unassigned and delete mappings
        consumer_member_id : member id to remove
        """
        with self.rebalance_lock:
            for topic_partition_id in self.consumer_nodes_mapping[consumer_member_id]:
                self.partitions[topic_partition_id] = PartitionStatus.UNASSIGNED.value
            del self.consumer_nodes_mapping[consumer_member_id]
            self.consumer_nodes.remove(consumer_member_id)

    def attach_to_topic(self, topic_name):
        """
        Get topics of partitions and mark them unassigned so that rebalance can assign them to relevant consumers
        :param topic_name:
        :return:
        """
        if not self.is_leader:
            print("Not a leader")
            return
        print(f"attaching to topic {topic_name}")
        with self.rebalance_lock:
            for partition_id in range(self.broker_client.attach_to_topic(topic_name, self.name)):
                topic_partition = PartitionTopic(topic_name, partition_id)
                if topic_partition in self.partitions:
                    continue
                self.partitions[topic_partition] = PartitionStatus.UNASSIGNED.value

        self.rebalance_partitions()

    def check_consumer_health(self):
        """
        Check consumer health from the continuous heart beat they are sending.
        If we do not receive a heart beat for some time, we will assume the consumer to be dead and
        remove it from the group.
        :return:
        """
        while True:
            threshold = 10
            for consumer_node in list(self.consumer_nodes):
                if time.time() - self.heart_beat[consumer_node] > threshold:
                    self.notify_consumer_member_removed(consumer_node)
            time.sleep(3)

    def ping_heart_beat(self, consumer_id):
        """
        Update heart beat of a consumer
        :param consumer_id:
        :return:
        """
        self.heart_beat[consumer_id] = time.time()

    def get_offset(self, topic_name, partition_id):
        """
        get offset for a topic<>partition ID
        :param topic_name:
        :param partition_id:
        :return:
        """
        return self.offsetClient.get_offset(self.name, topic_name, partition_id)

    def commit_offset(self, topic_name, partition_id, value):
        """
        commit offset for topic<>partiton ID
        :param topic_name:
        :param partition_id:
        :param value:
        :return:
        """
        return self.offsetClient.commit_offset(self.name, topic_name, partition_id, value)


from configs.redis_consumer_group_keys import RedisConsumerGroupKeys
from configs.redis_consumer_keys import RedisConsumerKeys


class RedisGroupCoordinatorClient(GroupCoordinator):
    from configs.redis_client import RedisClient

    redis_client = RedisClient()

    def __init__(self, consumer_group_name, partition_assignment_strategy='EqualAssignmentStrategy', is_leader=False):
        print(consumer_group_name, partition_assignment_strategy, is_leader)
        super(RedisGroupCoordinatorClient, self).__init__(consumer_group_name, partition_assignment_strategy, is_leader)
        self.offsetClient = RedisOffsetClient
        if not self.is_leader:
            return
        self.redis_client.add_sub(RedisConsumerGroupKeys.get_consumer_group_key(self.name),
                                  self.notify_consumer_members_changed)
        # print(self.offsetClient.__name__)
        threading.Thread(target=self.group_coordinator_health, daemon=True).start()

    def notify_consumer_members_changed(self, *args, **kwargs):
        print("consumer_member_changed")
        self.rebalance_partitions()

    @classmethod
    def get_assigned_partitions(cls, consumer_member_id):
        """
        Get assigned partitions for a consumer from redis
        :param consumer_member_id:
        :return:
        """
        partitions = cls.redis_client.get_set_members(RedisConsumerGroupKeys.get_partitions_assigned_to_consumers_key(
            consumer_member_id))
        return partitions

    def register_consumer_member(self, consumer_member_id):
        """
        Register consumer member to the group
        :param consumer_member_id:
        :return:
        """
        return self.redis_client.add_to_set(RedisConsumerGroupKeys.get_consumer_group_key(self.name),
                                            consumer_member_id)

    def notify_partitions_changed(self, topic_name):
        """
        Listener in case partitions in topic is changed, to assign new partitions to the nodes
        """
        for i in range(RedisTopicClient.get_no_of_topic_partitions(topic_name)):
            topic_partition = PartitionTopic(topic_name, i)

            # add new partitions in group in unassigned group.
            self.redis_client.add_to_hash(RedisConsumerGroupKeys.get_consumer_group_partitions_key(self.name),
                                          topic_partition.__str__(),
                                          'unassigned', method='hsetnx')
        self.rebalance_partitions()

    def partition_change_detect_func(self, *args):
        """
        this function will be called when partitions of any topic on which group is subscribed to changes, so that it can
        accommodate new partitions and re-balance
        :param args:
        :return:
        """
        topic_name = args[0].get('data')
        print(f"Topic partition changed {topic_name}")
        self.notify_partitions_changed(topic_name)

    def get_consumer_nodes_in_group(self):
        """
        Get consumer nodes in the group
        :return:
        """
        members = self.redis_client.get_set_members(RedisConsumerGroupKeys.get_consumer_group_key(self.name))
        return members

    def mark_partitions_unassigned(self, consumer_id, topic_partition_ids):
        """
        Remove multiple partitions from consumer and mark them unassigned
        :param consumer_id:
        :param topic_partition_ids:
        :return:
        """
        print(f"removing partitions from {consumer_id} {topic_partition_ids}")
        self.redis_client.call_lua('remove_x_partitions_from_consumer',
                                   keys=[RedisConsumerGroupKeys.get_partitions_assigned_to_consumers_key(consumer_id),
                                         RedisConsumerGroupKeys.get_consumer_group_partitions_key(self.name)],
                                   args=topic_partition_ids)

        return True

    def get_unassigned_partitions(self):
        """
        Get unassigned partitions of group
        :return:
        """
        all_partitions = self.redis_client.get_hash(RedisConsumerGroupKeys.get_consumer_group_partitions_key(self.name))
        return [k for k, v in all_partitions.items() if v == 'unassigned']

    def check_for_unassigned_partitions(self):
        """
        Keep checking for unassigned partitions , and trigger a re-balance if found
        :return:
        """
        while True:
            unassigned = self.get_unassigned_partitions()
            if unassigned:
                print("found unassigned partitions...")
                self.rebalance_partitions()
            time.sleep(5)

    def add_partition_mapping(self, consumer_member_id, topic_partition):
        """
        add partition mapping for a topic_partition to a consumer and mark it assigned
        :param consumer_member_id:
        :param topic_partition:
        :return:
        """
        return self.redis_client.call_lua("add_partition_mapping",
                                          [RedisConsumerGroupKeys.get_consumer_group_partitions_key(self.name),
                                           RedisConsumerGroupKeys.get_partitions_assigned_to_consumers_key(
                                               consumer_member_id),
                                           topic_partition], [])

    def notify_remove_partition(self, consumer_id, partition_ids):
        """
        Notify consumer to remove partition. It publishes a request on {consumer_id}_remove_partition with the
        partitions to remove Consumer listening on that channel will process the request. :param consumer_id: :param
        partition_ids: :return:
        """
        self.redis_client.publish_to_channel(f"{consumer_id}_remove_partition", ','.join(partition_ids))

    def ping_heart_beat(self, consumer_id):
        """
        update heart beat of consumer
        :param consumer_id:
        :return:
        """
        return self.redis_client.set_key(RedisConsumerKeys.get_consumer_health_key(consumer_id), "alive", 10)

    def partitions_len_in_group(self):
        """
        Get no of partitions in the group
        :return:
        """
        return self.redis_client.get_hash_len(RedisConsumerGroupKeys.get_consumer_group_partitions_key(self.name))

    def attach_to_topic(self, topic_name):
        for k in range(RedisTopicClient.attach_to_topic(topic_name, self.name, **{"change_detect_func":
                                                                                      self.partition_change_detect_func})):
            topic_partition = PartitionTopic(topic_name, k)
            self.redis_client.add_to_hash(RedisConsumerGroupKeys.get_consumer_group_partitions_key(self.name),
                                          topic_partition.__str__(), PartitionStatus.UNASSIGNED.value, method='hsetnx')

        self.rebalance_partitions()

    def notify_consumer_member_removed(self, consumer_member_id, send_stop_sig=False):
        """
        Remove consumer member from the group. ie mark all it's assigned partitions unassigned and remove from consumer
        members list
        :param consumer_member_id:
        :param send_stop_sig:
        :return:
        """
        print(f"removing the consumer --- {consumer_member_id}")
        return self.redis_client.call_lua("mark_partitions_unassigned_of_consumer",
                                          [RedisConsumerGroupKeys.get_partitions_assigned_to_consumers_key(
                                              consumer_member_id),
                                              RedisConsumerGroupKeys.get_consumer_group_partitions_key(self.name),
                                              RedisConsumerGroupKeys.get_consumer_group_key(self.name),
                                              consumer_member_id
                                          ], [])

    def check_consumer_health(self):
        """
        Monitors the state of consumers
        """
        while True:
            for cid in list(
                    self.redis_client.get_set_members(RedisConsumerGroupKeys.get_consumer_group_key(self.name))):
                if not self.redis_client.check_key_exists(RedisConsumerKeys.get_consumer_health_key(cid)):
                    with self.rebalance_lock:
                        print("consumer not alive , removing from group")
                        self.notify_consumer_member_removed(cid)
            time.sleep(2)

    def group_coordinator_health(self):
        while True:
            self.redis_client.set_key(RedisConsumerGroupKeys.get_group_coordinator_health_key(self.name), "alive", 10)
