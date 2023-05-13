import threading
import time

from broker.broker import Broker
from broker.partition_strategy.RR_partition_strategy import RRPartitionStrategy
from broker.topic import Data
from consumer.consumer_member import ConsumerMember
from consumer.group_coordinator_client import GroupCoordinatorClient
from group_coordinator.group_coordinator import GroupCoordinator


# from broker.topic import *
# from group_coordinator.group_coordinator import GroupCoordinator
# from consumer.consumer_member import ConsumerMember, RedisConsumerMember
# from consumer.group_coordinator_client import GroupCoordinatorClient
# from broker.broker import Broker
# from broker.partition_strategy.RR_partition_strategy import RRPartitionStrategy

def add_data(data_start, topic_name):
    for i in range(data_start*100,(data_start*100)+100):
        print(f'pushing {i}')
        Broker.add_data_to_topic(topic_name, Data(msg=str(i), ttl=None))

def fair_test():
    from group_coordinator.group_coordinator import GroupCoordinator
    from consumer.group_coordinator_client import GroupCoordinatorClient
    from broker.broker import Broker
    from consumer.consumer_member import ConsumerMember
    from broker.partition_strategy.RR_partition_strategy import RRPartitionStrategy
    from broker.topic import Data
    topic_name = "new_topic"
    group_name = "new_group"
    Broker.create_topic(topic_name, 6, RRPartitionStrategy())

    consumer_group_coordinator = GroupCoordinator(group_name)
    c1 = ConsumerMember(consumer_group_name=group_name)
    c2 = ConsumerMember(consumer_group_name=group_name)

    consumer_group_coordinator.attach_to_topic(topic_name)

    while True:
        x = input("choose action")
        if x == "add_data_to_topic":
            # add 100 data items to topic
            topic_to_add_data = str(input("Topic Name ?"))
            for i in range(100):
                Broker.add_data_to_topic(topic_to_add_data, Data(msg=str(i), ttl=None))
        elif x == "print_offsets":
            # print all offsets
            from offset_manager.offset_manager import OffsetClient
            print(OffsetClient.offsets)
        elif x == "add_consumer":
            # add consumer to the group
            c4 = ConsumerMember(group_name)
        elif x == "kill_consumer":
            # kill consumer
            node_id = str(input("provide node id"))
            GroupCoordinatorClient.stop_consumer(node_id)
        elif x == "add_partition_to_topic":
            # add partition to topic
            topic_to_add_partition = str(input("Topic Name ?"))
            Broker.add_partition(topic_to_add_partition)




def test():
    # topic = Topic("new_topic", 3, RRPartitionStrategy(3))
    Broker.create_topic("new_topic", 6, RRPartitionStrategy())
    Broker.create_topic("diff_topic", 6, RRPartitionStrategy())
    # for i in range(100):
    #     InMemoryBroker.add_data_to_topic("new_topic", str(i))
    # for k in InMemoryBroker.topics['new_topic'].partitions:
    #     print(k.data_len)
    # return
    print("topic_created")
    consumer_group_coordinator = GroupCoordinator("new_group", is_leader=True)
    GroupCoordinatorClient.register_group_coordinator("new_group", consumer_group_coordinator)

    c1 = ConsumerMember("new_group")
    # c2 = ConsumerMember("new_group")
    # c3 = ConsumerMember("new_group")

    # consumer_group_coordinator.register_consumer_member(c1)
    # consumer_group_coordinator.register_consumer_member(c2)
    # consumer_group_coordinator.register_consumer_member(c3)
    print("registry done")
    # ConsumerMemberManager.register_consumer_node(c1)
    # ConsumerMemberManager.register_consumer_node(c2)
    # ConsumerMemberManager.register_consumer_node(c3)
    # ConsumerManager.create_consumer_group("new_group")
    # ConsumerMemberManager.start_health_check()
    # ConsumerMemberManager.assign_to_consumer_group(c1.id, "new_group")
    # ConsumerMemberManager.assign_to_consumer_group(c2.id, "new_group")
    # ConsumerMemberManager.assign_to_consumer_group(c3.id, "new_group")

    # consumer_group.add_consumer_member(ConsumerMember())
    # consumer_group.add_consumer_member(ConsumerMember())
    # consumer_group.add_consumer_member(ConsumerMember())
    consumer_group_coordinator.attach_to_topic("new_topic")
    # t1 = threading.Thread(target=ConsumerManager.consumer_groups['new_group'].attach_to_topic, args=("new_topic",))
    # t1.start()
    print("here")
    time.sleep(5)
    # ConsumerMemberManager.assign_to_consumer_group(c1.id,"new_group")
    while True:
        x = input("choose action")
        # if x=="attach_to_topic":
        #     topic_name = str(input("topic_name"))
        #     t1 = threading.Thread(target=ConsumerManager.consumer_groups["new_group"].attach_to_topic,
        #                           args=(topic_name,))
        #     t1.start()
        if x == "add_data_to_topic":
            # curr_data = input("enter data")
            for i in range(1):
                threading.Thread(target=add_data, args=[i, "new_topic"]).start()
        if x == "print_count_data":
            # curr_data = input("enter data")
            from consumer.consumer_member import count
            print(count)
            # for i in range(5):
            #     threading.Thread(target=add_data, args=[i]).start()

        if x == "print_offsets":
            from offset_manager.offset_manager import OffsetClient
            print(OffsetClient.offsets)
        if x == "add_partition_to_topic":
            Broker.add_partition("new_topic")
        if x == "remove_partition":
            partiton_id = input("enter parttion id")
            Broker.remove_partition("new_topic", int(partiton_id))
        elif x == "add_consumer":
            c4 = ConsumerMember("new_group")
            # consumer_group_coordinator.register_consumer_member(c4)
            # ConsumerManager.register_consumer_member("new_group", c4.id)
        # elif x=="remove_consumer":
        #     node_id = str(input("give node id"))
        #     consumer_group_coordinator.("new_group",str(node_id))
        elif x == "kill_consumer":
            node_id = str(input("give node id"))
            GroupCoordinatorClient.stop_consumer(node_id)
            # ConsumerManager.register_consumer_member("new_group", c4.id)
    # topic.push_data(Data("one",2))
    # topic.push_data(Data("two", 2))
    # topic.push_data(Data("three", 2))
    # consumer_group.attach_to_topic("new_topic")
    # t1 = threading.Thread(target=consumer_group.attach_to_topic, args=("new_topic",))
    # t1.start()
    # topic.push_data(Data("four",2))
    # topic.push_data(Data("five", 2))
    # topic.push_data(Data("six", 2))
    # topic.push_data(Data("seven", 2))
    # print("added_new_data")
    # TopicRegistry.register_topic("new_topic", topic)


def testRedisFlow():
    # topic = Topic("new_topic", 3, RRPartitionStrategy(3))
    from broker.redis_topic import RedisTopicClient
    from broker.topic import Data
    from consumer.consumer_member import RedisConsumerMember
    from group_coordinator.group_coordinator import RedisGroupCoordinatorClient
    from configs.redis_client import RedisClient
    r = RedisClient()
    r.redis_client.flushall()
    RedisTopicClient.create_topic("new_topic", 6, 'RRPartitionStrategy')
    # curr = RedisTopic()
    # curr.push_data("new_topic","one")
    # print(curr.poll_data("new_topic",0,0,2))
    # return
    # TopicRegistry.create_topic("new_topic",6,RRPartitionStrategy())
    # TopicRegistry.create_topic("diff_topic", 6, RRPartitionStrategy())
    # print("topic_created")
    consumer_group_coordinator = RedisGroupCoordinatorClient("new_group", is_leader=True)
    # GroupCoordinatorClient.register_group_coordinator("new_group", consumer_group_coordinator)

    # c1 = RedisConsumerMember("new_group")
    # c2 = RedisConsumerMember("new_group")
    # c3 = RedisConsumerMember("new_group")
    # consumer_group_coordinator.register_consumer_member(c1.id)
    # consumer_group_coordinator.register_consumer_member(c2.id)
    # consumer_group_coordinator.register_consumer_member(c3.id)
    print("registry done")
    # ConsumerMemberManager.register_consumer_node(c1)
    # ConsumerMemberManager.register_consumer_node(c2)
    # ConsumerMemberManager.register_consumer_node(c3)
    # ConsumerManager.create_consumer_group("new_group")
    # ConsumerMemberManager.start_health_check()
    # ConsumerMemberManager.assign_to_consumer_group(c1.id, "new_group")
    # ConsumerMemberManager.assign_to_consumer_group(c2.id, "new_group")
    # ConsumerMemberManager.assign_to_consumer_group(c3.id, "new_group")

    # consumer_group.add_consumer_member(ConsumerMember())
    # consumer_group.add_consumer_member(ConsumerMember())
    # consumer_group.add_consumer_member(ConsumerMember())
    # time.sleep(2)
    consumer_group_coordinator.attach_to_topic("new_topic")
    # t1 = threading.Thread(target=ConsumerManager.consumer_groups['new_group'].attach_to_topic, args=("new_topic",))
    # t1.start()
    print("here")
    # time.sleep(5)
    time.sleep(5)
    curr = RedisTopicClient()
    curr.add_partition("new_topic")

    time.sleep(5)
    # curr.push_data("new_topic","one")
    # # ConsumerMemberManager.assign_to_consumer_group(c1.id,"new_group")
    # while True:
    #     print("")
    while True:
        x = input("choose action")
        # if x=="attach_to_topic":
        #     topic_name = str(input("topic_name"))
        #     t1 = threading.Thread(target=ConsumerManager.consumer_groups["new_group"].attach_to_topic,
        #                           args=(topic_name,))
        #     t1.start()
        if x == "add_data_to_topic":
            # add_data(0)
            for i in range(100):
                # curr_data = input("enter data")
                curr.add_data_to_topic("new_topic", Data(str(i),None))
            # TopicRegistry.add_data_to_topic("new_topic", Data(str(curr_data), 2))
        if x == "add_partition_to_topic":
            Broker.add_partition("new_topic")
        if x == "remove_partition":
            partiton_id = input("enter parttion id")
            Broker.remove_partition("new_topic", int(partiton_id))
        elif x == "add_consumer":
            print("adding_Consumer")
            c4 = RedisConsumerMember("new_group")
            # consumer_group_coordinator.register_consumer_member(c4.id)
            # consumer_group_coordinator.notify_consumer_member_added()
            # ConsumerManager.register_consumer_member("new_group", c4.id)
        # elif x=="remove_consumer":
        #     node_id = str(input("give node id"))
        #     consumer_group_coordinator.("new_group",str(node_id))
        elif x == "kill_consumer":
            node_id = str(input("give node id"))
            # consumer_group_coordinator.stop_consumer(node_id)
            r.publish_to_channel(f'{node_id}_stop_consumer', "1")
            # ConsumerManager.register_consumer_member("new_group", c4.id)
    # topic.push_data(Data("one",2))
    # topic.push_data(Data("two", 2))
    # topic.push_data(Data("three", 2))
    # consumer_group.attach_to_topic("new_topic")
    # t1 = threading.Thread(target=consumer_group.attach_to_topic, args=("new_topic",))
    # t1.start()
    # topic.push_data(Data("four",2))
    # topic.push_data(Data("five", 2))
    # topic.push_data(Data("six", 2))
    # topic.push_data(Data("seven", 2))
    # print("added_new_data")
    # TopicRegistry.register_topic("new_topic", topic)

# testRedisFlow()

"""
Refactor

1. unassigned,assigned as key names
2. any big functions
3. any dangling keys
4. any non thread safe operations
5/ Variable names and functions names
6. Class and their definitions
7. Comments and explanations of all functions or classes
"""