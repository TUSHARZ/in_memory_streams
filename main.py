"""
We have to design a message queue supporting publisher-subscriber model. It should support following operations:

It should support multiple topics where messages can be published.
Publisher should be able to publish a message to a particular topic.
Subscribers should be able to subscribe to a topic.
Whenever a message is published to a topic, all the subscribers, who are subscribed to that topic, should receive the message.
Subscribers should be able to run in parallel


User Flow:
1. User creates a topic
    - topic_name
    - partitions
2. subscribers choose to subscribe to topics
3. user pushes data to topic
4. All the subscribers will get a notification

PUSH MODEL
Components:

Topic:
    members:
    topic_name(string)
    subscribers(List<Subscribers>
    messages(List<Messages>)
    ttl

    functions:
    push_message(message)
    subscribe(subscriber)

Message:
    members:
    key(string)
    content(string)
    created_at

Subscriber:
    members:
    subscriber_id

    functions:
    update(topic_id, data)


PULL MODEL WITH PARTITIONS(KAFKA)
Components:

ConsumerGroupMember:
    members:
    partition_ids(List<int>)
    active(boolean)

    functions:
    assign_partition(partition_id)
    remove_partition(partition_id)

ConsumerGroup:
    members:
    consumer_group_name(string)
    members(List<ConsumerGroupMember>)

    functions:
    add_members(ConsumerGroupMember)

PartitionOffset:
    members:
    partition_no
    offset

    functions:
    set_offset(offset)

ConsumerGroupOffsets:
    members:
    consumer_group_name
    partition_offsets(List<PartitionOffsets>)


Topic:
    members:
    topic_name(string)
    partitions(List)
    messages(List<Messages>)
    consumer_group_offsets(List<ConsumerGroupOffsets>)
    ttl

    functions:
    push_message(message)
    poll_message(consumer_group, partition)
    commit(consumer_group, partition, offset)
    subscribe(consumer_group)

Message:
    members:
    key(string)
    content(string)
    created_at

Broker:
    members:
    topics(List<Topics>)
    no_of_nodes



Redis Integration:

use pub sub on keyspace notifications to receive notifications

Broker Storage
LPUSH for broker storage
LRANGEE for getting data

leader:
 2
 3
 4
 1



REDIS INTEGRATIONS:

Jobs:
track updates on mapping any notify relevant consumers using pub sub
track health check
deploy group coordination separately


Nodes Partitions
Store nodes<>partitions mapping
health check

REDIS KEYS:

topic_subs : {"topic":[consumer_groups...]}
consumer_groups : {"consumer_group":[member_ids...]}
nodes_partitions : {"node":["topic_partition_ids...]}
offsets : {"consumer_group':{topic_name:[offsets..]}
nodes_health : {node_id: is_active}
topics : {topic_name:[partitions...] , ... }
leader : {}

config set notify-keyspace-events KEA


"""


