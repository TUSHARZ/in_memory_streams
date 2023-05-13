import os
import sys

# add to python path , so that it can find module
sys.path.append('/'.join(os.path.dirname(__file__).split("/")[:-1]))
from broker.redis_topic import RedisTopicClient, Data


class RunBroker:

    @classmethod
    def start_broker_client(cls):
        while True:
            x = str(input('Select action \n'))
            if x == 'create_topic':
                topic_name = str(input('topic_name : '))
                topic_partitions = int(input('number of partitions : '))
                strategy = str(input('strategy : '))
                RedisTopicClient.create_topic(topic_name, topic_partitions, strategy)
                print("Created Topic \n")
            elif x == 'push_data_in_topic':
                topic_name = str(input('topic_name : '))
                data = str(input('data : '))
                RedisTopicClient.add_data_to_topic(topic_name, Data(data, None))
                print("Inserted Data to topic \n")
            elif x == 'exit':
                exit(0)


RunBroker.start_broker_client()
