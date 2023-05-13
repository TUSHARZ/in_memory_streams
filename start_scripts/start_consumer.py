import os
import sys
import signal
import time

# add to python path , so that it can find module
sys.path.append('/'.join(os.path.dirname(__file__).split("/")[:-1]))

from consumer.consumer_member import RedisConsumerMember
from group_coordinator.group_coordinator import RedisGroupCoordinatorClient


class RunConsumer:

    def __init__(self):
        # handler on SIGINT to handle stopping consumer
        signal.signal(signal.SIGINT, self.signal_handler)
        self.is_active = True
        self.consumer_ref, self.group_coordinator_ref = None, None
        self.consumer_member_config = self.get_config()
        self.consumer_group_name = self.consumer_member_config['consumer_group_name']
        if not self.consumer_group_name:
            print("no consumer group defined")
            exit(1)

    @classmethod
    def get_config(cls):
        redis_consumer_member_config = {}
        for k in sys.argv[1:]:
            key = k.split("=")[0]
            val = k.split("=")[1]
            redis_consumer_member_config[key] = val
        return redis_consumer_member_config

    def signal_handler(self, signum, frame):
        """
        whenever SIGINT is received ie. on ctrl+c , we will stop the consumer
        :param signum:
        :param frame:
        :return:
        """
        self.consumer_ref.stop_consumer_gracefully()
        time.sleep(2)  # buffer time for all threads to end
        self.is_active = False

    def start_consumer(self):
        # start consumer in a blocking main thread
        self.group_coordinator_ref = RedisGroupCoordinatorClient(self.consumer_group_name)
        self.consumer_ref = RedisConsumerMember(**self.consumer_member_config)
        print("starting consumer")
        while self.is_active:
            pass
        exit(0)


RunConsumer().start_consumer()
