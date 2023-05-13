import os
import signal
import sys
import time

# add to python path , so that it can find module

sys.path.append('/'.join(os.path.dirname(__file__).split("/")[:-1]))
from group_coordinator.group_coordinator import RedisGroupCoordinatorClient


class RunGroupCoordinator:

    def __init__(self):
        self.is_active = True
        self.group_coordinator_config = self.get_config()
        self.group_coordinator_ref = RedisGroupCoordinatorClient(**self.group_coordinator_config,
                                                                 is_leader=True)
        signal.signal(signal.SIGINT, self.signal_handler)

    @classmethod
    def get_config(cls):
        redis_consumer_member_config = {}
        for k in sys.argv[1:]:
            key = k.split("=")[0]
            val = k.split("=")[1]
            redis_consumer_member_config[key] = val
        return redis_consumer_member_config

    def signal_handler(self, signum, frame):
        print("stopping")
        time.sleep(2)
        self.is_active = False
        exit(0)

    def start_group_coordinator(self):
        while self.is_active:
            x = str(input("action\n"))
            if x == "attach_to_topic":
                topic_name = str(input('topic_name : '))
                self.group_coordinator_ref.attach_to_topic(topic_name)
                print("Attached to topic")


RunGroupCoordinator().start_group_coordinator()
