import time
import uuid
from consumer.group_coordinator_client import GroupCoordinatorClient
# from configs.redis_topic_helper import RedisTopicHelper
from offset_manager.offset_manager import OffsetClient
from broker.broker import Broker
from broker.redis_topic import RedisTopicClient
from threading import Lock
import threading
# from configs.redis_consumer_helper import RedisConsumerHelper
import concurrent.futures
from group_coordinator.group_coordinator import PartitionTopic


# class DefaultHandle


# track_multiple_consumption = {}
# if d in track_multiple_consumption:
#     print(f"FATAL MULTIPLE MULTIPLE MULTIPLE {d.msg}")
#     time.sleep(1000000)


class DataHandler:

    def __init__(self):
        pass

    def handle(self, data):
        raise Exception("Not Implemented")


class DefaultDataHandler(DataHandler):

    def handle(self, data):
        for d in data:
            print(f"processing data {d}")


import signal
import time


class ConsumerMember:

    def __init__(self, consumer_group_name, msg_to_consume=10, max_polling_concurrency=5, handler_class=None):
        self.active = True
        self.consumer_group = consumer_group_name
        self.handler = handler_class or DefaultDataHandler()  # handler class to use
        self.id = str(uuid.uuid4())
        self.commit_lock = Lock()  # this lock will be used in polling cycle
        self.group_coordinator_client = GroupCoordinatorClient  # group coordinator client to communicate with group coordinator
        self.topic_client = Broker  # topic client to communicate with broker
        self.msg_to_consume = msg_to_consume  # msg to consume from each partition
        self.max_polling_concurrency = max_polling_concurrency  # max partitions to poll parallely
        self.join_group()

    def send_heart_beat(self):
        """
        Send heart beat to coordinator
        :return:
        """
        while True:
            if self.active:
                self.group_coordinator_client.send_heart_beat(self.consumer_group, self.id)
            else:
                return False

    def join_group(self):
        """
        Join Consumer group
        :return:
        """
        self.active = True

        # start heart beat
        threading.Thread(target=self.send_heart_beat).start()

        time.sleep(1)
        self.group_coordinator_client.join_group_request(self.consumer_group, self.id, self)

        # start polling
        threading.Thread(target=self.poll).start()

    def get_assigned_partitions(self):
        """
        Get assigned partitions from group coordinator
        :return:
        """
        return list(self.group_coordinator_client.get_assigned_partitions(self.consumer_group, self.id))

    def notify_partition_removed(self, topic_partition_ids, *args, **kwargs):
        """
        Since group coordinator cannot simply remove partition from consumer, as it may lead to uncommitted offsets.
        This function receives request from group coordinator to remove partition and notifies back once it stops
        polling so that group coordinator can remove the partitions from this consumer.
        :param topic_partition_ids: partitions to remove
        :param args:
        :param kwargs:
        :return:
        """
        print(f"removing topic partition ids {topic_partition_ids}")
        retry_count = 0
        with self.commit_lock:
            # take the commit lock so that polling stops, and will be released once partitions
            # are removed and consumer fetches updated partitions from group coordinator.
            # This is to ensure no uncommitted offset remains
            while retry_count < 3:
                # commit lock to ensure no uncommitted offsets are left before removing the partition
                try:
                    # send request to group coordinator to remove partition
                    self.group_coordinator_client.revoke_partition(self.consumer_group, topic_partition_ids,
                                                                   consumer_id=self.id)

                    return True
                except Exception as e:
                    print(f"Unable to remove partition{e}")
                retry_count += 1

            # if retries exceeded , terminate consumer
            print("max retries exceeded, terminating consumer")
            self.active = False
            return False

    def stop_consumer_gracefully(self, *args, **kwargs):
        """
        Mark consumer inactive
        :param args:
        :param kwargs:
        :return:
        """
        print("Stopping consumer")
        with self.commit_lock:
            # take commit lock to avoid uncommitted partition
            self.active = False
            print("stopped_consumer")
            return True

    def poll(self):
        """
        This method polls the data from assigned partitions in parallel, processes and commit the updated offset
        :return:
        """

        if not self.active:
            return False

        # If no partitions is assigned, there is noting to do :(
        while not self.get_assigned_partitions():
            if not self.active:
                return False
            print("no_partition_assigned_to_poll")

        while True:

            # acquire lock for polling cycle
            self.commit_lock.acquire()

            if not self.active:
                self.commit_lock.release()
                break

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_polling_concurrency) as executor:
                # Process data from each assigned partition parallely
                polling_result = [executor.submit(self.poll_data_from_partition, topic_partition_id) for
                                  topic_partition_id in self.get_assigned_partitions()]

                # Wait for polling cycle to complete before releasing commit lock
                results = [task.result() for task in polling_result]

            # release lock after polling cycle
            self.commit_lock.release()
            time.sleep(1)  # some delay to avoid lock starvation

    def poll_data_from_partition(self, topic_partition):
        """
        :param topic_partition:
        Call offset client to get the current offset of topic<>partition until which data is consumed
        Call broker client to get data from topic partition from offset to offset+msg to consume
        Process the data
        Commit updated offset to offset client
        Note: If committing new offset fails after data processing or anything in data processing fails itself,
        consumer will be polling for the same data(as offset is not changed) and keeps processing same data again.
        (Additional measures can be added to avoid this , like msg deduplication key)
        :return:
        """
        try:
            if isinstance(topic_partition, str):
                topic_partition = PartitionTopic(topic_name=topic_partition.split("::")[0],
                                                 partition=int(topic_partition.split("::")[1]))
            _topic_name = topic_partition.topic_name
            _partition_id = topic_partition.partition
            _curr_offset_of_partition = self.group_coordinator_client.get_consumer_group_offset(self.consumer_group,
                                                                                                _topic_name,
                                                                                                _partition_id)
            if _curr_offset_of_partition is None:
                raise Exception("unable to get latest offset")
            _data = self.topic_client.poll_data(_topic_name, _partition_id, _curr_offset_of_partition,
                                                _curr_offset_of_partition + self.msg_to_consume)
            if not _data:
                return True
            print(_data)
            # handle data
            self.handler.handle(_data)

            # commit offset
            result = self.group_coordinator_client.commit_consumer_group_offset(self.consumer_group, _topic_name,
                                                                                _partition_id,
                                                                                _curr_offset_of_partition + len(_data))

            if not result:
                raise Exception("unable to commit offset")
            print(f"committed offset for topic_partition_id {_topic_name}_{_partition_id}")
            return True
        except Exception as e:
            print(f"Error while processing data {e}")
            return False


from configs.redis_offset_client import RedisOffsetClient


class RedisConsumerMember(ConsumerMember):
    from configs.redis_client import RedisClient

    redis_client = RedisClient()

    def __init__(self, consumer_group_name, msg_to_consume=10, max_polling_concurrency=5, handler_class=None):
        super(RedisConsumerMember, self).__init__(consumer_group_name, msg_to_consume, max_polling_concurrency,
                                                  handler_class)
        self.topic_client = RedisTopicClient
        self.add_chanel_sub()

    def remove_partitions_listener(self, *args, **kwargs):
        """
        Partition remove listener, this will be called when consumerCoordinator makes a request to consumer to
        revoke partition
        :param args:
        :param kwargs:
        :return:
        """
        topic_partition_ids = args[0].get('data').split(',')
        self.notify_partition_removed(topic_partition_ids=topic_partition_ids)

    def add_chanel_sub(self):
        """
        add channel subs
        :return:
        """
        # add sub for remove partition channel
        self.redis_client.add_channel_sub(f'{self.id}_remove_partition', self.remove_partitions_listener)
        # add sub for stop consumer channel
        self.redis_client.add_channel_sub(f'{self.id}_stop_consumer', self.stop_consumer_gracefully)
