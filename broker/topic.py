import json
import threading

class BaseData:
    """
    Base class of data objects. Defines serialize/deserialize methods for data
    """

    def __init__(self, ttl=None):
        self.ttl = ttl

    @classmethod
    def serialize(cls, data, encoding_scheme='utf-8'):
        raise Exception("No serialize method defined")

    @classmethod
    def deserialize(cls, data: bytes):
        raise Exception("No deserialize method defined")


class Data(BaseData):

    def __init__(self, msg, ttl):
        super(Data, self).__init__(ttl)
        self.msg = msg

    @classmethod
    def serialize(cls, data, encoding_scheme='utf-8') -> bytes:
        """
        Serialize data into bytes
        :param data:
        :param encoding_scheme: encoding scheme of data
        :return:
        """
        return json.dumps({"msg": data.msg, "ttl": data.ttl}).encode(encoding_scheme)

    @classmethod
    def deserialize(cls, data, encoding_scheme='utf-8'):
        """
        deserialize data and return the data object from bytes/string
        :param data:
        :param encoding_scheme:
        :return:
        """
        if isinstance(data, bytes):
            data = data.decode(encoding_scheme)
        data = json.loads(data)
        return cls(msg=data['msg'], ttl=data['ttl'])


class SerialStorage:
    """
    Interface for serial storage
    """

    def __init__(self, data_class=Data):
        self.data_class = data_class

    def add_data(self, data, ttl=None):
        pass

    def get_range_data(self, limit, offset):
        pass


class LLNode:

    def __init__(self, data):
        self.data = data
        self.next = None


class ArrayStorage(SerialStorage):

    def __init__(self, data_class=Data):
        super(ArrayStorage, self).__init__(data_class)
        self.data = []
        self.lock = threading.Lock()

    def add_data(self, data, ttl=None):
        with self.lock:
            self.data.append(self.data_class.serialize(data))

    def get_range_data(self, limit, offset):
        return [self.data_class.deserialize(_data) for _data in self.data[limit:limit + offset]]


class LLStorage(SerialStorage):

    def __init__(self, data_class=Data):
        super(LLStorage, self).__init__(data_class)
        self.manage_data = {}  # additional map to maintain nodes on any given offset , so that range query can be fast
        self.head = None
        self.tail = None
        self.data_len = 0  # maintains current length of LL
        self.lock = threading.Lock()

    def add_data(self, data, ttl=None):
        with self.lock:
            curr_node = LLNode(self.data_class.serialize(data))
            if not self.head:
                self.head = self.tail = curr_node
            else:
                self.tail.next = curr_node
                self.tail = curr_node
            self.manage_data[self.data_len] = curr_node  # add to map as well for fast access of nodes
            self.data_len += 1

    def get_range_data(self, limit, offset):
        """
        returns data between provided limits
        :param limit:
        :param offset:
        :return:
        """
        data = []
        curr = self.manage_data.get(limit)  # uses map to go to node faster instead of traversing
        if not curr:
            return data
        for i in range(limit, limit + offset):
            if not curr:
                # if no further data available , stop
                break
            data.append(self.data_class.deserialize(curr.data))
            curr = curr.next
        return data




# class RedisStorage(SerialStorage):
#
#     def __init__(self, data_class=Data, **kwargs):
#         super(RedisStorage, self).__init__(data_class)
#         self.redis_client = kwargs.get('redis_client')
#         self.list_name = kwargs.get('topic_partition_key')
#         # self.topic_metadata_key = kwargs.get('topic_metadata_key')
#         # self.topic_strategy_metadata_key = kwargs.get('topic_strategy_metadata_key')
#         # self.topic_strategy_metadata_val = kwargs.get('topic_strategy_metadata_val')
#
#     def add_data(self, data, ttl=None, **kwargs):
#         # keys = [self.list_name, self.topic_metadata_key, self.topic_strategy_metadata_key, self.topic_strategy_metadata_val]
#         self.redis_client.call_lua("add_data_to_topic", keys=kwargs.get('lua_keys',[]), args=[data.serialize()])
#         # self.redis_client.add_to_list(self.list_name, Data(data, ttl).__str__())
#
#     def get_range_data(self, limit, offset):
#         return [self.data_class.deserialize(k) for k in
#                 self.redis_client.get_range_list_data(self.list_name, limit, limit +
#                                                       offset - 1)]


class Topic:

    def __init__(self, topic_name, partitions, strategy, storage_type=LLStorage, storage_metadata=None):
        """
        Stores data in partitions
        :param topic_name: Name of the topic
        :param partitions: No of partitions to use
        :param strategy: Partitioning strategy to use
        :param storage_type: Serial Storage Class to use
        """
        self.topic_name = topic_name
        self.partitions = [storage_type() for _ in range(partitions)]
        self.partition_strategy = strategy
        self.storage_type = storage_type

    def push_data(self, data, ttl=None):
        """
        Get next partition from partition strategy and push the data
        :param data:
        :param ttl:
        :return:
        """
        next_partition = self.partition_strategy.get_next_partition(len(self.partitions))
        print(f"pushing data {data} in partition {next_partition}")
        self.partitions[next_partition].add_data(data, ttl)
        return

    def poll_data(self, partition, limit, offset):
        return self.partitions[partition].get_range_data(limit, offset)

    def add_partition(self):
        self.partitions.append(self.storage_type())


