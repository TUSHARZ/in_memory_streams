from broker.partition_strategy.base_partition_strategy import TopicDataPartitionStrategy
import json


class RRPartitionStrategy(TopicDataPartitionStrategy):
    """
    Round robin partition strategy: Uses each partition equally
    """

    def __init__(self, **kwargs):
        self.last_used = -1
        super(RRPartitionStrategy, self).__init__()
        self.last_used = int(kwargs.get('last_used', -1))

    def get_next_partition(self, no_of_partitions):
        to_use = (self.last_used + 1) % no_of_partitions
        self.last_used = to_use
        return to_use

    def serialize(self):
        return json.dumps({"last_used": self.last_used})

    @classmethod
    def deserialize(cls, obj_str):
        return RRPartitionStrategy(**json.loads(obj_str))
