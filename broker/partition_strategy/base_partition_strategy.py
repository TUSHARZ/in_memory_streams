class TopicDataPartitionStrategy:

    def __init__(self, **kwargs):
        pass

    def get_next_partition(self, no_of_partitions):
        pass

    @classmethod
    def get_partition_strategy_class(cls, class_name):
        from broker.partition_strategy.RR_partition_strategy import RRPartitionStrategy
        if class_name == 'RRPartitionStrategy':
            return RRPartitionStrategy

    def serialize(self):
        raise Exception("No Default implementation")

    @classmethod
    def deserialize(cls, obj_str):
        raise Exception("No Default implementation")


