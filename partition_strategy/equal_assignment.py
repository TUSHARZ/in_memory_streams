from collections import defaultdict


class PartitionAssignmentStrategy:
    """
    Provides a consumer_member_id<>number_of_topic_partition_ids mapping.
    It basically tells us how many partitions to be assigned to each consumer
    """

    def __init__(self, consumer_member_ids):
        """
        :param consumer_member_ids: we will sort consumer member ids as we don't want shuffling to change in case of
        ties between no of partitions
        """
        self.consumer_member_ids = sorted(consumer_member_ids)

    def get_partitions_assignment(self, no_of_partitions):
        raise Exception("No Default Implementation")

    @classmethod
    def get_partition_strategy(cls, strategy_name):
        if strategy_name == EqualAssignmentStrategy.__name__:
            return EqualAssignmentStrategy


class EqualAssignmentStrategy(PartitionAssignmentStrategy):
    """
    this will split the partitions equally among all consumer members
    for eg. consumers : 5 and partitions : 5 , it will say assign each consumer one partition
            consumers: 5 and partitions : 7 , it will say assign two partitions to two consumers , and all the rest one
            consumer: 6 and partitions : 5 , it will say assign 5 consumers one partition and one will be unassigned
    """

    def __init__(self, consumer_member_ids):
        super(EqualAssignmentStrategy, self).__init__(consumer_member_ids)

    def get_partitions(self, no_of_partitions):
        print("Len partition, len consumer_members", no_of_partitions, len(self.consumer_member_ids))
        if not len(self.consumer_member_ids):
            return {}
        _equal = no_of_partitions // len(self.consumer_member_ids)
        _rem = no_of_partitions % len(self.consumer_member_ids)
        sol = defaultdict(int)
        for ix, k in enumerate(self.consumer_member_ids):
            sol[k] += _equal + (1 if ix < _rem else 0)
        return sol
