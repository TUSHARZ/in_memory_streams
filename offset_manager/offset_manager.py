from collections import defaultdict


class NoConsumerGroupExistsException(Exception):
    pass


class OffsetClient:
    offsets = defaultdict(dict)

    @classmethod
    def get_offset_key(cls, consumer_group, topic_name, partition):
        return f'{consumer_group}_{topic_name}_{partition}'

    @classmethod
    def commit_offset(cls, consumer_group, topic, partition, offset):
        cls.offsets[cls.get_offset_key(consumer_group, topic, partition)] = offset
        return True

    @classmethod
    def get_offset(cls, consumer_group, topic, partition):
        print("getting from local")
        return cls.offsets.get(cls.get_offset_key(consumer_group, topic, partition), 0)
        # if not cls.offsets.get(cls.get_offset_key(consumer_group,topic,partition)):
        #     return 0
        # return cls.offsets[consumer_group][partition]
