import redis
from threading import Lock
import os

REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = 6379


class RedisClient:
    """
    Interacts with redis to perform operations.
    This will be singleton , since we need only one redis connection
    """

    instance = None
    lock = Lock()

    def __new__(cls):
        """
        Make sure only one instance of redis client is created
        """
        with cls.lock:
            if cls.instance is None:
                cls.instance = super(RedisClient, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        # create redis connection
        self.redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, charset="utf-8",
                                              decode_responses=True)

        # enable key-space notifications
        self.redis_client.config_set('notify-keyspace-events', 'KEA')

        # start redis pub sub in thread
        self.redis_pub_sub = self.redis_client.pubsub()
        self.redis_pub_sub.run_in_thread(daemon=True)

    def call_lua(self, script_name, keys, args):
        """
        Call lua script
        :param script_name: name of script in lua scripts folder
        :param keys: keys for lua script
        :param args: args for lua script
        :return:
        """
        lua_file_path = f'{os.path.dirname(__file__)}/lua_scripts/{script_name}.lua'
        f = open(lua_file_path, "r")
        lua = f.read()
        operation = self.redis_client.register_script(lua)
        op_return = operation(keys=keys, args=args)
        return op_return

    def set_key(self, key, value, exp=None):
        """
        set key in redis
        :param key:
        :param value:
        :param exp:
        :return:
        """
        return self.redis_client.set(key, value, exp)

    def add_to_list(self, list_name, values):
        """
        append data to end of list
        :param list_name:
        :param values:
        :return:
        """
        return self.redis_client.rpush(list_name, values)

    def get_range_list_data(self, list_name, start_index, end_index):
        """
        query range data from list
        :param list_name:
        :param start_index:
        :param end_index:
        :return:
        """
        return self.redis_client.lrange(list_name, start_index, end_index)

    def add_to_hash(self, hash_key, field, value, method='hset'):
        """
        add key<>value to hash
        :param hash_key:
        :param field:
        :param value:
        :param method: hset for overriding/creating keys or hsetnx for creating keys
        :return:
        """
        if method == 'hset':
            self.redis_client.hset(hash_key, field, value)
        elif method == 'hsetnx':
            self.redis_client.hsetnx(hash_key, field, value)
        else:
            raise Exception("No such method defined")

    def add_multiple_to_hash(self, hash_key, hash_map):
        """
        add multiple values to hash
        :param hash_key:
        :param hash_map:
        :return:
        """
        self.redis_client.hmset(hash_key, hash_map)

    def increment_hash_value(self, hash_key, field, inrby):
        """
        increment a key in hash
        :param hash_key:
        :param field:
        :param inrby:
        :return:
        """
        self.redis_client.hincrby(hash_key, field, inrby)

    def add_to_set(self, set_name, values):
        """
        add member to a set
        :param set_name:
        :param values:
        :return:
        """
        return self.redis_client.sadd(set_name, values)

    def publish_to_channel(self, channel, msg):
        """
        publish data to redis channel
        :param channel:
        :param msg:
        :return:
        """
        self.redis_client.publish(channel, msg)

    def get_key(self, key):
        """
        Get key from redis
        :param key:
        :return:
        """
        return self.redis_client.get(key)

    def get_hash(self, hash_name):
        """
        get hash from redis
        :param hash_name:
        :return:
        """
        return self.redis_client.hgetall(hash_name)

    def get_hash_key(self, hash_name, key):
        """
        get a single key from hash
        :param hash_name:
        :param key:
        :return:
        """
        return self.redis_client.hget(hash_name, key)

    def get_hash_len(self, hash_name):
        """
        get len of keys in hash
        :param hash_name:
        :return:
        """
        return self.redis_client.hlen(hash_name)

    def get_set_members(self, key):
        """
        Get set members of redis set
        :param key:
        :return:
        """
        return self.redis_client.smembers(key)

    def add_sub(self, key_name, function):
        """
        add a subscription to redis pub sub
        :param key_name: keyname for subcription
        :param function: call back function of sub
        :return:
        """
        print(f"adding sub for key {key_name}")
        self.redis_pub_sub.psubscribe(**{f'__keyspace@0__:{key_name}': function})

    def add_channel_sub(self, channel_name, function):
        """
        add sub to channel
        :param channel_name:
        :param function: callback function
        :return:
        """
        self.redis_pub_sub.psubscribe(**{channel_name: function})

    def check_key_exists(self, key):
        """
        check if key in redis exists or not
        :param key:
        :retaurn:
        """
        return self.redis_client.exists(key) == 1
