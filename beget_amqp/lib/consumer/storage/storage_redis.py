from ...helpers.RedisConnector import RedisConnector
from ...helpers.logger import Logger


class StorageRedis(object):
    CONSUMER_PREFIX = 'consumer'

    def __init__(self, redis_host, redis_port):
        self.logger = Logger.get_logger()
        self.redis = RedisConnector(redis_host, redis_port)
