from ...helpers.RedisConnector import RedisConnector
from ...helpers.logger import Logger


class StorageRedis(object):
    DEPENDENCE_PREFIX = 'dependence'

    def __init__(self, redis_host, redis_port):
        self.logger = Logger.get_logger()
        self.redis = RedisConnector(redis_host=redis_host, redis_port=redis_port)
