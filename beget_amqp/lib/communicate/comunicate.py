from ..helpers.logger import Logger
from ..helpers.RedisConnector import RedisConnector


class Communicate(object):
    PREFIX_QUESTION = 'q_'
    PREFIX_ANSWER = 'a_'

    LOCAL_STORAGE_LIVE_TIME = 60 * 60

    KEY_SERVICE_LIST = 'amqp_services'

    def __init__(self, redis_host, redis_port):
        self.logger = Logger.get_logger()
        self.redis = RedisConnector(redis_host, redis_port)
