from ...helpers.RedisConnector import RedisConnector
from ...helpers.logger import Logger


class StorageRedis(object):
    KEY_DONE = 'done'
    KEY_WORKER = 'worker_id'
    KEY_HEADER = 'headers'
    KEY_BODY = 'body'
    KEY_TIME_START_WAIT = 'start_wait'
    KEY_TIME_START_WORK = 'start_work'
    KEY_TIME_END_WORK = 'end_work'

    MESSAGE_DONE_NOT = '0'  # Сообщение было отработано
    MESSAGE_DONE_YES = '1'  # Сообщение еще не отработано

    MESSAGE_PREFIX = 'msg_store'

    LOCAL_STORAGE_LIVE_TIME = 60 * 60 * 24 * 3  # Время хранения информации в локальном хранилище

    def __init__(self, redis_host, redis_port):
        self.logger = Logger.get_logger()
        self.redis = RedisConnector(redis_host=redis_host, redis_port=redis_port)
