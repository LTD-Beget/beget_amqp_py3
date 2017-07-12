import json

import redis


class RedisConnection(object):
    def __new__(cls, redis_host='localhost', redis_port=6379):
        cls._host = redis_host
        cls._port = redis_port
        cls._redis = {}
        cls.instance = None
        if not cls.instance:
            cls.instance = super(RedisConnection, cls).__new__(cls)

        return cls.instance

    def get(self, db=0):
        """
        :param db:
        :rtype: redis.StrictRedis
        """
        try:
            if db not in self._redis or not self._redis[db].ping():
                self._redis[db] = redis.StrictRedis(host=self._host, port=self._port, db=db)
        except redis.ConnectionError:
            self._redis[db] = redis.StrictRedis(host=self._host, port=self._port, db=db)
        return self._redis[db]


class RedisConnector:
    def __init__(self, redis_host, redis_port, redis_timeout=8400):
        connection = RedisConnection(redis_host=redis_host, redis_port=redis_port)
        self.r = connection.get()
        self.timeout = redis_timeout

    def __getattr__(self, item):
        return getattr(self.r, item)

    def set(self, key, value):
        try:
            status = self.setex(key, self.timeout, value)
            return status

        except Exception as e:
            msg = "RedisException: " + str(e)
            raise Exception(msg)

    def set_list(self, key, list_value):
        try:
            value = json.dumps(list_value)
            status = self.r.set(key, value)
            return status
        except Exception as e:
            msg = "RedisException: " + str(e)
            raise Exception(msg)

    def get_list(self, key):
        try:
            status = json.loads(self.r.get(key).decode('UTF-8'))
            return status

        except Exception as e:
            msg = "RedisException: " + str(e)
            raise Exception(msg)

    def expire(self, key, seconds=-1):
        try:
            status = self.r.expire(key, seconds)
            return status
        except Exception as e:
            msg = "RedisException: " + str(e)
            raise Exception(msg)

    def delete(self, key):
        try:
            if self.exists(key) is True:
                status = self.r.delete(key)
            else:
                status = True
            return status

        except Exception as e:
            msg = "RedisException: " + str(e)
            raise Exception(msg)
