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

    def set(self, key, value):
        try:
            status = self.setex(key, value, self.timeout)
            return status

        except Exception as e:
            msg = "Unable to connect to redis. " + str(e)
            raise Exception(msg)

    def hset(self, name, key, value):
        try:
            status = self.r.hset(name, key, value)
            return status

        except Exception as e:
            msg = "Unable to connect to redis. " + str(e)
            raise Exception(msg)

    def hdel(self, name, *keys):
        try:
            status = self.r.hdel(name, keys)
            return status

        except Exception as e:
            msg = "Unable to connect to redis. " + str(e)
            raise Exception(msg)

    def hkeys(self, name):
        try:
            status = self.r.hkeys(name)
            return status

        except Exception as e:
            msg = "Unable to connect to redis. " + str(e)
            raise Exception(msg)

    def keys(self, name):
        try:
            status = self.r.keys(name)
            return status

        except Exception as e:
            msg = "Unable to connect to redis. " + str(e)
            raise Exception(msg)

    def setex(self, key, value, time):
        try:
            status = self.r.setex(key, time, value)
            return status
        except Exception as e:
            msg = "Unable to connect to redis." + str(e)
            raise Exception(msg)

    def lpush(self, key, *values):
        try:
            status = self.r.lpush(key, values)
            return status
        except Exception as e:
            msg = "Unable to connect to redis." + str(e)
            raise Exception(msg)

    def pipeline(self):
        try:
            pipe = self.r.pipeline()
            return pipe
        except Exception as e:
            raise Exception("Unable to connect to redis." + str(e))

    def incr(self, key, amount=1):
        try:
            status = self.r.incr(key, amount)
            return status
        except Exception as e:
            msg = "Unable to connect to redis." + str(e)
            raise Exception(msg)

    def set_list(self, key, list_value):
        try:
            value = json.dumps(list_value)
            status = self.r.set(key, value)
            return status
        except Exception as e:
            msg = "Unable to connect to redis." + str(e)
            raise Exception(msg)

    def get_list(self, key):
        try:
            status = json.loads(self.r.get(key))
            return status

        except Exception as e:
            msg = "Unable to connect to redis." + str(e)
            raise Exception(msg)

    def exists(self, key):
        try:
            status = self.r.exists(key)
            return status

        except Exception as e:
            msg = "Unable to connect to redis." + str(e)
            raise Exception(msg)

    def expire(self, key, seconds=-1):
        try:
            status = self.r.expire(key, seconds)
            return status
        except Exception as e:
            msg = "Unable to connect to redis." + str(e)
            raise Exception(msg)

    def delete(self, key):
        try:
            if self.exists(key) is True:
                status = self.r.delete(key)
            else:
                status = True
            return status

        except Exception as e:
            msg = "Unable to connect to redis." + str(e)
            raise Exception(msg)

    def get(self, key):
        try:
            status = self.r.get(key)
            return status
        except Exception as e:
            msg = "Unable to connect to redis." + str(e)
            raise Exception(msg)

    def hget(self, name, key):
        try:
            status = self.r.hget(name, key)
            return status
        except Exception as e:
            msg = "Unable to connect to redis." + str(e)
            raise Exception(msg)
