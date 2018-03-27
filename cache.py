import redis
import config
import utils

pool = redis.ConnectionPool.from_url(config.REDIS_URL)


class RedisObject:
    def __init__(self):
        self.r = redis.StrictRedis(connection_pool=pool)


class RedisExpiringSet(RedisObject):
    def __init__(self, name, expire):
        super().__init__()
        self.name = name
        self.expire = expire

    def __repr__(self):
        min_timestamp = utils.get_now_timestamp() - self.expire
        items = self.r.zrangebyscore(self.name, min_timestamp, float('+inf'))
        return 'RedisExpiringSet{%s}' % items

    def __contains__(self, item: str) -> bool:
        saved = self.r.zscore(self.name, item)
        now = utils.get_now_timestamp()

        # 1:30 + 1h, now 2:00, not expired
        if saved and saved + self.expire > now:
            self.r.zadd(self.name, now, item)
            return True

        self.r.zrem(self.name, item)
        return False

    def add(self, item: str):
        self.r.zadd(self.name, utils.get_now_timestamp(), item)

    def clear(self):
        self.r.delete(self.name)


class RedisQueue(RedisObject):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __repr__(self):
        items = self.r.lrange(self.name, 0, -1)
        return 'RedisQueue[%s]' % items

    def qsize(self) -> int:
        return self.r.llen(self.name)

    def task_done(self):
        pass

    def get(self) -> str:
        val = self.r.lpop(self.name)
        if val is None:
            return
        return val.decode('utf-8')

    def put(self, value: str):
        self.r.rpush(self.name, value)


class RedisDict(RedisObject):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def __repr__(self):
        items = self.r.hgetall(self.name)
        return 'RedisDict{%s}' % items

    def __getitem__(self, key: str):
        return self.r.hget(self.name, key).decode('utf-8')

    def __setitem__(self, key: str, value: str):
        self.r.hset(self.name, key, value)
