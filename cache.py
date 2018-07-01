import aioredis
import config
import utils


class RedisProperties(type):
    def __new__(mcs, class_name, class_bases, class_dict):
        new_class_dict = class_dict.copy()
        new_class_dict['r'] = utils.block(aioredis.create_redis_pool(address=config.REDIS_URL, minsize=1, maxsize=20))
        return type.__new__(mcs, class_name, class_bases, new_class_dict)


class RedisObject(metaclass=RedisProperties):
    r = None  # type: aioredis.Redis

    def __init__(self):
        pass


class RedisExpiringSet(RedisObject):
    def __init__(self, name, expire):
        super().__init__()
        self.name = name
        self.expire = expire

    async def repr(self) -> str:
        min_timestamp = utils.get_now_timestamp() - self.expire
        items = await self.r.zrangebyscore(self.name, min_timestamp, float('+inf'))
        return 'RedisExpiringSet%s' % (i.decode('utf-8') for i in items)

    def __repr__(self) -> str:
        return utils.block(self.repr())

    async def contains(self, item: str) -> bool:
        saved = await self.r.zscore(self.name, item)
        now = utils.get_now_timestamp()

        # 1:30 + 1h, now 2:00, not expired
        if saved and saved + self.expire > now:
            await self.r.zadd(self.name, now, item)
            return True

        await self.r.zrem(self.name, item)
        return False

    def __contains__(self, item) -> bool:
        return utils.block(self.contains(item))

    async def add(self, item: str):
        await self.r.zadd(self.name, utils.get_now_timestamp(), item)

    async def discard(self, item: str):
        await self.r.zrem(self.name, item)

    async def clear(self):
        await self.r.delete(self.name)


class RedisQueue(RedisObject):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    async def repr(self):
        items = await self.r.lrange(self.name, 0, -1)
        return 'RedisQueue%s' % (i.decode('utf-8') for i in items)

    def __repr__(self):
        return utils.block(self.repr())

    async def qsize(self) -> int:
        return await self.r.llen(self.name)

    async def task_done(self):
        pass

    async def get(self) -> str:
        val = await self.r.lpop(self.name)
        if val is None:
            return
        return val.decode('utf-8')

    async def put(self, value: str):
        await self.r.rpush(self.name, value)


class RedisDict(RedisObject):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    async def repr(self):
        return 'RedisDict%s' % {k.decode('utf-8'): v.decode('utf-8') for k, v in await self.r.hgetall(self.name).items()}

    def __repr__(self):
        return utils.block(self.repr())

    async def getitem(self, key: str) -> str:
        val = await self.r.hget(self.name, key)
        if val is None:
            return
        return val.decode('utf-8')

    def __getitem__(self, key: str) -> str:
        return utils.block(self.getitem(key))

    async def setitem(self, key: str, value: str):
        self.r.hset(self.name, key, value)

    def __setitem__(self, key: str, value: str):
        utils.block(self.setitem(key, value))

    async def delitem(self, key: str):
        await self.r.hdel(self.name, key)

    def __delitem__(self, key: str):
        utils.block(self.delitem(key))

    async def get(self, key: str, default: str):
        val = await self.getitem(key)
        if val is None:
            val = default
        return val

    async def incrby(self, key: str, val: int):
        await self.r.hincrby(self.name, key, val)

    async def items(self):
        return ((k.decode('utf-8'), v.decode('utf-8')) for k, v in await self.r.hgetall(self.name).items())
