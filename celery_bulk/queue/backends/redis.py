import json

import redis

from ...models import Job
from .. import JobQueue


class RedisClient:
    def __init__(self, key_prefix, *, timeout=None, client_factory=redis,
                 json_loads=json.loads, json_dumps=json.dumps):
        """
        Timeout é o tempo de expiração da key. Pode ser um int em segundos ou
        um timedelta.
        """
        self.key_prefix = key_prefix
        self.timeout = timeout
        self.client = redis.Redis()
        self.json_loads = json_loads
        self.json_dumps = json_dumps

    def _build_key(self, key):
        return f'{self.key_prefix}{key}'

    def push(self, key, value):
        key = self._build_key(key)
        pipeline = self.client.pipeline()
        pipeline.rpush(key, self.json_dumps(value))
        if self.timeout:
            pipeline.expire(key, self.timeout)
        pipeline.execute()

    def popmany(self, key, size):
        pipeline = self.client.pipeline()
        key = self._build_key(key)
        pipeline.lrange(key, 0, size - 1)
        pipeline.ltrim(key, size, -1)
        data, _ = pipeline.execute()
        return [self.json_loads(item) for item in data]

    def clear(self, key):
        key = self._build_key(key)
        self.client.delete(key)

    def count(self, key):
        key = self._build_key(key)
        return self.client.llen(key)


class Queue(JobQueue):
    prefix = ':jobs'

    def __init__(self):
        self._client = RedisClient('lazy_batch')

    def enqueue(self, job):
        self._client.push(self.prefix, job.serialize())

    def dequeue(self, quantity):
        items = self._client.popmany(self.prefix, quantity)
        return list(map(Job.deserialize, items))

    def clear(self):
        self._client.clear(self.prefix)

    def count(self):
        return self._client.count(self.prefix)

    def __len__(self):
        return self.count()
