import json


class Redis:
    def __init__(self, client, key='bulk_task', *,
                 timeout=None, json_loads=json.loads, json_dumps=json.dumps):
        self.client = client
        self.key = key
        self.timeout = timeout
        self.json_loads = json_loads
        self.json_dumps = json_dumps

    def enqueue(self, value):
        pipeline = self.client.pipeline()
        pipeline.rpush(self.key, self.json_dumps(value))
        if self.timeout:
            pipeline.expire(self.key, self.timeout)
        pipeline.execute()

    def dequeue(self, size):
        pipeline = self.client.pipeline()
        pipeline.lrange(self.key, 0, size - 1)
        pipeline.ltrim(self.key, size, -1)
        data, _ = pipeline.execute()
        return [self.json_loads(item) for item in data]

    def clear(self):
        self.client.delete(self.key)

    def count(self, key):
        return self.client.llen(self.key)


class Dummy:
    def __init__(self):
        self.data = []

    def enqueue(self, job):
        self.data.append(job)

    def dequeue(self, quantity):
        items = []
        for i in range(quantity):
            try:
                item = self.data.pop(0)
            except IndexError:
                break
            items.append(item)
        return items

    def clear(self):
        self.data.clear()

    def count(self):
        return len(self.data)
