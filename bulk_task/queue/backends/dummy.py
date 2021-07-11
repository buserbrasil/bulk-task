from ...models import Job
from .. import JobQueue


class Queue(JobQueue):
    def __init__(self):
        self.data = []

    def enqueue(self, job):
        self.data.append(job.serialize())

    def dequeue(self, quantity):
        items = []
        for i in range(quantity):
            try:
                item = self.data.pop(0)
            except IndexError:
                break
            items.append(item)
        return list(map(Job.deserialize, items))

    def clear(self):
        self.data.clear()

    def count(self):
        return len(self.data)
