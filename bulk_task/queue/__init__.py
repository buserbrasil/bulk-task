from importlib import import_module

from .. import settings


class JobQueue:
    def enqueue(self, job):
        raise NotImplementedError('must implement enqueue()')

    def dequeue(self, quantity):
        raise NotImplementedError('must implement dequeue()')

    def clear(self):
        raise NotImplementedError('must implement clear()')


def queue_factory() -> JobQueue:
    queue_module = import_module(settings.BULK_TASK_QUEUE)
    return queue_module.Queue()
