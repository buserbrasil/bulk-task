from importlib import import_module


class JobQueue:
    def enqueue(self, job):
        raise NotImplementedError('must implement enqueue()')

    def dequeue(self, quantity):
        raise NotImplementedError('must implement dequeue()')

    def clear(self):
        raise NotImplementedError('must implement clear()')

    def count(self):
        raise NotImplementedError('must implement count()')

    def __len__(self):
        return self.count()


def queue_factory(queue_module) -> JobQueue:
    return import_module(queue_module).Queue()
