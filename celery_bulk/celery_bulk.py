from collections import defaultdict
import functools
import typing
import operator

from django.conf import settings
from django.utils.module_loading import import_string
from sentry_sdk.api import capture_exception

from commons.redis import FIFO


def _group_by(iterable, key):
    buckets = defaultdict(list)
    for i in iterable:
        buckets[key(i)].append(i)
    return buckets


class Args:
    def __init__(self, model, args, kwargs=None):
        self.model = model
        self.args = tuple(args)
        if kwargs is None:
            kwargs = {}
        self.kwargs = kwargs

    @classmethod
    def deserialize(cls, data):
        model_cls, params = import_string(data[0]), data[1]
        return cls(model_cls, params['args'], params['kwargs'])

    def serialize(self):
        params = {'args': self.args, 'kwargs': self.kwargs}
        return f'{self.model.__module__}.{self.model.__name__}', params

    def as_model(self):
        return self.model(*self.args, **self.kwargs)


class Job:
    def __init__(self, func, args):
        self.func = func
        self.args = args

    @classmethod
    def deserialize(cls, data):
        func, args = import_string(data[0]), Args.deserialize(data[1])
        return cls(func, args)

    def serialize(self):
        return (
            f'{self.func.__module__}.{self.func.__name__}',
            self.args.serialize(),
        )


class JobQueue:
    def enqueue(self, job):
        raise NotImplementedError('must implement enqueue()')

    def dequeue(self, quantity):
        raise NotImplementedError('must implement dequeue()')

    def clear(self):
        raise NotImplementedError('must implement clear()')


class RedisQueue(JobQueue):
    prefix = ':jobs'

    def __init__(self):
        self._queue = FIFO('lazy_batch')

    def enqueue(self, job):
        self._queue.push(self.prefix, job.serialize())

    def dequeue(self, quantity):
        items = self._queue.popmany(self.prefix, quantity)
        return [Job.deserialize(item) for item in items]

    def __len__(self):
        return self._queue.count(self.prefix)

    def clear(self):
        self._queue.clear(self.prefix)


def queue_factory() -> JobQueue:
    queue_cls = import_string(settings.BATCH_TASK_QUEUE)
    return queue_cls()


queue = queue_factory()


class _FuncWrapper:
    def __init__(self, func, model):
        self.func = func
        self.model = model
        functools.update_wrapper(self, func)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def push(self, *args, **kwargs):
        job = Job(self.func, Args(self.model, args, kwargs))

        if _on_eager_mode():
            batch_call(self.func, [job])
        else:
            queue.enqueue(job)

    def __repr__(self):
        return repr(self.func)


def batch_task(func):
    type_hints = typing.get_type_hints(func)
    argument_type = list(type_hints.values())[0]
    model = typing.get_args(argument_type)[0]

    return _FuncWrapper(func, model)


def consume(quantity=500):
    jobs = queue.dequeue(quantity)
    grouped = _group_by(jobs, key=operator.attrgetter('func'))
    for func, grouped_jobs in grouped.items():
        try:
            batch_call(func, grouped_jobs)
        except Exception:
            capture_exception()
            for job in jobs:
                queue.enqueue(job)


def batch_call(func, jobs):
    func([job.args.as_model() for job in jobs])


def _on_eager_mode():
    return getattr(settings, 'BATCH_TASK_EAGER', False)
