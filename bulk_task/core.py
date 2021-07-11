import functools
import typing
import operator

from .models import Job, Args
from .queue import queue_factory
from .utils import group_by


class _FuncWrapper:
    def __init__(self, func, model, *, queue, eager_mode):
        self.func = func
        self.model = model
        self.queue = queue
        self.eager_mode = eager_mode
        functools.update_wrapper(self, func)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def push(self, *args, **kwargs):
        job = Job(self.func, Args(self.model, args, kwargs))

        if self.eager_mode:
            bulk_call(self.func, [job])
        else:
            self.queue.enqueue(job)

    def __repr__(self):
        return repr(self.func)


def consume(quantity=500):
    queue = queue_factory()
    jobs = queue.dequeue(quantity)
    grouped = group_by(jobs, key=operator.attrgetter('func'))
    for func, grouped_jobs in grouped.items():
        try:
            bulk_call(func, grouped_jobs)
        except Exception:
            capture_exception()
            for job in jobs:
                queue.enqueue(job)


def bulk_call(func, jobs):
    func([job.args.as_model() for job in jobs])


def capture_exception():
    pass


class BulkTask:
    def __init__(self, queue_module=None, eager_mode=False):
        self.queue_module = queue_module
        self.eager_mode = eager_mode

    @property
    def queue(self):
        try:
            self._queue
        except AttributeError:
            self._queue = queue_factory(self.queue_module)
        return self._queue

    def enqueue(self, job):
        self.queue.enqueue(job)

    def bulk_task(self, func):
        type_hints = typing.get_type_hints(func)
        argument_type = list(type_hints.values())[0]
        model = typing.get_args(argument_type)[0]

        return _FuncWrapper(
            func, model, queue=self.queue, eager_mode=self.eager_mode)

    def consume(self, quantity=500):
        jobs = self.queue.dequeue(quantity)
        grouped = group_by(jobs, key=operator.attrgetter('func'))
        for func, grouped_jobs in grouped.items():
            try:
                bulk_call(func, grouped_jobs)
            except Exception:
                capture_exception()
                for job in jobs:
                    self.queue.enqueue(job)

    def __call__(self, func):
        return self.bulk_task(func)
