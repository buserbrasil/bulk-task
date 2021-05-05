from collections import defaultdict
import functools
import typing
import operator

from . import settings
from .models import Job, Args
from .queue import queue_factory


def _group_by(iterable, key):
    buckets = defaultdict(list)
    for i in iterable:
        buckets[key(i)].append(i)
    return buckets


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
            queue = queue_factory()
            queue.enqueue(job)

    def __repr__(self):
        return repr(self.func)


def batch_task(func):
    type_hints = typing.get_type_hints(func)
    argument_type = list(type_hints.values())[0]
    model = typing.get_args(argument_type)[0]

    return _FuncWrapper(func, model)


def consume(quantity=500):
    queue = queue_factory()
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
    return settings.BATCH_TASK_EAGER


def capture_exception():
    pass
