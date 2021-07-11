import functools
import typing
import operator

from .models import Job, Args
from .utils import group_by


class _FuncWrapper:
    def __init__(self, func, model, client):
        self.func = func
        self.model = model
        self.client = client
        functools.update_wrapper(self, func)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def push(self, *args, **kwargs):
        job = Job(self.func, Args(self.model, args, kwargs))

        if self.client.eager_mode:
            self.client.bulk_call(self.func, [job])
        else:
            self.client.enqueue(job)

    def __repr__(self):
        return repr(self.func)


class BulkTask:
    def __init__(self, queue, eager_mode=False):
        self.queue = queue
        self.eager_mode = eager_mode

    def enqueue(self, job):
        self.queue.enqueue(job.serialize())

    def dequeue(self, quantity):
        items = self.queue.dequeue(quantity)
        return list(map(Job.deserialize, items))

    def bulk_task(self, func):
        type_hints = typing.get_type_hints(func)
        argument_type = list(type_hints.values())[0]
        model = typing.get_args(argument_type)[0]

        return _FuncWrapper(func, model, self)

    def bulk_call(self, func, jobs):
        try:
            func([job.args.as_model() for job in jobs])
        except Exception:
            size = len(jobs)

            # This is the broken job.
            if size == 1:
                self.capture_exception()
                for job in jobs:
                    self.enqueue(job)
            else:
                mid = size // 2
                jobs_bisect = jobs[:mid], jobs[mid:]
                for part in jobs_bisect:
                    if part:
                        self.bulk_call(func, part)

    def consume(self, quantity=500):
        jobs = self.dequeue(quantity)
        grouped = group_by(jobs, key=operator.attrgetter('func'))
        for func, grouped_jobs in grouped.items():
            self.bulk_call(func, grouped_jobs)

    def __call__(self, func):
        return self.bulk_task(func)

    def capture_exception(self):
        pass
