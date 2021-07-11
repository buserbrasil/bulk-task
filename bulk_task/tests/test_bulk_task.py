from dataclasses import dataclass
from typing import List
from unittest import mock

import fakeredis
import pytest

from bulk_task import BulkTask
from bulk_task.models import Job, Args
from bulk_task.backend import Redis


@dataclass
class DataclassModel:
    name: str


@pytest.fixture
def bulk_task():
    backend = Redis(client=fakeredis.FakeStrictRedis())
    return BulkTask(backend)


@pytest.fixture
def args():
    return Args(DataclassModel, ['walison'])


def echo(args):
    pass


def echo2(args):
    pass


def failing(args):
    for arg in args:
        if arg.name == 'error':
            raise Exception(arg.name)


@pytest.fixture
def job(args):
    return Job(echo, args)


@pytest.fixture
def job2(args):
    return Job(echo2, args)


def error(args):
    raise Exception


@pytest.fixture
def error_job(args):
    return Job(error, args)


def test_args_serialize(args):
    serialized = (
        'test_bulk_task.DataclassModel',
        {'args': ('walison',), 'kwargs': {}},
    )
    assert args.serialize() == serialized


def test_args_deserialize():
    serialized = (
        'test_bulk_task.DataclassModel',
        {'args': ('walison',), 'kwargs': {}},
    )
    args = Args.deserialize(serialized)

    assert isinstance(args, Args)
    assert args.model == DataclassModel
    assert args.args == ('walison',)
    assert args.kwargs == {}


def test_args_as_model(args):
    assert isinstance(args.as_model(), DataclassModel)


def test_job_serialize(job):
    serialized = (
        'test_bulk_task.echo',
        (
            'test_bulk_task.DataclassModel',
            {'args': ('walison',), 'kwargs': {}},
        )
    )
    assert job.serialize() == serialized


def test_job_deserialize():
    serialized = (
        'test_bulk_task.echo',
        (
            'test_bulk_task.DataclassModel',
            {'args': ('walison',), 'kwargs': {}},
        )
    )
    job = Job.deserialize(serialized)

    assert isinstance(job, Job)
    assert job.func == echo
    assert isinstance(job.args, Args)


def test_consume(job, job2, bulk_task):
    bulk_task.enqueue(job)
    bulk_task.enqueue(job2)
    with mock.patch.object(bulk_task, 'bulk_call') as mock_bulk_exec:
        bulk_task.consume()

    assert mock_bulk_exec.call_count == 2
    assert len(bulk_task.queue) == 0


def test_clear(job, job2, bulk_task):
    bulk_task.enqueue(job)
    bulk_task.enqueue(job2)
    bulk_task.queue.clear()
    with mock.patch.object(bulk_task, 'bulk_call') as mock_bulk_exec:
        bulk_task.consume()

    assert mock_bulk_exec.call_count == 0
    assert len(bulk_task.queue) == 0


def test_consume_handle_exception(error_job, bulk_task):
    """Deve remover o(s) job(s) da fila e capturar a exceção via sentry"""
    bulk_task.enqueue(error_job)
    with mock.patch.object(
        bulk_task, 'capture_exception'
    ) as mock_capture_exception:
        bulk_task.consume()

    assert len(bulk_task.queue) == 1
    mock_capture_exception.assert_called()


def test_lazy_bulk_dataclass_model(bulk_task):
    @bulk_task
    def func(args: List[DataclassModel]):
        pass

    func.push('walison')
    func.push('filipe')

    assert len(bulk_task.queue) == 2


def test_lazy_bulk_pydantic_model(bulk_task):
    pydantic = pytest.importorskip('pydantic')
    BaseModel = pydantic.BaseModel

    class PydanticModel(BaseModel):
        a: str

    @bulk_task
    def func(args: List[PydanticModel]):
        pass

    func.push(a='example')

    assert len(bulk_task.queue) == 1


def test_lazy_batch_bisect_errors(bulk_task):
    bulk_task.enqueue(Job(failing, Args(DataclassModel, ('walison',))))
    bulk_task.enqueue(Job(failing, Args(DataclassModel, ('error',))))
    bulk_task.enqueue(Job(failing, Args(DataclassModel, ('filipe',))))
    bulk_task.consume()
    assert len(bulk_task.queue) == 1
