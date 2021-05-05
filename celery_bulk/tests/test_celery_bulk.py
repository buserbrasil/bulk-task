from typing import List
from dataclasses import dataclass
from unittest import mock

import pytest

from celery_bulk.core import consume, batch_task
from celery_bulk.models import Job, Args
from celery_bulk.queue import queue_factory


@dataclass
class DataclassModel:
    name: str


@pytest.fixture
def args():
    return Args(DataclassModel, ['walison'])


def echo(args):
    pass


def echo2(args):
    pass


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


@pytest.fixture
def queue():
    return queue_factory()


@pytest.fixture(autouse=True)
def clean_queue():
    yield
    queue = queue_factory()
    queue.clear()


def test_args_serialize(args):
    serialized = (
        'test_celery_bulk.DataclassModel',
        {'args': ('walison',), 'kwargs': {}},
    )
    assert args.serialize() == serialized


def test_args_deserialize():
    serialized = (
        'test_celery_bulk.DataclassModel',
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
        'test_celery_bulk.echo',
        (
            'test_celery_bulk.DataclassModel',
            {'args': ('walison',), 'kwargs': {}},
        )
    )
    assert job.serialize() == serialized


def test_job_deserialize():
    serialized = (
        'test_celery_bulk.echo',
        (
            'test_celery_bulk.DataclassModel',
            {'args': ('walison',), 'kwargs': {}},
        )
    )
    job = Job.deserialize(serialized)

    assert isinstance(job, Job)
    assert job.func == echo
    assert isinstance(job.args, Args)


@mock.patch('celery_bulk.core.batch_call')
def test_consume(mock_batch_exec, job, job2, queue):
    queue.enqueue(job)
    queue.enqueue(job2)
    consume()

    assert mock_batch_exec.call_count == 2
    assert len(queue) == 0


@mock.patch('celery_bulk.core.capture_exception')
def test_consume_handle_exception(mock_capture_exception, error_job, queue):
    """Deve remover o(s) job(s) da fila e capturar a exceção via sentry"""
    queue.enqueue(error_job)
    consume()

    assert len(queue) == 1
    mock_capture_exception.assert_called()


def test_lazy_batch_dataclass_model(queue):
    @batch_task
    def func(args: List[DataclassModel]):
        pass

    func.push('walison')
    func.push('filipe')

    assert len(queue) == 2


def test_lazy_batch_pydantic_model(queue):
    pydantic = pytest.importorskip('pydantic')
    BaseModel = pydantic.BaseModel

    class PydanticModel(BaseModel):
        a: str

    @batch_task
    def func(args: List[PydanticModel]):
        pass

    func.push(a='example')

    assert len(queue) == 1
