import pytest

from terry.controller import Controller
from terry.worker import Worker, BasicResourceManager


@pytest.fixture
def controller():
    db_uri = 'mongodb://localhost/terry-tests'

    import pymongo
    client = pymongo.MongoClient(db_uri)
    client.drop_database(client.get_default_database().name)
    yield Controller(db_uri)


@pytest.fixture
def resource_manager():
    return BasicResourceManager({'cpu': 2, 'ram': 4})


@pytest.fixture
def worker(resource_manager, controller):
    worker = Worker('test-worker', resource_manager, None, controller)
    worker.start()
    yield worker
    worker.stop()
