import pytest

from terry.controller import Controller
from terry.worker import Worker


@pytest.fixture
def controller():
    db_uri = 'mongodb://localhost/terry-tests'

    import pymongo
    client = pymongo.MongoClient(db_uri)
    client.drop_database(client.get_default_database().name)
    yield Controller(db_uri)


@pytest.fixture
def worker(controller):
    worker = Worker('test-worker', {'cpu': 2, 'ram': 4}, None, controller)
    worker.start()
    yield worker
    worker.stop()
