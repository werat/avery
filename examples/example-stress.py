#!/usr/bin/env python

import time
import random

from terry.controller import Controller
from terry.worker import Worker, JobChannel  # noqa


def work_func(channel: JobChannel):
    pass


def setup_backend(db_uri):
    import pymongo
    client = pymongo.MongoClient(db_uri)
    client.drop_database(client.get_default_database().name)


if __name__ == '__main__':
    # logging.basicConfig(format='%(asctime)s\t%(levelname)s:\t%(message)s', level=logging.INFO)

    db_uri = 'mongodb://localhost/terry-stress'

    # setup_backend(db_uri)

    controller = Controller(db_uri)

    workers = []
    max_cpu = 0
    max_ram = 0
    max_dsk = 0
    for i in range(16):
        resources = {
            'cpu': random.randint(1, 32),
            'ram': random.randint(1, 32),
            'dsk': random.randint(1, 32),
        }
        max_cpu = max(resources['cpu'], max_cpu)
        max_ram = max(resources['ram'], max_ram)
        max_dsk = max(resources['dsk'], max_dsk)

        worker = Worker('w-{}'.format(i+1), resources, work_func, controller)
        workers.append(worker)

    for worker in workers:
        worker.start()
    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        for worker in workers:
            worker.request_stop()
        for worker in workers:
            worker.join()

    # for _ in range(5000):
    #     job_id = controller.create_job_id()
    #     reqs = {
    #         'cpu': random.randint(1, max_cpu//3),
    #         'ram': random.randint(1, max_ram//3),
    #         'dsk': random.randint(1, max_dsk//3),
    #     }
    #     controller.create_job(job_id, reqs=reqs, args={'payload': 42})

    #     if _ % 1000 == 0:
    #         print(_, 'created')
