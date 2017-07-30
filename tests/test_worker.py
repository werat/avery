import os
import signal
import time

from threading import Event, Thread

import pytest

from terry.api import Job


@pytest.mark.timeout(10)
def test_worker_job_success(controller, worker):
    job_started = Event()

    def work_func(channel):
        job_started.set()

    worker._worker_func = work_func

    job_id = controller.create_job_id()
    controller.create_job(job_id, reqs={'cpu': 1})

    job_started.wait()  # wait until worker start the job
    worker.stop()

    job = controller.get_job(job_id)

    assert job.status == Job.COMPLETED
    assert job.worker_exception is None
    assert job.worker_id == worker.id


@pytest.mark.timeout(10)
def test_worker_job_exception(controller, worker):
    job_started = Event()

    def work_func(channel):
        job_started.set()
        raise Exception('exception from job')

    worker._worker_func = work_func

    job_id = controller.create_job_id()
    controller.create_job(job_id, reqs={'cpu': 1})

    job_started.wait()  # wait until worker start the job
    worker.stop()

    job = controller.get_job(job_id)

    assert job.status == Job.COMPLETED
    assert job.worker_exception['reason'] == 'exception from job'
    assert job.worker_id == worker.id


@pytest.mark.timeout(10)
def test_worker_signal_handling(controller, worker):

    def handler(signum, frame):
        worker.request_stop()

    def signal_thread():
        time.sleep(1)  # wait until worker.join() is called
        os.kill(os.getpid(), signal.SIGUSR1)

    signal.signal(signal.SIGUSR1, handler)

    thread = Thread(target=signal_thread)
    thread.start()

    worker.join()

    assert not worker.is_running


@pytest.mark.timeout(10)
def test_worker_is_busy(controller, worker):
    job_started = Event()
    job_may_complete = Event()

    def work_func(channel):
        job_started.set()
        job_may_complete.wait()

    worker._worker_func = work_func

    job_id = controller.create_job_id()
    controller.create_job(job_id, reqs={'cpu': 1})

    job_started.wait()

    assert worker.is_busy

    job_may_complete.set()
    worker.stop()

    assert not worker.is_busy


@pytest.mark.timeout(10)
def test_worker_requeue_job_on_error(controller, worker):
    second_run = Event()

    def work_func(channel):
        channel.requeue_job_on_error()

        if second_run.is_set():
            # complete successfully
            worker.request_stop()
        else:
            second_run.set()
            raise Exception('error on first run')

    worker._worker_func = work_func

    job_id = controller.create_job_id()
    controller.create_job(job_id, reqs={'cpu': 1})

    worker.join()

    job = controller.get_job(job_id)
    assert job.status == Job.COMPLETED
    assert job.worker_exception is None
