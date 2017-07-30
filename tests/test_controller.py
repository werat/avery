from terry.api import Job


def test_create_job(controller):
    job_id = controller.create_job_id()
    controller.create_job(job_id, args={'payload': 42})
    job = controller.get_job(job_id)

    assert job.id == job_id
    assert job.status == Job.IDLE
    assert job.args == {'payload': 42}
    assert job.worker_id is None


def test_acquire_job(controller):
    job_id = controller.create_job_id()
    controller.create_job(job_id, reqs={'cpu': 1})

    job = controller.acquire_job({'cpu': 0}, 'test-worker')
    assert job is None

    job = controller.acquire_job({'cpu': 1}, 'test-worker')
    assert job.id == job_id
    assert job.worker_id == 'test-worker'

    job = controller.acquire_job({'cpu': 1}, 'test-worker')
    assert job is None


def test_acquire_job_multiple_requirements(controller):
    job_id = controller.create_job_id()
    controller.create_job(job_id, reqs={'cpu': 2, 'ram': 4})

    job = controller.acquire_job({'cpu': 4}, 'test-worker')
    assert job is None

    job = controller.acquire_job({'ram': 4}, 'test-worker')
    assert job is None

    job = controller.acquire_job({'cpu': 4, 'ram': 0}, 'test-worker')
    assert job is None

    job = controller.acquire_job({'cpu': 2, 'ram': 4, 'dsk': 4}, 'test-worker')
    assert job.id == job_id
    assert job.worker_id == 'test-worker'


def test_reacquire_job(controller):
    job_id = controller.create_job_id()
    controller.create_job(job_id, reqs={'cpu': 1})

    job = controller.acquire_job({'cpu': 1}, 'test-worker')
    assert job.id == job_id

    from datetime import datetime, timedelta
    fake_heartbeat = datetime.utcnow() - controller.HEARTBEAT_TIMEOUT - timedelta(minutes=1)
    controller._update_job(job.id, job.version, worker_heartbeat=fake_heartbeat)

    job = controller.acquire_job({'cpu': 1}, 'test-worker')
    assert job.id == job_id
