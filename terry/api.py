class Job:
    IDLE = 'idle'
    LOCKED = 'locked'
    CANCELLED = 'cancelled'
    COMPLETED = 'completed'

    def __init__(self, id_, reqs, args, version, *,
                 status=None,
                 created_at=None,
                 locked_at=None,
                 completed_at=None,
                 run_at=None,
                 worker_id=None,
                 worker_heartbeat=None,
                 worker_exception=None):
        self.id = id_
        self.reqs = reqs
        self.args = args
        self.version = version
        self.status = status or Job.IDLE
        self.created_at = created_at
        self.locked_at = locked_at
        self.completed_at = completed_at
        self.run_at = run_at
        self.worker_id = worker_id
        self.worker_heartbeat = worker_heartbeat
        self.worker_exception = worker_exception

    @property
    def failed(self):
        return self.worker_exception is not None


class RetriableError(Exception):
    pass


class ConcurrencyError(Exception):
    pass


class IJobController:
    def get_job(self, job_id):
        pass

    def create_job(self, job_id, *, reqs=None, args=None, run_at=None):
        pass

    def cancel_job(self, job_id, version):
        pass

    def delete_job(self, job_id, version):
        pass


class IWorkerController:
    def acquire_job(self, resources, worker_id):
        pass

    def heartbeat_job(self, job_id, version):
        pass

    def finalize_job(self, job_id, version, worker_exception=None):
        pass

    def requeue_job(self, job_id, version, run_at=None):
        pass
