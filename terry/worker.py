import ctypes
import logging
import math
import random
import sys
import threading
import time
import traceback

from .api import Job, ConcurrencyError, RetriableError


__all__ = [
    'ResourceManager', 'BasicResourceManager',
    'InterruptJob', 'Worker', 'JobChannel'
]


class InterruptJob(Exception):
    pass


class _RequeueRequested(Exception):
    pass


class JobContext:
    def __init__(self, worker_id, job):
        self.worker_id = worker_id
        self.job = job
        self.outdated = False
        self.requeue_requested = False
        self.requeue_run_at = None
        self.requeue_on_error = False

    def update(self, job):
        self.job = job
        self.outdated = False

    @property
    def cancelled(self):
        return self.job.status == Job.CANCELLED

    @property
    def revoked(self):
        return self.job.worker_id != self.worker_id

    def requeue_job(self, run_at):
        self.requeue_requested = True
        self.requeue_run_at = run_at


class JobChannel:
    def __init__(self, ctx):
        self.__ctx = ctx

    @property
    def job(self):
        return self.__ctx.job

    @property
    def cancelled(self):
        return self.__ctx.cancelled

    @property
    def revoked(self):
        return self.__ctx.revoked

    @property
    def cancelled_or_revoked(self):
        return self.cancelled or self.revoked

    def interrupt_if_requested(self):
        if self.cancelled or self.revoked:
            raise InterruptJob

    def requeue_job(self, run_at=None):
        self.__ctx.requeue_job(run_at)
        raise _RequeueRequested

    def requeue_job_on_error(self):
        self.__ctx.requeue_on_error = True


class WorkerThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(WorkerThread, self).__init__(*args, **kwargs)
        self.interrupt_requested = False
        self.interrupted = False
        self.exc_info = None

    @property
    def has_failed(self):
        return self.exc_info is not None

    def run(self):
        try:
            super(WorkerThread, self).run()
        except InterruptJob:
            self.interrupted = True
        except _RequeueRequested:
            pass
        except:
            self.exc_info = sys.exc_info()


def substract_resources(r1, r2):
    result = r1.copy()
    for k in r2:
        v = r1[k] - r2[k]
        if v > 0:
            result[k] = v
        else:
            result.pop(k)
    return result


class ResourceManager:
    def get_current_resources(self):
        pass

    def acquire(self):
        pass

    def reclaim(self, resources):
        pass


class BasicResourceManager(ResourceManager):
    def __init__(self, resources):
        self._resources = resources
        self._lock = threading.Lock()

    def get_current_resources(self):
        with self._lock:
            return self._resources.copy()

    def acquire(self):
        with self._lock:
            result = self._resources.copy()
            self._resources = {}
        return result

    def reclaim(self, resources):
        with self._lock:
            for k in resources:
                self._resources[k] = self._resources.get(k, 0) + resources[k]


class Worker:
    def __init__(self, id_, resources, worker_func, controller, *, interrupt_via_exception=False):
        self._id = id_
        self._resources = resources
        self._worker_func = worker_func
        self._controller = controller
        self._interrupt_via_exception = interrupt_via_exception

        # current job and worker thread
        self._job_ctx = None
        self._worker_thread = None

        self._stop = threading.Event()
        self._force_stop = threading.Event()

        # TODO: handle exception in _main_loop
        self._main_loop_thread = threading.Thread(target=self._loop)

        self.logger = logging.getLogger(__name__)

    @property
    def id(self):
        return self._id

    @property
    def is_running(self):
        return self._main_loop_thread.is_alive()

    @property
    def is_busy(self):
        return self._job_ctx is not None

    def start(self):
        self.logger.info('[%s] Available resources %r', self._id, self._resources.get_current_resources())
        self.logger.info('[%s] Starting worker...', self._id)
        self._main_loop_thread.start()

    def request_stop(self):
        self.logger.info('[%s] Got request to stop...', self._id)
        self._stop.set()

    def join(self, timeout=None):
        if timeout is None:
            # join without timeout will block signal handling
            while self._main_loop_thread.is_alive():
                self._main_loop_thread.join(0.1)
        else:
            self._main_loop_thread.join(timeout)

    def stop(self):
        self.request_stop()
        self.join()

    def _reclaim_resources(self, resources):
        self._resources.reclaim(resources)
        self.logger.debug('[%s] Reclaimed resources: %r', self._id, resources)

    def _reset_current_job(self):
        assert self._job_ctx is not None
        assert self._worker_thread is not None
        assert not self._worker_thread.is_alive()

        self._reclaim_resources(self._job_ctx.job.reqs)
        self._job_ctx = None
        self._worker_thread = None

    def _should_requeue_current_job(self):
        assert self._job_ctx is not None
        assert self._worker_thread is not None
        assert not self._worker_thread.is_alive()

        return self._job_ctx.requeue_requested or \
            self._worker_thread.has_failed and self._job_ctx.requeue_on_error

    def _loop(self):
        #
        retry_delay = 0
        #
        while True:
            if self._stop.is_set() and self._job_ctx is None:
                break

            if self._force_stop.is_set():
                break

            if retry_delay:
                # aka exponential backoff for retriable errors
                time.sleep(retry_delay)

            try:
                if self._job_ctx is None:
                    self._try_acquire_job()

                elif self._job_ctx.outdated:
                    self._try_update_current_job()

                elif self._job_ctx.cancelled or self._job_ctx.revoked:
                    self._wait_for_worker_thread_and_cleanup()

                elif self._worker_thread.is_alive():
                    self._try_heartbeat_current_job()

                elif self._should_requeue_current_job():
                    self._try_requeue_current_job()

                else:  # worker has finished processing the job
                    self._try_finalize_current_job()

            except RetriableError:
                retry_delay = 1 if retry_delay == 0 else min(10, retry_delay * 2)
            else:
                retry_delay = 0

    def _try_acquire_job(self):
        # lock resources from manager
        resources = self._resources.acquire()
        reclaimed = False
        try:
            self.logger.debug('[%s] Acquired resources: %r', self._id, resources)
            try:
                job = self._controller.acquire_job(resources, self._id)
            except ConcurrencyError:
                job = None

            if job:
                self._job_ctx = JobContext(self.id, job)
                self.logger.info('[%s] Acquired job %s', self._id, self._job_ctx.job.id)
                self._worker_thread = WorkerThread(target=self._worker_func, args=(JobChannel(self._job_ctx),))
                self._worker_thread.daemon = True  # to make force stop possible
                self._worker_thread.start()
                # reclaim leftovers
                self._reclaim_resources(substract_resources(resources, job.reqs))
                reclaimed = True
            else:
                self._reclaim_resources(resources)
                reclaimed = True
                time.sleep(math.e - random.random())
        except Exception:
            if not reclaimed:
                self._reclaim_resources(resources)
            raise

    def _try_update_current_job(self):
        job = self._controller.get_job(self._job_ctx.job.id)

        self._job_ctx.update(job)

        if self._job_ctx.cancelled:
            self.logger.info('[%s] It seems job %s was cancelled', self._id, self._job_ctx.job.id)

        if self._job_ctx.revoked:
            self.logger.info('[%s] It seems job %s was taken from us', self._id, self._job_ctx.job.id)

    def _wait_for_worker_thread_and_cleanup(self):
        if self._worker_thread.is_alive():
            if self._interrupt_via_exception and not self._worker_thread.interrupt_requested:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(self._worker_thread.ident),
                                                           ctypes.py_object(InterruptJob))
                self._worker_thread.interrupt_requested = True
                self.logger.info('[%s] Worker thread with job %s was interrupted via exception',
                                 self._id, self._job_ctx.job.id)

            self._worker_thread.join(math.pi - random.random())
        else:
            self.logger.info('[%s] Processing of job %s was terminated', self._id, self._job_ctx.job.id)
            self._reset_current_job()

    def _try_heartbeat_current_job(self):
        try:
            job = self._controller.heartbeat_job(self._job_ctx.job.id, self._job_ctx.job.version)
        except ConcurrencyError:
            job = None

        if job:
            self._job_ctx.update(job)
            self._worker_thread.join(math.pi - random.random())
        else:
            self._job_ctx.outdated = True
            self.logger.info('[%s] Failed to heartbeat job %s due to version mismatch',
                             self._id, self._job_ctx.job.id)

    def _try_requeue_current_job(self):
        assert not self._worker_thread.is_alive()
        # requeue current job with new run_at time
        try:
            self._controller.requeue_job(self._job_ctx.job.id, self._job_ctx.job.version,
                                         run_at=self._job_ctx.requeue_run_at)
        except ConcurrencyError:
            self._job_ctx.outdated = True
            self.logger.info('[%s] Failed to mark job %s as completed due to version mismatch',
                             self._id, self._job_ctx.job.id)
        else:
            self.logger.info('[%s] Job %s has been requeued', self._id, self._job_ctx.job.id)
            self._reset_current_job()

    def _try_finalize_current_job(self):
        assert not self._worker_thread.is_alive()
        # worker thread has finished, we should mark job as COMPLETED
        if self._worker_thread.has_failed:
            reason = str(self._worker_thread.exc_info[1])
            tback = ''.join(traceback.format_exception(*self._worker_thread.exc_info))
            worker_exception = {'reason': reason, 'traceback': tback}
        else:
            worker_exception = None

        try:
            job = self._controller.finalize_job(self._job_ctx.job.id, self._job_ctx.job.version,
                                                worker_exception=worker_exception)
        except ConcurrencyError:
            job = None

        if job:
            self.logger.info('[%s] Job %s has been processed', self._id, self._job_ctx.job.id)
            self._reset_current_job()
        else:
            self._job_ctx.outdated = True
            self.logger.info('[%s] Failed to mark job %s as completed due to version mismatch',
                             self._id, self._job_ctx.job.id)
