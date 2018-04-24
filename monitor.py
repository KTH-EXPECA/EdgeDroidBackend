from collections import namedtuple
from multiprocessing import Process, RLock, Condition
import psutil
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from datetime import datetime


class ResourceMonitor():
    Sample = namedtuple('Sample', ['timestamp', 'cpu_load', 'avail_mem'])

    def __init__(self):
        self.cond = Condition(RLock())
        self.sampling_interval = 1.0 / 5  # 5 Hz
        self.samples = list()
        self.total_mem = psutil.virtual_memory()['total']

        self.scheduler = BackgroundScheduler(
            executors={'default': ProcessPoolExecutor(1)}
        )
        self.job = None

    def start(self):
        if not self.job:
            self.job = self.scheduler.add_job(ResourceMonitor._sample,
                                          'interval', args=(self,),
                                          seconds=self.sampling_interval)
            self.scheduler.start()
        else:
            raise RuntimeError('There is already a sampling job running!')

    def stop(self):
        if not self.job:
            return

        self.job.remove()
        self.scheduler.shutdown()

    def _sample(self):
        pass
