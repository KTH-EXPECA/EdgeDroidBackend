"""
 Copyright 2019 Manuel Olguín
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""

import time
from collections import namedtuple
from multiprocessing import Manager

import psutil
from apscheduler.executors.pool import ProcessPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler


class ResourceMonitor():

    def __init__(self, time_offset, sampling_freq=5):
        self.sampling_interval = 1.0 / sampling_freq  # 5 Hz by default
        self.total_mem = psutil.virtual_memory().total
        self.manager = Manager()
        self.samples = self.manager.list()
        self.time_offset = time_offset

        self.scheduler = BackgroundScheduler(
            executors={'default': ProcessPoolExecutor(1)}
        )
        self.job = None

    def start(self):
        if not self.job:
            self.job = self.scheduler.add_job(
                ResourceMonitor._sample,
                'interval',
                args=(self.samples, self.time_offset),
                seconds=self.sampling_interval
            )
            self.scheduler.start()
        else:
            raise RuntimeError('There is already a sampling job running!')

    def stop(self):
        if not self.job:
            return

        self.job.remove()
        self.scheduler.shutdown()

        self.job = None

    def shutdown(self, print_samples=False):
        self.stop()

        samples = self.samples._getvalue()
        self.manager.shutdown()

        if print_samples:
            from pprint import PrettyPrinter
            PrettyPrinter(indent=4).pprint(samples)
        return samples

    @staticmethod
    def _sample(samples, time_offset):

        cpu_load = psutil.cpu_percent(percpu=False)
        mem_status = psutil.virtual_memory()
        timestamp = time.time() * 1000.0 + time_offset
        # convert to milliseconds

        samples.append(
            {
                'cpu_load' : cpu_load,
                'mem_avail': mem_status.available,
                'timestamp': timestamp
            }
        )


if __name__ == '__main__':
    monitor = ResourceMonitor()
    monitor.start()
    time.sleep(5)
    monitor.shutdown(print_samples=True)
