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

import docker
from docker.models.containers import Container

import constants
from config import ExperimentConfig
from custom_logging.logging import LOGGER


class NullCPULoadManager:
    def start_cpu_load(self):
        pass

    def shutdown(self):
        pass


class CPULoadManager(NullCPULoadManager):

    def __init__(self, config: ExperimentConfig):
        self.target_load = config.target_load
        self.cores = config.cpu_cores
        self.docker = docker.from_env()
        self.container = Container()

    def start_cpu_load(self):
        LOGGER.info('Initiating artificial CPU load...')
        LOGGER.info('Target cores: %s', self.cores)
        LOGGER.info('Artificial load: %04.1f percent', self.target_load * 100)

        core_cmds = map(lambda c: f'-c {c}', self.cores)
        cmd = ' '.join(core_cmds) + f' -l {self.target_load} -d -1'

        self.container = self.docker.containers.run(
            constants.CPU_LOAD_IMG,
            command=cmd,
            detach=True,
            auto_remove=True,
        )

        LOGGER.info('Wait for CPU load to ramp up (2s)...')
        time.sleep(2)
        LOGGER.info('CPU load ready.')

    def shutdown(self):
        LOGGER.info('Shutting down artificial CPU load...')
        self.container.kill()
        LOGGER.info('Artificial CPU load shut down.')
