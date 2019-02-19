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

import constants
from config import ExperimentConfig
from custom_logging.logging import LOGGER


class BackendManager:

    def __init__(self, config: ExperimentConfig):
        self.docker = docker.from_env()
        self.containers = list()
        self.docker_img = config.docker_img

        self.clients = config.clients
        self.cpu_cores = config.cpu_cores
        self.cpu_set = ','.join(map(str, self.cpu_cores))
        self.port_configs = config.port_configs

    def spawn_backends(self):
        LOGGER.info('Spawning Docker containers...')
        for i, port_cfg in enumerate(self.port_configs):
            LOGGER.info(
                f'Launching container {i + 1} of {self.clients}...')

            self.containers.append(
                self.docker.containers.run(
                    self.docker_img,
                    detach=True,
                    auto_remove=True,
                    ports={
                        constants.DEFAULT_VIDEO_PORT  : port_cfg.video,
                        constants.DEFAULT_RESULT_PORT : port_cfg.result,
                        constants.DEFAULT_CONTROL_PORT: port_cfg.control
                    },
                    cpuset_cpus=self.cpu_set
                )
            )

            LOGGER.info('Wait 5s for container warm up...')
            time.sleep(5.0)
            LOGGER.info('Initialization done')

    def shutdown(self):
        LOGGER.warning('Shutting down containers...')
        for cont in self.containers:
            cont.kill()
        LOGGER.warning('Containers shut down!')
