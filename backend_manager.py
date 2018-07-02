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
