import signal
import time
from multiprocessing import Process, Barrier

import constants
from custom_logging.logging import LOGGER
import docker


class DockerManager(Process):
    class ShutdownException(Exception):
        pass

    @staticmethod
    def __signal_handler(*args):
        raise DockerManager.ShutdownException()

    def __init__(self, config):
        self.dck = docker.from_env()
        self.containers = list()
        self.docker_img = config.docker_img

        self.clients = config.clients
        self.cpu_cores = config.cpu_cores
        self.cpu_set = ','.join(map(str, self.cpu_cores))
        self.port_configs = config.port_configs

        super(DockerManager, self).__init__()

    def run(self):
        LOGGER.info('Spawning Docker containers...')
        try:
            for i, port_cfg in enumerate(self.port_configs):
                LOGGER.info(
                    f'Launching container {i + 1} of {self.clients}...')

                # register signal handler
                signal.signal(signal.SIGINT, DockerManager.__signal_handler)
                signal.signal(signal.SIGTERM, DockerManager.__signal_handler)

                self.containers.append(
                    self.dck.containers.run(
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

                LOGGER.info('Wait for container warm up...')
                time.sleep(5)
                LOGGER.info('Initialization done')

                while True:
                    time.sleep(1)

        except DockerManager.ShutdownException \
               or InterruptedError \
               or KeyboardInterrupt:
            pass

        finally:
            LOGGER.warning('Shutting down containers...')
            for cont in self.containers:
                cont.kill()
