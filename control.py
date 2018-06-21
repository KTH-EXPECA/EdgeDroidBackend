#!/usr/bin/env python3
import os
import csv
import hashlib
import itertools
import json
import shlex
import signal
import subprocess
import time
from multiprocessing import Barrier, Process
from multiprocessing.pool import Pool
from random import shuffle
from socket import *

import click
import docker
import ntplib
import numpy as np
import psutil
import toml

import constants
from client import Client
from config import ExperimentConfig, RecursiveNestedDict
from custom_logging.logging import LOGGER
from monitor import ResourceMonitor

CPU_CFS_PERIOD = 100000  # 100 000 microseconds, 100 ms


def set_keepalive_linux(sock, after_idle_sec=1, interval_sec=3, max_fails=5):
    """Set TCP keepalive on an open socket.

    It activates after 1 second (after_idle_sec) of idleness,
    then sends a keepalive ping once every 3 seconds (interval_sec),
    and closes the connection after 5 failed ping (max_fails), or 15 seconds
    """
    sock.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
    sock.setsockopt(IPPROTO_TCP, TCP_KEEPIDLE, after_idle_sec)
    sock.setsockopt(IPPROTO_TCP, TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(IPPROTO_TCP, TCP_KEEPCNT, max_fails)


def send_step(client, index, checksum, data):
    client.send_step(index, checksum, data)


def send_config(client):
    client.send_config()


# def fetch_traces(client):
#     client.fetch_traces()


def run_exp(client, stagger_time):
    time.sleep(stagger_time)
    client.run_experiment()
    client.wait_for_experiment_finish()


def close_conn(client):
    client.close()


def ntp_sync(client):
    client.ntp_sync()


def get_stats(client):
    return client.get_remote_stats()


class Experiment:

    def __init__(self, config, host, port, output_dir):

        with open(config, 'r') as f:
            LOGGER.info('Loading config')
            self.config = ExperimentConfig(
                toml.load(f, _dict=RecursiveNestedDict))

        LOGGER.warning('Loaded config from %s', config)
        LOGGER.warning('Output directory: %s', output_dir)

        self.clients = list()
        self.host = host
        self.port = port
        self.tcpdump_proc = None
        self.output_dir = output_dir

        self.docker_barrier = Barrier(2)
        self.docker_proc = Process(target=Experiment._init_docker,
                                   args=(self.config, self.docker_barrier))

        self.ntp_client = ntplib.NTPClient()
        self.offset = 0

        LOGGER.info('Experiment ID: %s', self.config.name)
        LOGGER.info('Clients: %d', self.config.clients)
        LOGGER.info('Runs: %d', self.config.runs)

    def shutdown(self, e=None):
        LOGGER.warning('Shut down!')

        if e:
            LOGGER.critical(e)

        try:
            for client in self.clients:
                client.shutdown()
        except Exception as e:
            LOGGER.error(
                'Something went wrong while shutting down clients'
            )
            LOGGER.error(e)

        try:
            if self.tcpdump_proc:
                self.tcpdump_proc.send_signal(signal.SIGINT)
        except Exception as e:
            LOGGER.error(
                'Something went wrong while shutting down TCPDUMP'
            )
            LOGGER.error(e)

        try:
            self.docker_barrier.wait()
            if self.docker_proc:
                self.docker_proc.join()
        except Exception as e:
            LOGGER.error(
                'Something went wrong while shutting down Docker containers'
            )
            LOGGER.error(e)

    @staticmethod
    def _init_docker(config: ExperimentConfig, barrier: Barrier):
        LOGGER.info('Spawning Docker containers...')
        dck = docker.from_env()
        containers = list()
        try:
            LOGGER.warning('Limiting containers to {} out of {} CPUs'
                           .format(config.num_cpus,
                                   psutil.cpu_count()))
            cpuset = '{}-{}'.format(0, config.num_cpus - 1)

            for i, port_cfg in enumerate(config.port_configs):
                LOGGER.info('Launching container {} of {}'
                            .format(i + 1, len(config.port_configs)))

                containers.append(
                    dck.containers.run(
                        constants.LEGO_DOCKER_IMG,
                        detach=True,
                        auto_remove=True,
                        ports={
                            constants.DEFAULT_VIDEO_PORT  : port_cfg.video,
                            constants.DEFAULT_RESULT_PORT : port_cfg.result,
                            constants.DEFAULT_CONTROL_PORT: port_cfg.control
                        },
                        cpuset_cpus=cpuset
                    )
                )

            LOGGER.info('Wait for container warm up...')
            time.sleep(5)
            LOGGER.info('Initialization done')

            barrier.wait()

        except InterruptedError:
            pass
        except Exception as e:
            LOGGER.critical("Error while spawning Docker containers!")
            raise e
        finally:
            LOGGER.warning('Shutting down containers...')
            for cont in containers:
                cont.kill()

    def _init_tcpdump(self, run_path: str):
        LOGGER.info('Initializing TCP dump...')
        LOGGER.info('TCPdump directory: {}'.format(run_path))
        port_cmds = list()
        for port_cfg in self.config.port_configs:
            cmds = [
                'port {}'.format(port_cfg.video),
                'port {}'.format(port_cfg.result),
                'port {}'.format(port_cfg.control),
            ]

            port_cmds.append(' or '.join(cmds))

        port_cmds = [' or '.join(port_cmds)]
        tcpdump = shlex.split(' '.join(constants.TCPDUMP_CMD_PREFIX +
                                       port_cmds +
                                       constants.TCPDUMP_CMD_SUFFIX))

        self.tcpdump_proc = subprocess.Popen(tcpdump, cwd=run_path)
        if self.tcpdump_proc.poll():
            raise RuntimeError('Could not start TCPDUMP!')

    def _gen_config_for_client(self, client_index):
        c = dict()
        c['experiment_id'] = self.config.name
        c['client_id'] = client_index
        c['steps'] = len(self.config.trace_steps)
        c['ports'] = self.config.port_configs[client_index]
        c['ntp_server'] = self.config.ntp_servers[0]
        # TODO change in the future (to work with more than one server)
        return c

    def _pollNTPServer(self):
        LOGGER.info('Getting NTP offset')
        sync_cnt = 0
        cum_offset = 0
        while sync_cnt < constants.DEFAULT_NTP_POLL_COUNT:
            try:
                res = self.ntp_client.request(
                    self.config.ntp_servers[0],
                    version=4
                )

                cum_offset += res.offset
                sync_cnt += 1
            except ntplib.NTPException:
                continue

        self.offset = (cum_offset * 1000.0) / sync_cnt
        # convert to milliseconds

        LOGGER.info('Got NTP offset from %s', self.config.ntp_servers[0])
        LOGGER.info('Offset: %f ms', self.offset)

    def execute(self):
        server_socket = None
        error = None
        try:
            with socket(AF_INET, SOCK_STREAM) as server_socket:
                server_socket.bind((self.host, self.port))
                server_socket.listen(self.config.clients)
                server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                LOGGER.info('Listening on {}:{}'.format(self.host, self.port))

                # accept N clients for the experiment
                LOGGER.info('Waiting for {} clients to connect...'
                            .format(self.config.clients))

                with Pool(2) as pool:
                    self.docker_proc.start()

                    config_send = []
                    for i in range(self.config.clients):
                        conn, addr = server_socket.accept()
                        set_keepalive_linux(conn, max_fails=100)  # 5 minutes
                        client = Client(conn, addr,
                                        self._gen_config_for_client(i))
                        self.clients.append(client)

                        LOGGER.info(
                            '{} out of {} clients connected: {}:{}'.format(
                                i + 1, self.config.clients, *addr
                            ))

                        LOGGER.info("Sending config...")
                        config_send.append(
                            pool.apply_async(send_config, args=(client,))
                        )

                    for result in config_send:
                        result.wait()

                    # have the clients fetch traces 2 at the time to avoid
                    # congestion in the network
                    # LOGGER.info("Triggering trace download...")
                    # pool.map(fetch_traces, self.clients)

                    LOGGER.info("Pushing step files to clients...")
                    for i, path in enumerate(self.config.trace_steps):
                        with open(path, 'rb') as step:
                            step_data = step.read()
                            checksum = hashlib.md5(step_data).hexdigest()

                        pool.starmap(send_step,
                                     zip(
                                         self.clients,
                                         itertools.repeat(i + 1),
                                         itertools.repeat(checksum),
                                         itertools.repeat(step_data)
                                     ))

                    for r in range(self.config.runs):
                        LOGGER.info('Executing run {} out of {}'.format(
                            r + 1, self.config.runs
                        ))

                        run_path = self.output_dir + '/run_{}/'.format(r + 1)
                        if os.path.exists(run_path):
                            if not os.path.isdir(run_path):
                                raise RuntimeError('Output path {} already '
                                                   'exists and is not a '
                                                   'directory!'.format(run_path)
                                                   )
                        else:
                            os.mkdir(run_path)

                        self._pollNTPServer()

                        LOGGER.info('Trigger client NTP sync')
                        pool.map(ntp_sync, self.clients)

                        self._init_tcpdump(run_path)
                        LOGGER.info('TCPdump warmup...')
                        time.sleep(5)

                        LOGGER.info('Starting resource monitor...')
                        monitor = ResourceMonitor(self.offset)
                        monitor.start()

                        # All clients are ready, now let's run the experiment!
                        # Stagger client experiment start to avoid weird
                        # synchronous
                        # effects on the processing times...
                        LOGGER.info('Execute experiment!')

                        # shuffle clients before each run
                        shuffle(self.clients)

                        # for client in self.clients:
                        #    time.sleep(constants.DEFAULT_STAGGER_INTERVAL)
                        #    client.run_experiment()

                        # self.clients.clear()

                        start_times = np.random.uniform(
                            0,
                            constants.DEFAULT_START_WINDOW,
                            len(self.clients)
                        )

                        start_timestamp = time.time() * 1000.0
                        with Pool(len(self.clients)) as exec_pool:
                            exec_pool.starmap(
                                run_exp,
                                zip(self.clients, start_times)
                            )
                        end_timestamp = time.time() * 1000.0

                        # LOGGER.info('Waiting for {} clients to
                        # reconnect...'
                        #       .format(self.config['clients']))

                        # for i in range(self.config['clients']):
                        #     conn, addr = server_socket.accept()
                        #     set_keepalive_linux(conn,
                        #                         max_fails=100)  # 5 minutes
                        #     self.clients.append(Client(conn, addr))
                        #     LOGGER.info(
                        #         '{} out of {} clients '
                        #         'reconnected: {}:{}'.format(
                        #             i + 1, self.config['clients'], *addr
                        #         )
                        #     )

                        # all clients reconnected
                        # wait a second before terminating TCPdump
                        LOGGER.info('Shut down monitor.')
                        system_stats = monitor.shutdown()

                        time.sleep(1)
                        LOGGER.info('Terminate TCPDUMP')
                        self.tcpdump_proc.send_signal(signal.SIGINT)
                        self.tcpdump_proc.wait()

                        LOGGER.info('Get stats from clients!')
                        stats = pool.map(get_stats, self.clients)

                        # store this runs' system stats
                        with open(run_path + constants.SYSTEM_STATS, 'w') as f:
                            fieldnames = ['cpu_load', 'mem_avail', 'timestamp']
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(system_stats)

                        # store the client stats
                        for stat_coll in stats:
                            # stat_coll['server_offset'] = self.offset
                            client_index = stat_coll['client_id']
                            with open(run_path +
                                      constants.CLIENT_STATS.format(
                                          client_index
                                      ), 'w') as f:
                                json.dump(stat_coll, f)

                        # save server stats:
                        with open(run_path + constants.SERVER_STATS, 'w') as f:
                            json.dump({
                                'server_offset': self.offset,
                                'run_start'    : start_timestamp + self.offset,
                                'run_end'      : end_timestamp + self.offset
                            }, f)

        except Exception as e:
            error = e
        finally:
            try:
                if server_socket:
                    # server_socket.shutdown(SHUT_RDWR)
                    server_socket.close()
            except Exception as e:
                LOGGER.critical('Error closing server socket.')
                LOGGER.critical(e)

            self.shutdown(e=error)


@click.command(help='Gabriel Trace Demo control server, version {}'
               .format(constants.CONTROL_SERVER_VERSION))
@click.argument('experiment_directory',
                type=click.Path(
                    exists=True,
                    dir_okay=True,
                    file_okay=False,
                    resolve_path=True
                ))
@click.option('--host', type=str, default=constants.DEFAULT_CONTROLSERVER_HOST,
              help='Address to which bind this server instance.',
              show_default=True)
@click.option('--port', type=int, default=constants.DEFAULT_CONTROLSERVER_PORT,
              help='Port on which to listen for incoming connection.',
              show_default=True)
@click.option('--experiment_config', type=str,
              default=constants.DEFAULT_EXPERIMENT_CONFIG_FILENAME,
              help='Filename of the experiment config JSON file.',
              show_default=True)
@click.option('--output_dir', show_default=False,
              type=click.Path(
                  exists=True,
                  dir_okay=True,
                  file_okay=False,
                  resolve_path=True
              ),
              help='Output directory for result files. '
                   'Defaults to the experiment directory')
def execute(experiment_directory, experiment_config, host, port,
            output_dir=None):
    config_path = os.path.join(experiment_directory, experiment_config)
    if not os.path.exists(config_path):
        raise click.UsageError(
            'No experiment config file named {} found in {}'
                .format(experiment_config, experiment_directory)
        )

    if not output_dir:
        output_dir = experiment_directory

    LOGGER.info('Starting Control server version {}'
                .format(constants.CONTROL_SERVER_VERSION))
    e = Experiment(config_path, host, port, output_dir)
    e.execute()


if __name__ == '__main__':
    execute()
    # TODO: post processing here
