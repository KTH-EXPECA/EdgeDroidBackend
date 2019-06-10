#!/usr/bin/env python3
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

import os
import csv
import hashlib
import itertools
import json
import shlex
import signal
import subprocess
import time
from multiprocessing import Barrier
from random import shuffle
from socket import *

import click
import ntplib
import numpy as np
import toml

import constants
from config import ExperimentConfig, RecursiveNestedDict
from cpu_load_manager import CPULoadManager, NullCPULoadManager
from custom_logging.logging import LOGGER
from backend_manager import BackendManager
from monitor import ResourceMonitor
from client import AsyncClient, NullClient

CPU_CFS_PERIOD = 100000  # 100 000 microseconds, 100 ms


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


class ShutdownException(Exception):
    pass


def signal_handler(*args):
    raise ShutdownException()


def set_keepalive_linux(sock, after_idle_sec=1, interval_sec=3, max_fails=5):
    '''Set TCP keepalive on an open socket.

    It activates after 1 second (after_idle_sec) of idleness,
    then sends a keepalive ping once every 3 seconds (interval_sec),
    and closes the connection after 5 failed ping (max_fails), or 15 seconds
    '''
    sock.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
    sock.setsockopt(IPPROTO_TCP, TCP_KEEPIDLE, after_idle_sec)
    sock.setsockopt(IPPROTO_TCP, TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(IPPROTO_TCP, TCP_KEEPCNT, max_fails)


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

        self.backend_mgr = BackendManager(self.config)

        if self.config.gen_load:
            self.load_mgr = CPULoadManager(self.config)
        else:
            self.load_mgr = NullCPULoadManager()

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
            if self.backend_mgr:
                self.backend_mgr.shutdown()
        except Exception as e:
            LOGGER.error(
                'Something went wrong while shutting down Docker containers')
            LOGGER.error(e)

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

    # noinspection PyProtectedMember
    def _gen_config_for_client(self, client_index):
        c = dict()
        c['experiment_id'] = self.config.name
        c['client_id'] = client_index
        c['steps'] = len(self.config.trace_steps)
        c['ports'] = self.config.port_configs[client_index]._asdict()
        c['ntp_server'] = self.config.ntp_servers[0]
        # TODO change in the future (to work with more than one server)

        c['fps'] = self.config.trace_fps
        c['rewind_seconds'] = self.config.rewind_seconds
        c['max_replays'] = self.config.max_replays
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

    def __execute_run(self, run_index):
        LOGGER.info('Executing run %d out of %d',
                    run_index + 1, self.config.runs)

        run_path = self.output_dir + f'/run_{run_index + 1}/'
        if os.path.exists(run_path) and not os.path.isdir(run_path):
            raise RuntimeError(f'Output path {run_path} already '
                               'exists and is not a directory!')
        elif os.path.isdir(run_path):
            pass
        else:
            os.mkdir(run_path)

        # sync NTP
        # self._pollNTPServer()

        # todo: estimate minimum local delay

        LOGGER.info('Trigger clock sync')
        for client in self.clients:
            client.clock_sync()

        # self._init_tcpdump(run_path)
        # LOGGER.info('TCPdump warmup...')
        # time.sleep(5)

        # make sure all clients are done syncing NTP before starting the
        # experiment
        for client in self.clients:
            client.wait_for_tasks()

        # All clients are ready, now let's run the experiment!
        shuffle(self.clients)  # shuffle clients before each run

        # Randomly offset client experiment start to avoid weird
        # synchronous effects on the processing times...
        start_times = np.random.uniform(
            0,
            constants.DEFAULT_START_WINDOW,
            len(self.clients))

        # start artificial CPU load
        self.load_mgr.start_cpu_load()

        LOGGER.info('Execute experiment!')
        LOGGER.info('Starting resource monitor...')
        monitor = ResourceMonitor(self.offset)
        monitor.start()

        start_timestamp = time.time() * 1000.0

        # finally, actually trigger the experiment on the clients...
        # asynchronously, of course
        for client, init_offset in zip(self.clients, start_times):
            client.execute_experiment(init_offset)

        # wait for the experiment to finish
        for client in self.clients:
            client.wait_for_tasks()

        end_timestamp = time.time() * 1000.0

        LOGGER.info('Shut down monitor.')
        system_stats = monitor.shutdown()

        # shut down CPU load generator
        self.load_mgr.shutdown()

        time.sleep(1)
        # LOGGER.info('Terminate TCPDUMP')
        # self.tcpdump_proc.send_signal(signal.SIGINT)
        # self.tcpdump_proc.wait()

        LOGGER.info('Get stats from clients!')
        for client in self.clients:
            client.fetch_stats()  # asynchronously triggers fetching stats

        # store this runs' system stats
        with open(run_path + constants.SYSTEM_STATS, 'w') as f:
            fieldnames = ['cpu_load', 'mem_avail', 'timestamp']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(system_stats)

        # store the client stats
        stats = [c.get_stats() for c in self.clients]
        for stat_coll in stats:
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

    def execute(self):
        server_socket = None
        error = None

        try:
            # first start docker instances
            self.backend_mgr.spawn_backends()

            with socket(AF_INET, SOCK_STREAM) as server_socket:
                server_socket.bind((self.host, self.port))
                server_socket.listen(self.config.clients)
                server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                LOGGER.info('Listening on %s:%d', self.host, self.port)

                # accept N clients for the experiment
                LOGGER.info('Waiting for %d clients to connect...',
                            self.config.clients)

                signal.signal(signal.SIGINT, signal_handler)
                signal.signal(signal.SIGTERM, signal_handler)

                # wait for all clients to connect
                for i in range(self.config.clients):
                    conn, addr = server_socket.accept()
                    set_keepalive_linux(conn, max_fails=100)  # 5 minutes

                    client = AsyncClient(conn, addr,
                                         self._gen_config_for_client(i))

                    self.clients.append(client)

                    LOGGER.info(
                        '{} out of {} clients connected: {}:{}'.format(
                            i + 1, self.config.clients, *addr))
                    # send config
                    client.send_config()

                # wait for clients to finish sending configs
                for c in self.clients:
                    c.wait_for_tasks()

                # send traces, two clients at the time
                LOGGER.info('Pushing step files to clients...')

                # grouper is a generator, so wrap it in a list
                chunks = list(grouper(self.clients, 2, fillvalue=NullClient()))

                # use list comprehensions to avoid laziness (map())
                step_data = [open(p, 'rb').read() for p in
                             self.config.trace_steps]
                step_chksums = [hashlib.md5(d).hexdigest() for d in step_data]

                steps = zip(
                    itertools.count(start=1),
                    step_chksums,
                    step_data
                )

                for chunk, step in itertools.product(chunks, steps):
                    for c in chunk:
                        c.send_step(*step)
                    for c in chunk:
                        c.wait_for_tasks()

                # execute runs!
                for r in range(self.config.runs):
                    self.__execute_run(r)

        except ShutdownException:
            pass

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
