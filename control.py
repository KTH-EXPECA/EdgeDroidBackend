#!/usr/bin/env python3
import csv
import json
import shlex
import signal
import subprocess
import time
from datetime import datetime
from multiprocessing import Barrier, Process
from multiprocessing.pool import Pool
from random import shuffle
from socket import *

import os
import ntplib
import click
import docker

import constants
from client import Client
from monitor import ResourceMonitor


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


def send_config(client):
    client.send_config()


def fetch_traces(client):
    client.fetch_traces()


def run_exp(client_idx, client):
    time.sleep(client_idx * constants.DEFAULT_STAGGER_INTERVAL)
    client.run_experiment()
    client.wait_for_experiment_finish()


def close_conn(client):
    client.close()


def ntp_sync(client):
    client.ntp_sync()


def get_stats(client, experiment_id):
    return client.get_remote_stats(experiment_id)


class Experiment:

    def __init__(self, config, host, port, output_dir):
        with open(config, 'r') as f:
            print('Loading config...')
            self.config = json.loads(f.read())
            self._validate_config()

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

        print('Config:')
        print('Experiment ID:', self.config['experiment_id'])
        print('Client:', self.config['clients'])
        print('Runs:', self.config['runs'])

    def _validate_config(self):
        try:
            v = self.config['experiment_id']
            v = self.config['clients']
            v = self.config['runs']
            v = self.config['steps']
            v = self.config['trace_root_url']
            v = self.config['ntp_server']
            p = self.config['ports']
            assert type(p) == list
            assert len(p) == self.config['clients']

            for p_config in p:
                v = p_config['video']
                v = p_config['result']
                v = p_config['control']

        except Exception as e:
            print("Invalid configuration file.")
            self.shutdown(e)

    def shutdown(self, e=None):
        print('Shut down!')
        if e:
            print(e)

        try:
            for client in self.clients:
                client.shutdown()
        except Exception as e:
            print('Something went wrong while shutting down clients')
            print(e)

        try:
            if self.tcpdump_proc:
                self.tcpdump_proc.send_signal(signal.SIGINT)
        except Exception as e:
            print('Something went wrong while shutting down TCPDUMP')
            print(e)

        try:
            self.docker_barrier.wait()
            if self.docker_proc:
                self.docker_proc.join()
        except Exception as e:
            print('Something went wrong while shutting down Docker containers')
            print(e)

    @staticmethod
    def _init_docker(config, barrier):
        print('Spawning Docker containers...')
        dck = docker.from_env()
        containers = list()
        try:
            for i, port_config in enumerate(config['ports']):
                print('Launching container {} of {}'
                      .format(i + 1, len(config['ports'])))

                containers.append(
                    dck.containers.run(
                        constants.LEGO_DOCKER_IMG,
                        detach=True,
                        auto_remove=True,
                        ports={
                            constants.DEFAULT_VIDEO_PORT  : port_config[
                                'video'],
                            constants.DEFAULT_RESULT_PORT : port_config[
                                'result'],
                            constants.DEFAULT_CONTROL_PORT: port_config[
                                'control']
                        }
                    )
                )

            print('Wait for container warm up...')
            time.sleep(5)
            print('Initialization done')

            barrier.wait()

        except InterruptedError:
            pass
        except Exception as e:
            print("Error while spawning Docker containers!")
            raise e
        finally:
            print('Shutting down containers...')
            for cont in containers:
                cont.kill()

    def _init_tcpdump(self, run_path):
        print('Initializing TCP dump...')
        print('TCPdump directory: {}'.format(run_path))
        port_cmds = list()
        for port_config in self.config['ports']:
            cmds = [
                'port {}'.format(port_config['video']),
                'port {}'.format(port_config['result']),
                'port {}'.format(port_config['control']),
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
        c['experiment_id'] = self.config['experiment_id']
        c['client_id'] = client_index
        # c['runs'] = self.config['runs']
        c['steps'] = self.config['steps']
        c['trace_root_url'] = self.config['trace_root_url']
        c['ports'] = self.config['ports'][client_index]
        c['ntp_server'] = self.config['ntp_server']
        return c

    def _pollNTPServer(self):
        print('Getting NTP offset')
        sync_cnt = 0
        cum_offset = 0
        while sync_cnt < constants.DEFAULT_NTP_POLL_COUNT:
            try:
                res = self.ntp_client.request(
                    self.config['ntp_server'],
                    version=4
                )

                cum_offset += res.offset
                sync_cnt += 1
            except ntplib.NTPException:
                continue

        self.offset = (cum_offset * 1000.0) / sync_cnt
        # convert to milliseconds

        print('Got NTP offset from ', self.config['ntp_server'])
        print('Offset: {} ms'.format(self.offset))

    def execute(self):
        server_socket = None
        error = None
        try:
            with socket(AF_INET, SOCK_STREAM) as server_socket:
                server_socket.bind((self.host, self.port))
                server_socket.listen(self.config['clients'])
                server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                print('Listening on {}:{}'.format(self.host, self.port))

                # accept N clients for the experiment
                print('Waiting for {} clients to connect...'
                      .format(self.config['clients']))

                with Pool(2) as pool:
                    self.docker_proc.start()

                    config_send = []
                    for i in range(self.config['clients']):
                        conn, addr = server_socket.accept()
                        set_keepalive_linux(conn, max_fails=100)  # 5 minutes
                        client = Client(conn, addr,
                                        config=self._gen_config_for_client(i))
                        self.clients.append(client)

                        print('{} out of {} clients connected: {}:{}'.format(
                            i + 1, self.config['clients'], *addr
                        ))

                        print("Sending config...")
                        config_send.append(
                            pool.apply_async(send_config, args=(client,))
                        )

                    for result in config_send:
                        result.wait()

                    # have the clients fetch traces 2 at the time to avoid
                    # congestion in the network
                    print("Triggering trace download...")
                    pool.map(fetch_traces, self.clients)

                    for r in range(self.config['runs']):
                        print('Executing run {} out of {}'.format(
                            r + 1, self.config['runs']
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

                        print('Starting resource monitor...')
                        monitor = ResourceMonitor()
                        monitor.start()

                        self._pollNTPServer()

                        print('Trigger client NTP sync')
                        pool.map(ntp_sync, self.clients)

                        self._init_tcpdump(run_path)
                        print('TCPdump warmup...')
                        time.sleep(5)

                        # All clients are ready, now let's run the experiment!
                        # Stagger client experiment start to avoid weird
                        # synchronous
                        # effects on the processing times...
                        print('Execute experiment!')

                        # shuffle clients before each run
                        shuffle(self.clients)

                        # for client in self.clients:
                        #    time.sleep(constants.DEFAULT_STAGGER_INTERVAL)
                        #    client.run_experiment()

                        # self.clients.clear()

                        start_timestamp = datetime.utcnow().timestamp() * 1000.0
                        with Pool(len(self.clients)) as exec_pool:
                            exec_pool.starmap(run_exp, enumerate(self.clients))
                        end_timestamp = datetime.utcnow().timestamp() * 1000.0

                        # print('Waiting for {} clients to reconnect...'
                        #       .format(self.config['clients']))

                        # for i in range(self.config['clients']):
                        #     conn, addr = server_socket.accept()
                        #     set_keepalive_linux(conn,
                        #                         max_fails=100)  # 5 minutes
                        #     self.clients.append(Client(conn, addr))
                        #     print(
                        #         '{} out of {} clients '
                        #         'reconnected: {}:{}'.format(
                        #             i + 1, self.config['clients'], *addr
                        #         )
                        #     )

                        # all clients reconnected
                        # wait a second before terminating TCPdump
                        time.sleep(1)
                        print('Terminate TCPDUMP')
                        self.tcpdump_proc.send_signal(signal.SIGINT)
                        self.tcpdump_proc.wait()

                        print('Shut down monitor.')
                        system_stats = monitor.shutdown()

                        print('Get stats from clients!')
                        exp_id_list = [self.config[
                                           'experiment_id']] * len(self.clients)
                        stats = pool.starmap(get_stats,
                                             zip(self.clients, exp_id_list))

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
                print('Error closing server socket.')
                print(e)

            self.shutdown(e=error)


@click.command()
@click.argument('experiment_config', default=constants.DEFAULT_EXPCONFIG_PATH,
                type=click.Path(exists=True, dir_okay=False))
@click.option('--host', type=str, default=constants.DEFAULT_CONTROLSERVER_HOST,
              help='Addresss to which bind this server instance.',
              show_default=True)
@click.option('--port', type=int, default=constants.DEFAULT_CONTROLSERVER_PORT,
              help='Port on which to listen for incoming connection.',
              show_default=True)
@click.option('--output_dir', default=constants.DEFAULT_OUTPUT_DIR,
              show_default=True,
              type=click.Path(dir_okay=True, file_okay=False, exists=True),
              help='Output directory for result files.')
def execute(experiment_config, host, port, output_dir):
    e = Experiment(experiment_config, host, port, output_dir)
    e.execute()


if __name__ == '__main__':
    execute()
    # TODO: post processing here
