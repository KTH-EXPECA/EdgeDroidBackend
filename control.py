#!/usr/bin/env python3
import csv
import json
import shlex
import signal
import subprocess
import time
from multiprocessing.pool import Pool
from socket import *

import click
import docker

import constants
import os
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


def run_exp(client):
    client.run_experiment()


def close_conn(client):
    client.close()


def get_stats(client, experiment_id):
    return client.get_remote_stats(experiment_id)


class Experiment:

    def __init__(self, config, host, port, output_dir):
        with open(config, 'r') as f:
            print('Loading config...')
            self.config = json.loads(f.read())

        self.clients = list()
        self.host = host
        self.port = port
        self.docker = docker.from_env()
        self.containers = list()
        self.tcpdump_proc = None
        self.output_dir = output_dir

        print('Config:')
        print('Experiment ID:', self.config['experiment_id'])
        print('Client:', self.config['clients'])
        print('Runs:', self.config['runs'])

    def shutdown(self):
        print('Shut down!')

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
            for cont in self.containers:
                cont.kill()
        except Exception as e:
            print('Something went wrong while shutting down containers')
            print(e)

    def _init_docker(self):
        print('Spawning Docker containers...')
        for i, port_config in enumerate(self.config['ports']):
            print('Launching container {} of {}'
                  .format(i + 1, len(self.config['ports'])))

            self.containers.append(
                self.docker.containers.run(
                    constants.LEGO_DOCKER_IMG,
                    detach=True,
                    auto_remove=True,
                    ports={
                        constants.DEFAULT_VIDEO_PORT  : port_config['video'],
                        constants.DEFAULT_RESULT_PORT : port_config['result'],
                        constants.DEFAULT_CONTROL_PORT: port_config['control']
                    }
                )
            )
            time.sleep(1)

        print('Wait for container warm up...')
        time.sleep(5)
        print('Initialization done')

    def _init_tcpdump(self, run_path):
        print('Initializing TCP dump...')
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
        return c

    def execute(self):
        self._init_docker()
        server_socket = None
        try:
            with socket(AF_INET, SOCK_STREAM) as server_socket:
                server_socket.bind((self.host, self.port))
                server_socket.listen(self.config['clients'])
                server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
                print('Listening on {}'.format((self.host, self.port)))

                # accept N clients for the experiment
                print('Waiting for {} clients to connect...'
                      .format(self.config['clients']))

                with Pool(2) as pool:
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

                        config_send.append(
                            pool.apply_async(send_config, args=(client,))
                        )

                    for result in config_send:
                        result.wait()

                    # have the clients fetch traces 2 at the time to avoid
                    # congestion in the network
                    pool.map(fetch_traces, self.clients)

                with Pool(self.config['clients']) as pool:

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

                        self._init_tcpdump(run_path)

                        # All clients are ready, now let's run the experiment!
                        # Stagger client experiment start to avoid weird
                        # synchronous
                        # effects on the processing times...
                        print('Execute experiment!')
                        for client in self.clients:
                            time.sleep(constants.DEFAULT_STAGGER_INTERVAL)
                            client.run_experiment()

                        self.clients.clear()
                        print('Waiting for {} clients to reconnect...'
                              .format(self.config['clients']))

                        for i in range(self.config['clients']):
                            conn, addr = server_socket.accept()
                            set_keepalive_linux(conn,
                                                max_fails=100)  # 5 minutes
                            self.clients.append(Client(conn, addr))
                            print(
                                '{} out of {} clients '
                                'reconnected: {}:{}'.format(
                                    i + 1, self.config['clients'], *addr
                                )
                            )

                        # all clients reconnected
                        print('Terminate TCPDUMP')
                        self.tcpdump_proc.send_signal(signal.SIGINT)

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
                            client_index = stat_coll['client_id']
                            with open(run_path +
                                      constants.CLIENT_STATS.format(
                                          client_index
                                      ), 'w') as f:
                                json.dump(stat_coll, f)

        finally:
            try:
                if server_socket:
                    # server_socket.shutdown(SHUT_RDWR)
                    server_socket.close()
            except Exception as e:
                print('Error closing server socket.')
                print(e)

            self.shutdown()


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
