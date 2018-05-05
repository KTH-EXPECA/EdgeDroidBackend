#!/usr/bin/env python3
import csv
import json
import os
import shlex
import signal
import subprocess
import time

from client import Client
from socket import socket, AF_INET, SOCK_STREAM
from multiprocessing.pool import Pool
import click
import docker

from monitor import ResourceMonitor

LEGO_DOCKER_IMG = 'jamesjue/gabriel-lego'
SYSTEM_STATS = 'system_stats.csv'
CLIENT_STATS = '{:02}_stats.json'

# defaults
DEFAULT_OUTPUT_DIR = '{}/results/'.format(os.getcwd())
DEFAULT_EXPCONFIG_PATH = '{}/experiment_config.json'.format(os.getcwd())
DEFAULT_CTRLSERVER_ADDR = '0.0.0.0'
DEFAULT_CTRLSERVER_PORT = 1337
DEFAULT_VIDEO_PORT = 9098
DEFAULT_RESULT_PORT = 9111
DEFAULT_CONTROL_PORT = 22222
NET_IFACE = 'enp0s31f6'

TCPDUMP_CMD_PREFIX = ['tcpdump', '-s 0', '-i {}'.format(NET_IFACE)]
TCPDUMP_CMD_SUFFIX = ['-w tcp.pcap']

def wait_for_result(async_result):
    async_result.wait()

def send_config(client):
    client.send_config()


def run_exp(client):
    client.run_experiment()


def close_conn(client):
    client.close()


def get_stats(client, experiment_id):
    return client.get_remote_stats(experiment_id)


class Experiment():

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
        for client in self.clients:
            client.close()

        if self.tcpdump_proc:
            self.tcpdump_proc.send_signal(signal.SIGINT)

        print('Shutting down containers...')
        for cont in self.containers:
            cont.kill()

    def init_docker(self):
        print('Spawning Docker containers...')
        for i, port_config in enumerate(self.config['ports']):
            print('Launching container {} of {}'
                  .format(i + 1, len(self.config['ports'])))

            self.containers.append(
                self.docker.containers.run(
                    LEGO_DOCKER_IMG,
                    detach=True,
                    auto_remove=True,
                    ports={
                        DEFAULT_VIDEO_PORT  : port_config['video'],
                        DEFAULT_RESULT_PORT : port_config['result'],
                        DEFAULT_CONTROL_PORT: port_config['control']
                    }
                )
            )

        print('Wait for container warm up...')
        time.sleep(5)
        print('Initialization done')

    def init_tcpdump(self):
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
        tcpdump = shlex.split(' '.join(TCPDUMP_CMD_PREFIX + port_cmds +
                                       TCPDUMP_CMD_SUFFIX))

        self.tcpdump_proc = subprocess.Popen(tcpdump, cwd=self.output_dir)
        if self.tcpdump_proc.poll():
            exit(-1)

    def _gen_config_for_client(self, client_index):
        c = dict()
        c['experiment_id'] = self.config['experiment_id']
        c['client_id'] = client_index
        c['runs'] = self.config['runs']
        c['steps'] = self.config['steps']
        c['trace_root_url'] = self.config['trace_root_url']
        c['ports'] = self.config['ports'][client_index]
        return c

    def execute(self):
        server_socket = None
        try:
            with socket(AF_INET, SOCK_STREAM) as server_socket:
                server_socket.bind((self.host, self.port))
                server_socket.listen(self.config['clients'])
                print('Listening on {}'.format((self.host, self.port)))

                with Pool(self.config['clients']) as pool:
                    # accept N clients for the experiment
                    print('Waiting for {} clients to connect...'
                          .format(self.config['clients']))

                    config_send = []
                    for i in range(self.config['clients']):
                        conn, addr = server_socket.accept()
                        client = Client(conn, addr,
                                        config=self._gen_config_for_client(i))
                        self.clients.append(client)
                        print('Client {} connected.'.format(addr))
                        # send configs in parallel to all clients
                        config_send.append(
                            pool.apply_async(send_config, args=(client,))
                        )

                    # send configs in parallel to all clients
                    # print('Sending configs...')
                    # pool.map(send_config, self.clients)

                    pool.map(wait_for_result, config_send)

                    print('Starting resource monitor...')
                    monitor = ResourceMonitor()
                    monitor.start()

                    # all clients are instantiated, now let's run the experiment
                    print('Execute experiment!')
                    pool.map(run_exp, self.clients)
                    pool.map(close_conn, self.clients)

                    self.clients.clear()
                    print('Waiting for {} clients to reconnect...'
                          .format(self.config['clients']))
                    for i in range(self.config['clients']):
                        conn, addr = server_socket.accept()
                        self.clients.append(Client(conn, addr))
                        print('Client {} reconnected.'.format(addr))

                    # all clients reconnected
                    print('Shut down monitor.')
                    system_stats = monitor.shutdown()
                    # pool.map(lambda c: c.get_remote_config(
                    #    self.config['experiment_id']), self.clients)

                    print('Get stats from clients!')
                    exp_id_list = [self.config[
                                       'experiment_id']] * len(self.clients)
                    stats = pool.starmap(get_stats, zip(self.clients,
                                                        exp_id_list))

                with open(self.output_dir + SYSTEM_STATS, 'w') as f:
                    fieldnames = ['cpu_load', 'mem_avail', 'timestamp']
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(system_stats)

                for stat_coll in stats:
                    client_index = stat_coll['client_id']
                    with open(self.output_dir +
                              CLIENT_STATS.format(client_index), 'w') as f:
                        json.dump(stat_coll, f)
        finally:
            if server_socket:
                server_socket.close()
            self.shutdown()


@click.command()
@click.argument('experiment_config', default=DEFAULT_EXPCONFIG_PATH,
                type=click.Path(exists=True, dir_okay=False))
@click.option('--host', type=str, default=DEFAULT_CTRLSERVER_ADDR,
              help='Addresss to which bind this server instance.',
              show_default=True)
@click.option('--port', type=int, default=DEFAULT_CTRLSERVER_PORT,
              help='Port on which to listen for incoming connection.',
              show_default=True)
@click.option('--output_dir', default=DEFAULT_OUTPUT_DIR, show_default=True,
              type=click.Path(dir_okay=True, file_okay=False, exists=True),
              help='Output directory for result files.')
def execute(experiment_config, host, port, output_dir):
    e = Experiment(experiment_config, host, port, output_dir)
    e.init_docker()
    e.init_tcpdump()
    e.execute()


if __name__ == '__main__':
    execute()
