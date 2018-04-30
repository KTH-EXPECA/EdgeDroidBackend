#!/usr/bin/env python3
import csv
import json
import os

from client import Client
from socket import socket, AF_INET, SOCK_STREAM
from multiprocessing.pool import Pool
import click

from monitor import ResourceMonitor


def send_config(client):
    client.send_config()


def run_exp(client):
    client.run_experiment()


def close_conn(client):
    client.close()


def get_stats(client, experiment_id):
    return client.get_remote_stats(experiment_id)


class Experiment():

    def __init__(self, config, host, port):
        with open(config, 'r') as f:
            print('Loading config...')
            self.config = json.loads(f.read())

        self.clients = list()
        self.host = host
        self.port = port

        print('Config:')
        print('Experiment ID:', self.config['experiment_id'])
        print('Client:', self.config['clients'])
        print('Runs:', self.config['runs'])

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

                # accept N clients for the experiment
                print('Waiting for {} clients to connect...'
                      .format(self.config['clients']))
                for i in range(self.config['clients']):
                    conn, addr = server_socket.accept()
                    self.clients.append(
                        Client(conn, addr,
                               config=self._gen_config_for_client(i))
                    )
                    print('Client {} connected.'.format(addr))

                with Pool(self.config['clients']) as pool:
                    # send configs in parallel to all clients
                    print('Sending configs...')
                    pool.map(send_config, self.clients)

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

                with open('system_stats.csv', 'w') as f:
                    fieldnames = ['cpu_load', 'mem_avail', 'timestamp']
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(system_stats)

                for stat_coll in stats:
                    client_index = stat_coll['client_id']
                    with open('{:03}_stats.json'.format(client_index),
                              'w') as f:
                        json.dump(stat_coll, f)
        finally:
            print('Shut down!')
            if server_socket:
                server_socket.close()
            for client in self.clients:
                client.close()

@click.command()
@click.argument('experiment_config', type=click.Path(exists=True,
                                                     dir_okay=False),
                default=os.getcwd() + '/experiment_config.json')
@click.option('--host', type=str, default='0.0.0.0',
              help='Addresss to which bind this server instance.',
              show_default=True)
@click.option('--port', type=int, default=1337,
              help='Port on which to listen for incoming connection.',
              show_default=True)
def execute(experiment_config, host, port):
    e = Experiment(experiment_config, host, port)
    e.execute()

if __name__ == '__main__':
    execute()
