#!/usr/bin/env python3
import csv
import json
from client import Client
from socket import socket, AF_INET, SOCK_STREAM
from multiprocessing.pool import Pool

from monitor import ResourceMonitor

HOST, PORT = 'localhost', 1337


class Experiment():

    def __init__(self, config):
        with open(config, 'r') as f:
            self.config = json.loads(f.read())

        self.clients = list()

    def _gen_config_for_client(self, client_index):
        c = dict()
        c['experiment_id'] = self.config['experiment_id']
        c['client_idx'] = client_index
        c['runs'] = self.config['runs']
        c['steps'] = self.config['steps']
        c['trace_root_url'] = self.config['trace_root_url']
        c['ports'] = self.config['ports'][client_index]
        return c

    def execute(self):
        try:
            with socket(AF_INET, SOCK_STREAM) as server_socket:
                server_socket.bind((HOST, PORT))
                server_socket.listen(backlog=self.config['num_clients'])

                # accept N clients for the experiment
                for i in range(self.config['num_clients']):
                    conn, addr = server_socket.accept()
                    self.clients.append(
                        Client(conn, addr,
                               config=self._gen_config_for_client(i))
                    )

                with Pool(self.config['num_clients']) as pool:
                    # send configs in parallel to all clients
                    pool.map(lambda c: c.send_config(), self.clients)

                    monitor = ResourceMonitor()
                    monitor.start()

                    # all clients are instantiated, now let's run the experiment
                    pool.map(lambda c: c.run_experiment(), self.clients)
                    pool.map(lambda c: c.close(), self.clients)

                    self.clients.clear()
                    for i in range(self.config['num_clients']):
                        self.clients.append(Client(*server_socket.accept()))

                    # all clients reconnected
                    system_stats = monitor.shutdown()
                    pool.map(lambda c: c.get_remote_config(
                        self.config['experiment_id']), self.clients)

                    stats = pool.map(lambda c: c.get_remote_stats(),
                                     self.clients)

                with open('system_stats.csv', 'w') as f:
                    fieldnames = ['cpu_load', 'mem_avail', 'timestamp']
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(system_stats)

                for stat_coll in stats:
                    client_index = stat_coll['client_idx']
                    with open('{:03}_stats.json'.format(client_index),
                              'w') as f:
                        json.dump(stat_coll, f)
        finally:
            for client in self.clients:
                client.close()


if __name__ == '__main__':
    e = Experiment('./experiment_config.json')
