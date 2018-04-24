#!/usr/bin/env python3
import json
from client import Client
from socket import socket, AF_INET, SOCK_STREAM
from multiprocessing.pool import Pool

HOST, PORT = 'localhost', 1337

class Experiment():

    def __init__(self, config):
        with open(config, 'r') as f:
            self.config = json.loads(f.read())

        self.clients = list()

    def gen_config_for_client(self, client_index):
        c = dict()
        c['name'] = self.config['name']
        c['client_idx'] = client_index
        c['runs'] = self.config['runs']
        c['steps'] = self.config['steps']
        c['trace_root_url'] = self.config['trace_root_url']
        c['ports'] = self.config['ports'][client_index]
        return c

    def start(self):
        with socket(AF_INET, SOCK_STREAM) as server_socket:
            server_socket.bind((HOST, PORT))
            server_socket.listen(backlog=self.config['num_clients'])

            # accept N clients for the experiment
            for i in range(self.config['num_clients']):
                conn, addr = server_socket.accept()
                self.clients.append(Client(self.gen_config_for_client(i),
                                           conn, addr))

            with Pool(self.config['num_clients']) as pool:
                # send configs in parallel to all clients
                pool.map(lambda c: c.send_config(), self.clients)
                pool.join()

                # TODO: Start local monitoring of computational load

                # all clients are instantiated, now let's run the experiment
                pool.map(lambda c: c.run_experiment(), self.clients)
                pool.join()

                # TODO: listen for incoming connections from finished clients


if __name__ == '__main__':
    e = Experiment('./experiment_config.json')
