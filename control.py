#!/usr/bin/env python3
import json
import struct
from socket import socket, AF_INET, SOCK_STREAM
from multiprocessing.pool import Pool

HOST, PORT = 'localhost', 1337


def recvall(conn, len):
    total_recv = 0
    data = []
    while total_recv < len:
        d = conn.recv(len - total_recv)
        if d == b'':
            raise RuntimeError("socket connection broken")
        data.append(d)
        total_recv += total_recv + len(d)
    return b''.join(data)


def wait_for_confirmation(conn, addr):
    # wait for confirmation by client
    # client sends back a byte with value 0x00000001 when ready
    confirmation_b = recvall(conn, 4)
    (confirmation) = struct.unpack('>I', confirmation_b)
    if confirmation != 0x00000001:
        raise RuntimeError('Error when waiting for confirmation from client {}'
                           .format(addr))


def send_config_and_wait(config_bytes, conn, addr):
    # pack config and send
    length = len(config_bytes)
    buf = struct.pack('>I{len}s'.format(len=length),
                      length, config_bytes)

    # send
    conn.sendall(buf)

    # wait until the client confirms it's ready (after it has downloaded the
    # traces and shit)
    wait_for_confirmation(conn, addr)
    return


def trigger_run(conn, addr):
    # send a 0x00000001 byte to trigger execution of the experiment
    buf = struct.pack('>I', 0x00000001)
    conn.sendall(buf)
    wait_for_confirmation(conn, addr)

    # after confirmation, close the connection
    conn.close()
    return


class Experiment():

    def __init__(self, config):
        with open(config, 'r') as f:
            self.config = json.loads(f.read())

        self.clients = list()

    def start(self):
        with socket(AF_INET, SOCK_STREAM) as server_socket:
            server_socket.bind((HOST, PORT))
            server_socket.listen(backlog=self.config['num_clients'])

            # accept N clients for the experiment
            while len(self.clients) != self.config['num_clients']:
                self.clients.append(server_socket.accept())

            with Pool(self.config['num_clients']) as pool:
                # send configs in parallel to all clients
                config_b = json.dumps(self.config,
                                      separators=(',', ':')).encode('utf-8')
                params = [(config_b, conn, addr)
                          for (conn, addr) in self.clients]

                pool.starmap(send_config_and_wait, params)
                pool.join()

                # all clients are instantiated, now let's run the experiment
                pool.starmap(trigger_run, self.clients)
                pool.join()

                # TODO: Start local monitoring of computational load
                # TODO: listen for incoming connections from finished clients




if __name__ == '__main__':
    e = Experiment('./experiment_config.json')
