import socket
import struct
import json

import constants

MAX_CHUNK_SIZE = 2048


def recvall(conn, length):
    total_recv = 0
    data = []
    while total_recv < length:
        d = conn.recv(min(length - total_recv, MAX_CHUNK_SIZE))
        if d == b'':
            raise RuntimeError("socket connection broken")
        data.append(d)
        total_recv += len(d)
    return b''.join(data)


def recvJSON(conn):
    length_b = recvall(conn, 4)
    (length,) = struct.unpack('>I', length_b)

    json_b = recvall(conn, length)
    try:
        assert len(json_b) == length
    except AssertionError:
        print('len(json_b)', len(json_b))
        print('length', length)
        raise

    # total_parsed = 0
    # json_s = ''
    # while total_parsed < length:
    #     parsed_s = struct.unpack('>{l}s'.format(l=min(length,
    #                                                   MAX_CHUNK_SIZE)),
    # json_b)

    (json_s,) = struct.unpack('>{l}s'.format(l=len(json_b)), json_b)
    return json.loads(json_s.decode('utf-8'))


class Client():
    """
    Local representation of a remote client

    Client message format:

    0               31
    -----------------
    | CMD_ID: INT32 |
    -----------------
    | LEN: INT32    |
    -----------------
    |               |
    | PAYLOAD: LEN  |
    |               |
    |      ...      |
    -----------------

    """

    def __init__(self, conn, addr, config=None):
        self.config = config
        self.conn = conn
        self.addr = addr
        self.stats = None

    def close(self):
        self.conn.shutdown(socket.SHUT_RDWR)
        self.conn.close()

    def shutdown(self):
        buf = struct.pack('>I', constants.CMD_SHUTDOWN)
        self.conn.sendall(buf)

        self.close()

    def _check_config(self):
        if not self.config:
            raise RuntimeError('No config set for client {}!'.format(self.addr))

    def get_remote_stats(self, experiment_id):
        buf = struct.pack('>I', constants.CMD_PULL_STATS)
        self.conn.sendall(buf)

        self.stats = recvJSON(self.conn)

        # check that experiment id matches
        if self.stats['experiment_id'] != experiment_id:
            buf = struct.pack('>I', constants.STATUS_ERROR)
            self.conn.sendall(buf)
            raise RuntimeError('Experiment ID\'s do not match!')

        self.config = dict()
        self.config['client_id'] = self.stats['client_id']
        self.config['experiment_id'] = self.stats['experiment_id']

        return self.stats

    def _wait_for_confirmation(self):
        # wait for confirmation by client
        # client sends back a byte with value 0x00000001 when ready
        confirmation_b = recvall(self.conn, 4)
        (confirmation,) = struct.unpack('>I', confirmation_b)
        if confirmation != constants.STATUS_SUCCESS:
            raise RuntimeError(
                'Client {}: error on confirmation!'.format(self.addr))

    def send_config(self):
        self._check_config()

        config_b = json.dumps(self.config,
                              separators=(',', ':')).encode('utf-8')
        length = len(config_b)
        buf = struct.pack('>II{l}s'.format(l=length),
                          constants.CMD_PUSH_CONFIG,
                          length, config_b)
        self.conn.sendall(buf)
        self._wait_for_confirmation()

    def fetch_traces(self):
        self._check_config()
        buf = struct.pack('>I', constants.CMD_FETCH_TRACES)
        self.conn.sendall(buf)
        self._wait_for_confirmation()

    def run_experiment(self):
        buf = struct.pack('>I', constants.CMD_START_EXP)
        self.conn.sendall(buf)
        self._wait_for_confirmation()
        self.close()
