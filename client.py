import struct
import json

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
    #                                                   MAX_CHUNK_SIZE)), json_b)

    (json_s,) = struct.unpack('>{l}s'.format(l=len(json_b)), json_b)
    return json.loads(json_s.decode('utf-8'))


class Client():
    """
    Local representation of a remote client
    """

    success_msg = 0x00000001
    getstats_msg = 0x00000003
    error_msg = 0xffffffff

    def __init__(self, conn, addr, config=None):
        self.config = config
        self.conn = conn
        self.addr = addr
        self.stats = None

    def close(self):
        self.conn.close()

    def get_remote_stats(self, experiment_id):
        buf = struct.pack('>I', self.getstats_msg)
        self.conn.sendall(buf)

        self.stats = recvJSON(self.conn)

        # check that experiment id matches
        if self.stats['experiment_id'] != experiment_id:
                buf = struct.pack('>I', self.error_msg)
                self.conn.sendall(buf)
                raise RuntimeError('Experiment ID\'s do not match!')

        self.config = dict()
        self.config['client_id'] = self.stats['client_id']
        self.config['experiment_id'] = self.stats['experiment_id']

        return self.stats

    #def get_remote_config(self, experiment_id):
    #    buf = struct.pack('>I', self.getconfig_msg)
    #    self.conn.sendall(buf)

    #    self.config = recvJSON(self.conn)

    #    if self.config['experiment_id'] != experiment_id:
    #        buf = struct.pack('>I', self.error_msg)
    #        self.conn.sendall(buf)
    #        raise RuntimeError('Experiment ID\'s do not match!')

    def _wait_for_confirmation(self):
        # wait for confirmation by client
        # client sends back a byte with value 0x00000001 when ready
        confirmation_b = recvall(self.conn, 4)
        (confirmation,) = struct.unpack('>I', confirmation_b)
        if confirmation != self.success_msg:
            raise RuntimeError(
                'Client {}: error on confirmation!'.format(self.addr))

    def send_config(self):
        if not self.config:
            raise RuntimeError('No config set for client {}!'.format(self.addr))

        config_b = json.dumps(self.config,
                              separators=(',', ':')).encode('utf-8')
        length = len(config_b)
        buf = struct.pack('>I{l}s'.format(l=length), length, config_b)
        self.conn.sendall(buf)
        self._wait_for_confirmation()

    def run_experiment(self):
        buf = struct.pack('>I', self.success_msg)
        self.conn.sendall(buf)
