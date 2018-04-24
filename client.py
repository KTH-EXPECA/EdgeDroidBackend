import struct
import json


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


class Client():
    """
    Local representation of a remote client
    """

    success_msg = 0x00000001

    def __init__(self, config, conn, addr):
        self.config = config
        self.conn = conn
        self.addr = addr

    def wait_for_confirmation(self):
        # wait for confirmation by client
        # client sends back a byte with value 0x00000001 when ready
        confirmation_b = recvall(self.conn, 4)
        (confirmation) = struct.unpack('>I', confirmation_b)
        if confirmation != self.success_msg:
            raise RuntimeError(
                'Error when waiting for confirmation from client {}'
                    .format(self.addr))

    def send_config(self):
        config_b = json.dumps(self.config,
                              separators=(',', ':')).encode('utf-8')
        length = len(config_b)
        buf = struct.pack('>I{l}s'.format(l=length), length, config_b)
        self.conn.sendall(buf)
        self.wait_for_confirmation()

    def run_experiment(self):
        buf = struct.pack('>I', self.success_msg)
        self.conn.sendall(buf)
        self.wait_for_confirmation()
