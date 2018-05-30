import json
import socket
import struct

import constants
from custom_logging.logging import LOGGER

MAX_CHUNK_SIZE = 2048


def recvall(conn, length):
    total_recv = 0
    data = []
    while total_recv < length:
        d = conn.recv(min(length - total_recv, MAX_CHUNK_SIZE))
        if d == b'':
            raise RuntimeError('socket connection broken')
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


def sendJSON(conn, dict_data):
    json_data = json.dumps(dict_data,
                           separators=(',', ':')).encode('utf-8')
    length = len(json_data)
    buf = struct.pack('>I{l}s'.format(l=length), length, json_data)
    conn.sendall(buf)


class Client:
    '''
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

    '''

    def __init__(self, conn, addr, config):
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

    def get_remote_stats(self):
        buf = struct.pack('>I', constants.CMD_PULL_STATS)
        self.conn.sendall(buf)

        self.stats = recvJSON(self.conn)

        return self.stats

    def _wait_for_confirmation(self):
        # wait for confirmation by client
        # client sends back a byte with value 0x00000001 when ready
        confirmation_b = recvall(self.conn, 4)
        (confirmation,) = struct.unpack('>I', confirmation_b)
        if confirmation != constants.STATUS_SUCCESS:
            raise Exception(
                'Client {}: error on confirmation!'.format(self.addr))

    def send_config(self):
        self._check_config()
        buf = struct.pack('>I', constants.CMD_PUSH_CONFIG)
        self.conn.sendall(buf)
        sendJSON(self.conn, self.config)
        self._wait_for_confirmation()

    # def fetch_traces(self):
    #     self._check_config()
    #     LOGGER.info('Fetching traces: client %d...', self.config['client_id'])
    #     buf = struct.pack('>I', constants.CMD_FETCH_TRACES)
    #     self.conn.sendall(buf)
    #     self._wait_for_confirmation()
    #     LOGGER.info('Fetching traces: client %d done!',
    #                 self.config['client_id'])

    def run_experiment(self):
        LOGGER.info('Client %d starting experiment!',
                    self.config['client_id'])

        buf = struct.pack('>I', constants.CMD_START_EXP)
        self.conn.sendall(buf)
        self._wait_for_confirmation()

    def wait_for_experiment_finish(self):
        confirmation_b = recvall(self.conn, 4)
        (confirmation,) = struct.unpack('>I', confirmation_b)
        if confirmation != constants.MSG_EXPERIMENT_FINISH:
            raise RuntimeError(
                'Client {}: error on experiment finish!'.format(self.addr))

        LOGGER.info('Client %d done!', self.config['client_id'])

    def ntp_sync(self):
        buf = struct.pack('>I', constants.CMD_SYNC_NTP)
        self.conn.sendall(buf)
        self._wait_for_confirmation()

    def send_step(self, index, checksum, data):
        # first build and send header
        hdr = {
            constants.STEP_METADATA_INDEX : index,
            constants.STEP_METADATA_SIZE  : len(data),
            constants.STEP_METADATA_CHKSUM: checksum
        }

        LOGGER.info('Checking step %d, client %d',
                    index, self.config['client_id'])

        buf = struct.pack('>I', constants.CMD_PUSH_STEP)
        self.conn.sendall(buf)
        sendJSON(self.conn, hdr)
        try:
            self._wait_for_confirmation()
            LOGGER.info('Client %d already has the step!',
                        self.config['client_id'])
            # client already had the step file
            return
        except Exception:
            LOGGER.info('Client %d does not have step %d!',
                        self.config['client_id'], index)
            pass

        # client-side verification of the step failed,
        # we need to push a new copy

        LOGGER.info('Sending step %d to client %d. Total size: %d bytes',
                    index, self.config['client_id'], len(data))
        buf = struct.pack('>I{}s'.format(len(data)),
                          len(data), data)
        self.conn.sendall(buf)

        self._wait_for_confirmation()
