import socket
from threading import Thread, Event, Condition, RLock
from queue import Queue

import constants
from custom_logging.logging import LOGGER
from net_utils import *


class Client:
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

    def __init__(self, conn, address, config):
        self.config = config
        self.conn = conn
        self.address = address
        self.stats = None

        self.op_queue = Queue()
        self.shutdown_flag = Event()
        self.stats_cond = Condition(lock=RLock())

        self.worker = Thread(target=Client.__worker_run, args=(self,))
        self.worker.start()

    def wait_for_tasks(self):
        self.op_queue.join()

    def close(self):
        self.op_queue.put(Client.__close)

    def __close(self):
        self.conn.shutdown(socket.SHUT_RDWR)
        self.conn.close()

    def shutdown(self):
        self.op_queue.put((Client.__shutdown,))

    def __shutdown(self):
        buf = struct.pack('>I', constants.CMD_SHUTDOWN)
        self.conn.sendall(buf)
        self.__close()

    def __worker_run(self):
        while not self.shutdown_flag.is_set():
            # wait until an operation is available and then execute
            op, args = self.op_queue.get(block=True)
            op(*(self, *args))
            self.op_queue.task_done()

    def __wait_for_confirmation(self):
        # wait for confirmation by client
        # client sends back a byte with value 0x00000001 when ready
        confirmation_b = recvall(self.conn, 4)
        (confirmation,) = struct.unpack('>I', confirmation_b)
        if confirmation != constants.STATUS_SUCCESS:
            raise Exception(
                f'Client {self.address}: error on confirmation!')

    def ntp_sync(self):
        self.op_queue.put((Client.__ntp_sync,))

    def __ntp_sync(self):
        buf = struct.pack('>I', constants.CMD_SYNC_NTP)
        self.conn.sendall(buf)
        self.__wait_for_confirmation()

    def fetch_stats(self):
        self.op_queue.put((Client.__get_stats,))

    def get_stats(self):
        with self.stats_cond:
            while not self.stats:
                self.stats_cond.wait()
        return self.stats

    def __get_stats(self):
        buf = struct.pack('>I', constants.CMD_PULL_STATS)
        self.conn.sendall(buf)
        incoming_stats = recvJSON(self.conn)

        with self.stats_cond:
            self.stats = incoming_stats
            self.stats_cond.notify_all()

    def send_config(self):
        self.op_queue.put((Client.__send_config,))

    def __send_config(self):
        buf = struct.pack('>I', constants.CMD_PUSH_CONFIG)
        self.conn.sendall(buf)
        sendJSON(self.conn, self.config)
        self.__wait_for_confirmation()

    def execute_experiment(self):
        self.op_queue.put((Client.__run_experiment,))

    def __run_experiment(self):
        LOGGER.info('Client %d starting experiment!',
                    self.config['client_id'])

        buf = struct.pack('>I', constants.CMD_START_EXP)
        self.conn.sendall(buf)
        self.__wait_for_confirmation()

        # wait for experiment finish
        confirmation_b = recvall(self.conn, 4)
        (confirmation,) = struct.unpack('>I', confirmation_b)
        if confirmation != constants.MSG_EXPERIMENT_FINISH:
            raise RuntimeError(
                f'Client {self.address}: error on experiment finish!')

        LOGGER.info('Client %d done!', self.config['client_id'])

    def send_step(self, index, checksum, data):
        self.op_queue.put((Client.__send_step,
                           (index, checksum, data)))

    def __send_step(self, index, checksum, data):
        # first build and send header
        hdr = {
            constants.STEP_METADATA_INDEX : index,
            constants.STEP_METADATA_SIZE  : len(data),
            constants.STEP_METADATA_CHKSUM: checksum
        }

        LOGGER.info('Client %d checking step %d',
                    self.config['client_id'], index)

        buf = struct.pack('>I', constants.CMD_PUSH_STEP)
        self.conn.sendall(buf)
        sendJSON(self.conn, hdr)
        try:
            self.__wait_for_confirmation()
            LOGGER.info('Client %d already has step %d!',
                        self.config['client_id'], index)
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

        self.conn.sendall(data)
        self.__wait_for_confirmation()
