"""
 Copyright 2019 Manuel Olguín
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""
import datetime
import socket
import time
from threading import Thread, Event, Condition, RLock
from queue import Queue, Empty

import constants
from custom_logging.logging import LOGGER
from net_utils import *


class NullClient:
    def __init__(self, *args):
        pass

    def wait_for_tasks(self):
        pass

    def shutdown(self):
        pass

    def clock_sync(self):
        pass

    def fetch_stats(self):
        pass

    def get_stats(self):
        pass

    def send_config(self):
        pass

    def execute_experiment(self, init_offset):
        pass

    def send_step(self, index, checksum, data):
        pass


class AsyncClient(NullClient):
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
        super(AsyncClient, self).__init__()
        self.config = config
        self.conn = conn
        self.address = address
        self.stats = None

        self.op_queue = Queue()
        self.shutdown_flag = Event()
        self.stats_cond = Condition(lock=RLock())

        self.worker = Thread(target=AsyncClient.__worker_run, args=(self,))
        self.worker.start()

    def __enqueue_task(self, fn, *args, **kwargs):
        self.op_queue.put((fn, args, kwargs))

    def wait_for_tasks(self):
        self.op_queue.join()

    def shutdown(self):
        # not asynchronous by design
        LOGGER.warning(f'Shutting down client %d...', self.config['client_id'])

        # wait for worker thread to finish the current task
        self.shutdown_flag.set()
        self.worker.join()

        buf = struct.pack('>I', constants.CMD_SHUTDOWN)
        self.conn.sendall(buf)
        self.conn.shutdown(socket.SHUT_RDWR)
        self.conn.close()

        LOGGER.warning(f'Client %d shut down.', self.config['client_id'])

    def __worker_run(self):
        while not self.shutdown_flag.is_set():
            try:
                # wait until an operation is available and then execute
                op, args, kwargs = self.op_queue.get(block=True, timeout=0.1)
                op(*(self, *args), **kwargs)
                self.op_queue.task_done()
            except (InterruptedError, Empty):
                pass
            except Exception as e:
                self.shutdown_flag.set()
                raise e

    def __wait_for_confirmation(self):
        # wait for confirmation by client
        # client sends back a byte with value 0x00000001 when ready
        confirmation_b = recvall(self.conn, 4)
        (confirmation,) = struct.unpack('>I', confirmation_b)
        if confirmation != constants.STATUS_SUCCESS:
            raise Exception(
                f'Client {self.address}: error on confirmation!')

    def clock_sync(self):
        self.__enqueue_task(AsyncClient.__clock_sync)

    def __clock_sync(self):
        buf = struct.pack('>I', constants.CMD_SYNC_CLOCK)
        self.conn.sendall(buf)

        start_cmd = -1
        for i in range(10):
            # go into sync mode
            # wait for sync start:
            start_cmd_b = recvall(self.conn, 4)
            (start_cmd,) = struct.unpack('>I', start_cmd_b)
            if start_cmd != constants.CMD_SYNC_CLOCK_START:
                continue
            else:
                break

        if start_cmd != constants.CMD_SYNC_CLOCK_START:
            raise Exception(f'Client {self.address}: failed to start clock '
                            f'synchronization.')

        LOGGER.info(
            f'Client {self.config["client_id"]}: '
            f'got clock sync start command.')

        # todo: add minimum delay estimation
        while True:
            # wait for beacon
            beacon_b = recvall(self.conn, 4)
            tbr = time.time()  # we work with absolute time here
            (beacon,) = struct.unpack('>I', beacon_b)
            if beacon == constants.CMD_SYNC_CLOCK_END:
                break
            elif beacon != constants.CMD_SYNC_CLOCK_BEACON:
                raise Exception(f'Client {self.address}: did not receive '
                                f'expected beacon.')

            # reply
            # one-liner to try to get some additional efficiency in the
            # timestamping
            self.conn.sendall(
                struct.pack(
                    '>Idd',
                    constants.CMD_SYNC_CLOCK_REPLY,
                    tbr * 1000.0,  # 1 s = 1 000 ms
                    time.time() * 1000.0
                ))

        # sync done.
        self.__wait_for_confirmation()

    def fetch_stats(self):
        self.__enqueue_task(AsyncClient.__get_stats)

    def get_stats(self):
        with self.stats_cond:
            while not self.stats:
                self.stats_cond.wait()
        return self.stats

    def __get_stats(self):
        LOGGER.info('Getting stats from client %d', self.config['client_id'])
        buf = struct.pack('>I', constants.CMD_PULL_STATS)
        self.conn.sendall(buf)
        incoming_stats = recvJSON(self.conn)

        with self.stats_cond:
            self.stats = incoming_stats
            self.stats_cond.notify_all()

    def send_config(self):
        self.__enqueue_task(AsyncClient.__send_config)

    def __send_config(self):
        LOGGER.info('Sending config to client %d...',
                    self.config['client_id'])
        buf = struct.pack('>I', constants.CMD_PUSH_CONFIG)
        self.conn.sendall(buf)
        sendJSON(self.conn, self.config)
        self.__wait_for_confirmation()

    def execute_experiment(self, init_offset):
        self.__enqueue_task(AsyncClient.__run_experiment, init_offset)

    def __run_experiment(self, init_offset):
        time.sleep(init_offset)  # TODO: maybe make more accurate?
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
        self.__enqueue_task(AsyncClient.__send_step, index, checksum, data)

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
