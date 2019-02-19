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

import logging
import sys
from multiprocessing import RLock

# stolen from user KCJ @ StackOverflow
# source:
# https://stackoverflow.com/questions/384076/how-can-i-color-python-logging
# -output

from copy import copy
from logging import Formatter

MAPPING = {
    'DEBUG'   : 37,  # white
    'INFO'    : 36,  # cyan
    'WARNING' : 33,  # yellow
    'ERROR'   : 31,  # red
    'CRITICAL': 41,  # white on red bg
}

PREFIX = '\033['
SUFFIX = '\033[0m'


class ColoredFormatter(Formatter):

    def __init__(self, patern):
        Formatter.__init__(self, patern)

    def format(self, record):
        colored_record = copy(record)
        levelname = colored_record.levelname
        seq = MAPPING.get(levelname, 37)  # default white
        colored_levelname = ('{0}{1}m{2}{3}') \
            .format(PREFIX, seq, levelname, SUFFIX)
        colored_record.levelname = colored_levelname
        return Formatter.format(self, colored_record)


class ConcurrentLog:
    FILE_LOG_FMT = logging.Formatter(
        '%(asctime)s - %(levelname)s: %(message)s'
    )
    STREAM_LOG_FMT = ColoredFormatter(
        '%(asctime)s - %(levelname)s: %(message)s'
    )

    def __init__(self, file='control.log',
                 stream=sys.stdout,
                 level=logging.INFO):
        self.logger = logging.getLogger('ExperimentControl')

        stream_hdlr = logging.StreamHandler(stream=stream)
        file_hdlr = logging.FileHandler(file)
        stream_hdlr.setFormatter(self.STREAM_LOG_FMT)
        file_hdlr.setFormatter(self.FILE_LOG_FMT)

        self.logger.addHandler(stream_hdlr)
        self.logger.addHandler(file_hdlr)
        self.logger.setLevel(level)

        self.lock = RLock()

    def info(self, *args, **kwargs):
        with self.lock:
            self.logger.info(*args, **kwargs)

    def warning(self, *args, **kwargs):
        with self.lock:
            self.logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        with self.lock:
            self.logger.error(*args, **kwargs)

    def debug(self, *args, **kwargs):
        with self.lock:
            self.logger.debug(*args, **kwargs)

    def critical(self, *args, **kwargs):
        with self.lock:
            self.logger.critical(*args, **kwargs)


# global log instance
LOGGER = ConcurrentLog()
