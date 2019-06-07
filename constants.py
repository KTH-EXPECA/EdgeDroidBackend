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

# Protocol definition for control server commands

CONTROL_SERVER_VERSION = "0.7.2"

DEFAULT_CONTROLSERVER_HOST = '0.0.0.0'
DEFAULT_CONTROLSERVER_PORT = 1337

STATUS_SUCCESS = 0x00000001
STATUS_ERROR = 0xffffffff

MSG_EXPERIMENT_FINISH = 0x000000b1

CMD_PUSH_CONFIG = 0x000000a1
CMD_PULL_STATS = 0x000000a2
CMD_START_EXP = 0x000000a3
# CMD_FETCH_TRACES = 0x000000a4
CMD_PUSH_STEP = 0x000000a4
CMD_SYNC_CLOCK = 0x000000a5

CMD_SYNC_CLOCK_START = 0xa0000001
CMD_SYNC_CLOCK_END = 0xa0000002
CMD_SYNC_CLOCK_BEACON = 0xa0000010
CMD_SYNC_CLOCK_REPLY = 0xa0000011

CMD_SHUTDOWN = 0x000000af

# LEGO_DOCKER_IMG = 'jamesjue/gabriel-lego'
SYSTEM_STATS = 'system_stats.csv'
SERVER_STATS = 'server_stats.json'
CLIENT_STATS = '{:02}_stats.json'

STEP_FILE_FMT = 'step_{}.trace'
STEP_METADATA_INDEX = 'index'
STEP_METADATA_SIZE = 'size'
STEP_METADATA_CHKSUM = 'md5checksum'

# defaults
DEFAULT_EXPERIMENT_CONFIG_FILENAME = 'experiment_config.toml'
DEFAULT_VIDEO_PORT = 9098
DEFAULT_RESULT_PORT = 9111
DEFAULT_CONTROL_PORT = 22222
DEFAULT_START_WINDOW = 10.0
DEFAULT_NTP_POLL_COUNT = 11

NET_IFACE = 'enp0s31f6'  # TODO: Parametrize

TCPDUMP_CMD_PREFIX = ['tcpdump', '-s 0', '-i {}'.format(NET_IFACE)]
TCPDUMP_CMD_SUFFIX = ['-w tcp.pcap']

CPU_LOAD_IMG = 'molguin/cpuload'
