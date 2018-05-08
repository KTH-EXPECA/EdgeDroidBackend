# Protocol definition for control server commands
import os

DEFAULT_CONTROLSERVER_HOST = '0.0.0.0'
DEFAULT_CONTROLSERVER_PORT = 1337

STATUS_SUCCESS = 0x00000001
STATUS_ERROR = 0xffffffff

CMD_PUSH_CONFIG = 0x000000a1
CMD_PULL_STATS = 0x000000a2
CMD_START_EXP = 0x000000a3
CMD_FETCH_TRACES = 0x000000a4

CMD_SHUTDOWN = 0x000000af

LEGO_DOCKER_IMG = 'jamesjue/gabriel-lego'
SYSTEM_STATS = 'system_stats.csv'
CLIENT_STATS = '{:02}_stats.json'

# defaults
DEFAULT_OUTPUT_DIR = '{}/results/'.format(os.getcwd())
DEFAULT_EXPCONFIG_PATH = '{}/experiment_config.json'.format(os.getcwd())
DEFAULT_VIDEO_PORT = 9098
DEFAULT_RESULT_PORT = 9111
DEFAULT_CONTROL_PORT = 22222
DEFAULT_STAGGER_INTERVAL = 1.0 # seconds!


NET_IFACE = 'enp0s31f6'

TCPDUMP_CMD_PREFIX = ['tcpdump', '-s 0', '-i {}'.format(NET_IFACE)]
TCPDUMP_CMD_SUFFIX = ['-w tcp.pcap']
