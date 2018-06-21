import os
from typing import NamedTuple, Dict

import constants
from custom_logging.logging import LOGGER

PortConfig = NamedTuple('PortConfig', video=int, results=int, control=int)


class RecursiveNestedDict(dict):
    _PATH_SEP = '.'

    def __init__(self, *args, **kwargs):
        super(RecursiveNestedDict, self).__init__(*args, **kwargs)
        for k, v in self.items():
            if isinstance(v, dict):
                self[k] = RecursiveNestedDict(v)

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            value = RecursiveNestedDict(value)

        super(RecursiveNestedDict, self).__setitem__(key, value)

    def __recursive_find(self, path: str, _previous_path: str = None):
        keys = path.split(self._PATH_SEP, maxsplit=1)

        current_path = keys[0] if not _previous_path \
            else self._PATH_SEP.join((_previous_path, keys[0]))

        if len(keys) == 1:
            # final split
            try:
                return self[keys[0]]
            except KeyError as e:
                raise KeyError(current_path) from e
        else:
            try:
                next_level = self[keys[0]]
                assert isinstance(next_level, RecursiveNestedDict)
                return next_level.__recursive_find(keys[1], current_path)
            except AssertionError:
                raise TypeError(
                    'Can\'t traverse dictionary: '
                    'element {} is not a {}'.format(
                        current_path,
                        self.__class__.__name__
                    ))

    def find(self, path: str):
        """
        Recursively finds the specified "path" through nested dictionaries,
        with origin in the current dictionary.
        Paths are specified as a sequence of keys separated by periods.
        For instance, in the following dictionary, key "c" in the innermost
        nested dictionary has path "a.b.c" from the outermost dictionary:

        {
            "a": {
                "b": {
                    "c": "Hello World!"
                }
            }
        }

        :param path: String representation of the path to find.
        :return: The value at located at the end of the given path.
        :raises: KeyError in case the path cannot be followed.
        """
        return self.__recursive_find(path)


class ConfigException(Exception):
    pass


class ExperimentConfig:
    name = None
    clients = 0
    runs = 0
    trace_steps = []
    ntp_servers = []
    num_cpus = 0
    port_configs = []

    def __init__(self, toml_config: dict):
        toml_config = RecursiveNestedDict(toml_config)
        try:
            self.name = toml_config.find('experiment.name')
            self.clients = toml_config.find('experiment.clients')
            self.runs = toml_config.find('experiment.runs')

            num_steps = toml_config.find('experiment.trace.steps')
            trace_dir = toml_config.find('experiment.trace.dir')

            # verify trace dir actually exists
            if not os.path.isdir(trace_dir):
                error_str = 'Invalid trace directory.'
                LOGGER.error(error_str)
                raise ConfigException(error_str)

            # verify step files exist
            for i in range(1, num_steps + 1):
                filename = constants.STEP_FILE_FMT.format(i)
                path = os.path.join(trace_dir, filename)
                if not os.path.isfile(path):
                    error_str = '{} does not seem to be a valid step trace'
                    'file'.format(path)
                    LOGGER.error(error_str)
                    raise Exception(error_str)
                else:
                    self.trace_steps.append(path)

            self.ntp_servers = toml_config.find('experiment.ntp.servers')
            self.num_cpus = toml_config.find('experiment.performance.cpus')

            for port_cfg in toml_config.find('experiment.ports'):
                self.port_configs.append(PortConfig(
                    video=port_cfg['video'],
                    results=port_cfg['results'],
                    control=port_cfg['control']
                ))

        except KeyError as e:
            LOGGER.error('Error when parsing TOML config.')
            LOGGER.error('Missing required configuration key: %s', *e.args)
            raise ConfigException('Missing required configuration key: '
                                  '{}'.format(*e.args))
