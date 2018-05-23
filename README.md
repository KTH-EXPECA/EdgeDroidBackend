# Control server for Gabriel Client Emulator

Configures and collects statistics from Gabriel Client Emulators.

## Requirements

- Install and configure [Docker](https://www.docker.com/).

  - Although the script automatically downloads and builds the required Docker container image if needed, it's still recommended to do it once by hand: ```docker pull jamesjue/gabriel-lego```

- Install [tcpdump](http://www.tcpdump.org/) and configure your *sudoers* file to allow capture of packets as a non-root user:

```bash
$ sudo visudo

...
# append the following line for sudo-less tcpdump.
# replace <username> with your username
<username> ALL=(ALL) NOPASSWD:/usr/sbin/tcpdump
...
```

- Cellphone clients must be running the latest version of the [ClientEmulator](https://github.com/molguin92/GabrielClientEmulator).

## Usage

1. Setup a virtualenv with Python 3.6: 
```bash
$ virtualenv --python=python3 ./venv
$ . ./venv/bin/activate
```

2. Install dependencies:

```bash
$ pip install -r requirements.txt
```

3. Set up an experiment configuration JSON file. See file ```example_experiment_config.json``` for an example of the general structure.

4. Run the backend, passing your configuration file as argument:
```bash
$ python ./control.py experiment_config.json --output_dir=/tmp/control_test
```

