# Control server for EdgeDroid

Configures experiment and collects statistics.

## Requirements

- Install and configure [Docker](https://www.docker.com/).

  - Although the script automatically downloads and builds the required Docker container image if needed, it's still recommended to do it once by hand: ```docker pull molguin/gabriel-lego```

- This is not currently needed as we're not using TCPDump for the time being! Timestamping is done at application level.

    ~~Install [tcpdump](http://www.tcpdump.org/) and configure your *sudoers* file to allow capture of packets as a non-root user:~~

```bash
$ sudo visudo

...
# append the following line for sudo-less tcpdump.
# replace <username> with your username
<username> ALL=(ALL) NOPASSWD:/usr/sbin/tcpdump
...
```


- Set up a virtualenv with Python 3.x: 
```bash
$ virtualenv --python=python3 ./venv
$ . ./venv/bin/activate
```

- Install dependencies:

```bash
$ pip install -r requirements.txt --upgrade
```

- Open port 1337 (on Ubuntu: `$ ufw allow 1337`).

## Setting up and running an experiment

1. Set up an experiment directory. Use `example_experiment_dir` as a boilerplate.

2. Set up an experiment configuration TOML file. See file `example_experiment_dir\experiment_config.toml` for an example of the general structure. 
    - Make sure all the ports declared in the experiment configuration file are open.
    - Make sure the `experiment.trace.dir` key points to the correct directory containing the experiment trace.

3. Run the backend, passing the experiment directory as a parameter. Results will be output there as well.
```bash
$ python ./control.py example_experiment_dir
```

