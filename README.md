# Control server for Gabriel Client Emulator

Configures and collects statistics from Gabriel Client Emulators.

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

3. Set up an experiment configuration JSON file (see next section).
4. Run the backend, passing your configuration file as argument:
```bash
$ python ./control.py experiment_config.json
```

## JSON Experiment Config

The experiment configuration is a JSON file which is parsed by the control script and pushed to the clients.

General structure:

```

{
  "experiment_id": <name of the experiment, string>,
  "clients": <number of clients, int>,
  "runs": <number of repetitions per client, int>,
  "steps": <number of steps in the task, int>,
  "trace_root_url": <URL for downloading .trace files for the steps, should end in a trailing forward slash ("/"), string>,
  "ports": [ <one entry per client!:>
    {
      "video": <video port, int>,
      "result": <result port, int>,
      "control": <control port, int>
    }
  ]
}

```