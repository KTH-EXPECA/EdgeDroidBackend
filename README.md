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

```json

{
  "experiment_id": <name of the experiment>,
  "clients": <number of clients>,
  "runs": <number of repetitions per client>,
  "steps": <number of steps in the task>,
  "trace_root_url": <URL for downloading .trace files for the steps. Should end in a trailing forward slash ("/")!>,
  "ports": [ <one entry per client!:>
    {
      "video": <video port>,
      "result": <result port>,
      "control": <control port>
    }
  ]
}

```