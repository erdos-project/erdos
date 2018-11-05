# Getting started

## System requirements

Pylot currently works on Ubuntu 16.04. It requires pytorch, and caffe to
be installed on your machine.

## Setup instructions

```console
$ cd dependencies
$ ./get_models.sh
$ ./get_simulator.sh
$ ./set_pythonpath.sh
$ export PYTHONPATH=$PYTHONPATH:<PATH_TO_ERDOS>
```

## Running Pylot

In order to run Pylot on a Carla simulation you have first to start the simulator:
```console
$ ./dependencies/run_simulator.sh
```
Folowwing, you have to run Pylot in a different terminal:
```console
$ python pylot.py --framework=ray --replay=False
```

Alternatively, you can run Pylot without Carla. In this setup Pylot replays the
camera RGB frames stored in the images directory.

```console
$ python pylot.py --framework=ray --replay=True
```
