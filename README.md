# ERDOS
ERDOS is a platform for building self-driving cars and controlling robots.

# Getting started

The easiest way to get ERDOS running is to use our Docker image:

```
$ git clone https://github.com/erdos-project/erdos
$ docker build -t erdos .
$ docker run -it --rm -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix erdos
```

The current Docker image installs Gazebo and allows you to run our implementation
of the-feeling-of-success pipeline. To run it, just do

```
$ cd ~/ros_ws
$ ./intera.sh sim
$ roslaunch sawyer_gazebo sawyer_world.launch &
$ cd ~/ros_ws/src/ERDOS/examples/the-feeling-of-success
$ python run_experiments.py
```

To install the Carla pipeline, clone the repository and build the Docker image:

```
$ git clone https://github.com/erdos-project/erdos
$ git submodule update --init
$ docker build -t erdos-carla
$ docker run -p 2000-2002:2000-2002 -it --runtime=nvidia -e NVIDIA_VISIBLE_DEVICES=0 --shm-size="14g" erdos /bin/bash
```

To run the pipeline, do the following:

```
$ cd ~/workspace/CARLA_0_8_4/ && ./CarlaUE4.sh -windowed -ResX=8 -ResY=8 -carla-server
$ cd ~/workspace/ERDOS/examples/benchmarks/pylot && python pylot.py --replay=True
```


# Local instalation

## System requirements

ERDOS is currently known to work on Ubuntu LTS 16.04. It does not
work on OS X or Windows because of its dependency on ROS.

## Instalation instructions
After cloning the repository, run

```console
./install_dependencies.sh
```

This script installs ROS and Python dependencies.

## Running an example

```console
export PYTHONPATH=$PYTHONPATH:<PATH_TO_ERDOS_DIR>
cd examples
python pylot/pylot.py --framework=ray
```

```console
export PYTHONPATH=$PYTHONPATH:<PATH_TO_ERDOS_DIR>
# Setup the ROS paths
source /opt/ros/kinetic/setup.bash
# Run the pipeline using ROS
python pylot/pylot.py --framework=ros
```

# Getting involved
If you would like to contact us, please send an email to:
erdos-developers@googlegroups.com, or create an issue on GitHub.

We always welcome contributions to ERDOS. One way to get started is to
pick one of the issues tagged with **good first issue** -- these are usually
good issues that help you familiarize yourself with the ERDOS code base. Please
submit contributions using pull requests.
