# ERDOS
ERDOS is a platform for building self-driving cars and controlling robots.

[![Build Status](https://travis-ci.org/erdos-project/erdos.svg)](https://travis-ci.org/erdos-project/erdos)
[![Documentation Status](https://readthedocs.org/projects/erdos/badge/?version=latest)](https://erdos.readthedocs.io/en/latest/?badge=latest)

# Getting started

The easiest way to get ERDOS running is to use our Docker image:

```console
docker pull erdosproject/erdos
```

# Local installation

## System requirements

ERDOS is currently known to work on Ubuntu LTS 16.04.

## Installation instructions
After cloning the repository, run

```console
source ./install.sh
```

This script installs ROS and Python dependencies.

## Running an example

```console
# Run the test using Ray
python tests/sum_squares_test.py --framework=ray
# Run the test using ROS
python tests/sum_squares_test.py --framework=ros
```

# Getting involved
If you would like to contact us, please send an email to:
erdos-developers@googlegroups.com, or create an issue on GitHub.

We always welcome contributions to ERDOS. One way to get started is to
pick one of the issues tagged with **good first issue** -- these are usually
good issues that help you familiarize yourself with the ERDOS code base. Please
submit contributions using pull requests.
