#!/bin/bash

###############################################################################
# Install ERDOS
###############################################################################

# Install pip packages
sudo apt-get install -y python-pip

# Install the erdos package
pip install -e ./
