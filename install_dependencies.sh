#!/bin/bash
sudo apt install git python python-pip
sudo apt install ros-core
sudo apt install python-rospy    # Installation involves timezone config
sudo apt install python-catkin-pkg python-cv-bridge
# Install pip packages
pip install -r requirements.txt
