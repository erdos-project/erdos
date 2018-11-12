#!/bin/bash

# Install and setup ROS.
sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu xenial main" > /etc/apt/sources.list.d/ros-latest.list'
sudo apt-key adv --keyserver hkp://ha.pool.sks-keyservers.net:80 --recv-key 421C365BD9FF1F717815A3895523BAEEB01FA116
sudo apt-get update
sudo apt-get install -y ros-kinetic-ros-base python-rospkg
echo "source /opt/ros/kinetic/setup.bash" >> ~/.bashrc

# Install pip packages
sudo apt-get install -y python-pip
pip install -r requirements.txt
