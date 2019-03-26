#!/bin/bash

# Install and setup ROS.
sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu xenial main" > /etc/apt/sources.list.d/ros-latest.list'
sudo apt-key adv --keyserver hkp://ha.pool.sks-keyservers.net:80 --recv-key 421C365BD9FF1F717815A3895523BAEEB01FA116
sudo apt-get update
sudo apt-get install -y --allow-unauthenticated ros-kinetic-ros-base
echo "source /opt/ros/kinetic/setup.bash" >> ~/.bashrc
sudo apt install python-cv-bridge

# Install pip packages
sudo apt-get install -y python-pip
pip install erdos
