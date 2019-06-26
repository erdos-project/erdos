#!/bin/bash

sudo apt-get install -y lsb-release
os_version=$(lsb_release -sr)

###############################################################################
# Install ROS
###############################################################################
if [ $os_version == '16.04' ]
then
    # Install and setup ROS.
    sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu xenial main" > /etc/apt/sources.list.d/ros-latest.list'
    sudo apt-key adv --keyserver hkp://ha.pool.sks-keyservers.net:80 --recv-key 421C365BD9FF1F717815A3895523BAEEB01FA116
    sudo apt-get -y update
    sudo apt-get install -y --allow-unauthenticated ros-kinetic-ros-base
    echo "source /opt/ros/kinetic/setup.bash" >> ~/.bashrc
    source /opt/ros/kinetic/setup.bash
elif [ $os_version == '18.04' ]
then
    sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu bionic main" > /etc/apt/sources.list.d/ros-latest.list'
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-key 421C365BD9FF1F717815A3895523BAEEB01FA116
    sudo apt-get -y update
    sudo apt-get install -y --allow-unauthenticated ros-melodic-ros-base
    echo "source /opt/ros/melodic/setup.bash" >> ~/.bashrc
    source /opt/ros/melodic/setup.bash
else
    echo "Unsupported OS version"
    exit 1
fi
sudo apt-get install -y --allow-unauthenticated python-cv-bridge python-rosinstall

# Initialize rosdep
sudo rosdep init
rosdep update

###############################################################################
# Install ERDOS
###############################################################################

# Install pip packages
sudo apt-get install -y python-pip

# Install the erdos package
pip install -e ./
