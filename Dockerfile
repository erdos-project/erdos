FROM ubuntu:16.04

# Set up an erdos user first.
RUN apt-get -y update && apt-get -y install sudo
ENV uid 1000
ENV gid 1000

RUN mkdir -p /home/erdos
RUN groupadd erdos -g ${gid}
RUN useradd -r -u ${uid} -g erdos erdos
RUN echo "erdos ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/erdos
RUN chmod 0440 /etc/sudoers.d/erdos
RUN chown ${uid}:${gid} -R /home/erdos

USER erdos
ENV HOME /home/erdos
ENV SHELL /bin/bash

SHELL ["/bin/bash", "-c"]

# Get ROS Kinetic for Ubuntu 16.04
RUN sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu xenial main" > /etc/apt/sources.list.d/ros-latest.list'
RUN sudo apt-key adv --keyserver hkp://ha.pool.sks-keyservers.net:80 --recv-key 421C365BD9FF1F717815A3895523BAEEB01FA116
RUN sudo apt-get update && sudo apt-get -y install ros-kinetic-ros-base

# Source the ROS setup script at the init of the shell.
RUN echo "source /opt/ros/kinetic/setup.bash" >> ~/.bashrc

# Get the erdos directory.
RUN mkdir -p /home/erdos/workspace/erdos
ADD . /home/erdos/workspace/erdos
RUN echo "export PYTHONPATH=\$PYTHONPATH:/home/erdos/workspace/erdos" >> ~/.bashrc
WORKDIR /home/erdos/workspace/erdos

# Install pip and get all the requirements.
RUN sudo apt-get install -y python-pip
RUN pip install -r requirements.txt
