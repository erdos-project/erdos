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

# Get the erdos directory.
RUN sudo apt-get -y install git
RUN mkdir -p /home/erdos/workspace
RUN cd /home/erdos/workspace && git clone https://github.com/erdos-project/erdos.git
WORKDIR /home/erdos/workspace/erdos

# Install all the requirements.
RUN cd /home/erdos/workspace/erdos/ && ./install.sh
