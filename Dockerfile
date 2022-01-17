FROM ubuntu:20.04

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

# Install rust.
RUN sudo apt-get -y install curl clang python3 python3-pip git
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/home/erdos/.cargo/bin:${PATH}"

# Get the erdos directory.
RUN mkdir -p /home/erdos/workspace
RUN cd /home/erdos/workspace && git clone https://github.com/erdos-project/erdos.git
WORKDIR /home/erdos/workspace/erdos
RUN cd /home/erdos/workspace/erdos

# Install erdos.
RUN cargo build --release
# Install the python package
ENV PATH="/home/erdos/.local/bin:${PATH}"
RUN pip3 install -U pip>=21.3
RUN cd python && pip3 install -e .