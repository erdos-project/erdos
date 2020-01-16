#!/bin/bash

# Install pip packages
sudo apt-get install -y curl

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
export PATH=$PATH:$HOME/.cargo/bin
rustup default nightly
