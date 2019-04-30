#!/bin/bash
sudo apt-get -y install nvidia-cuda-toolkit
cd ../dependencies/CenterNet/
pip install -r requirements.txt
cd models/
# Dowload model.
~/.local/bin/gdown http://drive.google.com/uc?id=1pl_-ael8wERdUREEnaIfqOV_VF2bEVRT
cd ../src/lib/models/networks/DCNv2/
./make.sh
cd ../../../external
make
