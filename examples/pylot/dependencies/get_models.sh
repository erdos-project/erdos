#!/bin/bash

mkdir data
cd data
wget https://www.dropbox.com/s/i6v54gng0rao6ff/drn_d_22_cityscapes.pth
wget https://pjreddie.com/media/files/yolov3.weights
wget https://www.dropbox.com/s/fgvvfsjuezbswy2/traffic_light_det_inference_graph.pb
wget http://download.tensorflow.org/models/object_detection/faster_rcnn_resnet101_coco_2018_01_28.tar.gz
tar -xvf faster_rcnn_resnet101_coco_2018_01_28.tar.gz
wget http://download.tensorflow.org/models/object_detection/ssd_mobilenet_v1_coco_2018_01_28.tar.gz
tar -xvf ssd_mobilenet_v1_coco_2018_01_28.tar.gz
wget http://download.tensorflow.org/models/object_detection/ssd_resnet50_v1_fpn_shared_box_predictor_640x640_coco14_sync_2018_07_03.tar.gz
tar -xvf ssd_resnet50_v1_fpn_shared_box_predictor_640x640_coco14_sync_2018_07_03.tar.gz
pip install gdown
~/.local/bin/gdown http://drive.google.com/uc?id=0B1sg8Yyw1JCDOUNsYkpQTGdLYVU
cd ../
mkdir -p conv_reg_vot/vgg_model
mv data/VGG_16_layers_py3.npz conv_reg_vot/vgg_model/
