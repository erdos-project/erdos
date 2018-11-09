#!/bin/bash

# Test ROS
python tests/communication_1to1_test.py --framework=ros
python tests/communication_1to2_test.py --framework=ros
python tests/communication_2to1_test.py --framework=ros
python control_loop_test.py --framework=ros

# Test Ray
python tests/communication_1to1_test.py --framework=ray
python tests/communication_1to2_test.py --framework=ray
python tests/communication_2to1_test.py --framework=ray
python control_loop_test.py --framework=ray