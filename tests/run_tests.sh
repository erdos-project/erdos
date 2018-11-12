#!/bin/bash

# Test ROS
python tests/communication_pattern_test.py --framework=ros --case=1-1
python tests/communication_pattern_test.py --framework=ros --case=1-2
python tests/communication_pattern_test.py --framework=ros --case=2-1
python tests/control_loop_test.py --framework=ros

# Test Ray
python tests/communication_pattern_test.py --framework=ray --case=1-1
python tests/communication_pattern_test.py --framework=ray --case=1-2
python tests/communication_pattern_test.py --framework=ray --case=2-1
python tests/control_loop_test.py --framework=ray