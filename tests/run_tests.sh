#!/bin/bash

# General test
python -m pytest -v tests/test_graph_uses.py

# Test ROS
python tests/communication_pattern_test.py --framework=ros --case=1-1
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/communication_pattern_test.py --framework=ros --case=1-2
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/communication_pattern_test.py --framework=ros --case=2-1
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/control_loop_subgraph_test.py --framework=ros
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/control_loop_test.py --framework=ros
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/deadline_test.py --framework=ros
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/nested_graph_test.py --framework=ros
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/subgraph_test.py --framework=ros
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/logging_test.py --framework=ros
if [ $? -ne 0 ] ; then exit 1 ; fi

# Test Ray
python tests/communication_pattern_test.py --framework=ray --case=1-1
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/communication_pattern_test.py --framework=ray --case=1-2
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/communication_pattern_test.py --framework=ray --case=2-1
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/control_loop_subgraph_test.py --framework=ray
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/control_loop_test.py --framework=ray
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/deadline_test.py --framework=ray
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/nested_graph_test.py --framework=ray
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/subgraph_test.py --framework=ray
if [ $? -ne 0 ] ; then exit 1 ; fi
python tests/logging_test.py --framework=ray
if [ $? -ne 0 ] ; then exit 1 ; fi
