import logging
from absl import app
from sensor_msgs.msg import Image

from insert_table_op import InsertTableOperator
from insert_block_op import InsertBlockOperator
from init_robot_op import InitRobotOperator
from gel_sight_op import GelSightOperator
from mock_loc_obj_op import MockLocateObjectOperator
from goto_xyz_op import GoToXYZOperator
from move_above_object_op import MoveAboveObjectOperator
from mock_gripper_op import MockGripperOperator
from mock_grasp_object_op import MockGraspObjectOperator
from raise_object_op import RaiseObjectOperator
from mock_predict_grip_op import MockPredictGripOperator
from random_position_op import RandomPositionOperator
from mock_ungrasp_object_op import MockUngraspObjectOperator

import erdos.graph
from erdos.ros.ros_subscriber_op import ROSSubscriberOp

logger = logging.getLogger(__name__)
table_init_arguments = {"_x": 0.75, "_y": 0.0, "_z": 0.0, "ref_frame": "world"}
block_init_arguments = {
    "_x": 0.4225,
    "_y": 0.1265,
    "_z": 0.7725,
    "ref_frame": "world"
}
robot_init_arguments = {
    "joint_angles": {
        'right_j0': -0.041662954890248294,
        'right_j1': -1.0258291091425074,
        'right_j2': 0.0293680414401436,
        'right_j3': 2.17518162913313,
        'right_j4': -0.06703022873354225,
        'right_j5': 0.3968371433926965,
        'right_j6': 1.7659649178699421
    },
    "limb_name": "right"
}


def construct_graph(graph):
    logger.info("Starting the construction of the graph.")

    # First, insert the table in the world.
    insert_table_op = graph.add(
        InsertTableOperator, init_args=table_init_arguments)

    # Now, insert the block in the world.
    insert_block_op = graph.add(
        InsertBlockOperator, init_args=block_init_arguments)
    graph.connect([insert_table_op], [insert_block_op])

    # Initialize the robot and move it to the rest position.
    init_robot_op = graph.add(
        InitRobotOperator, init_args=robot_init_arguments)
    graph.connect([insert_block_op], [init_robot_op])

    # Initialize the gelsight operators and connect them to the rostopics.
    gel_sight_topics = [("/gelsightA/image_raw", Image, "gelsightA"),
                        ("/gelsightB/image_raw", Image, "gelsightB")]
    ros_gel_sight_op = graph.add(
        ROSSubscriberOp,
        name='ros_gel_sight',
        init_args={'ros_topics_type': gel_sight_topics},
        setup_args={'ros_topics_type': gel_sight_topics})
    gel_sight_a = graph.add(
        GelSightOperator,
        name="gelsight-a-op",
        init_args={'output_name': "gelsight-stream-a"},
        setup_args={
            'input_name': "gelsightA",
            'output_name': "gelsight-stream-a"
        })
    gel_sight_b = graph.add(
        GelSightOperator,
        name="gelsight-b-op",
        init_args={'output_name': "gelsight-stream-b"},
        setup_args={
            'input_name': "gelsightB",
            'output_name': "gelsight-stream-b"
        })
    graph.connect([ros_gel_sight_op], [gel_sight_a])
    graph.connect([ros_gel_sight_op], [gel_sight_b])

    # Retrieve the kinect images from the rostopics and feed them to the
    # object locator.
    ros_kinect_topics = [("/kinectA/image_raw", Image, "image-stream"),
                         ("/kinectA/depth_raw", Image, "depth-stream")]
    ros_kinect_op = graph.add(
        ROSSubscriberOp,
        name='ros_kinect',
        init_args={'ros_topics_type': ros_kinect_topics},
        setup_args={'ros_topics_type': ros_kinect_topics})

    locate_object_op = graph.add(
        MockLocateObjectOperator,
        name='locate-object-op',
        init_args={
            'image_stream_name': 'image-stream',
            'depth_stream_name': 'depth-stream',
            'trigger_stream_name': InitRobotOperator.stream_name
        },
        setup_args={
            'image_stream_name': 'image-stream',
            'depth_stream_name': 'depth-stream',
            'trigger_stream_name': InitRobotOperator.stream_name
        })
    graph.connect([ros_kinect_op, init_robot_op], [locate_object_op])

    # Move the Sawyer arm above the detected object.
    goto_xyz_move_above_op = graph.add(
        GoToXYZOperator,
        name='goto-xyz',
        init_args={
            'limb_name': 'right',
            'output_stream_name': 'goto-move-above'
        },
        setup_args={
            'input_stream_name': MoveAboveObjectOperator.goto_stream_name,
            'output_stream_name': 'goto-move-above'
        })
    move_above_object_op = graph.add(
        MoveAboveObjectOperator,
        name='controller',
        setup_args={
            'trigger_stream_name': MockLocateObjectOperator.stream_name,
            'goto_xyz_stream_name': 'goto-move-above'
        })
    graph.connect([locate_object_op, goto_xyz_move_above_op],
                  [move_above_object_op])
    graph.connect([move_above_object_op], [goto_xyz_move_above_op])

    # Closes the gripper.
    gripper_close_op = graph.add(
        MockGripperOperator,
        name="gripper-close-op",
        init_args={
            'gripper_speed': 0.25,
            'output_stream_name': 'gripper_close_stream'
        },
        setup_args={
            'gripper_stream': MockGraspObjectOperator.gripper_stream,
            'output_stream_name': 'gripper_close_stream'
        })
    grasp_object_op = graph.add(
        MockGraspObjectOperator,
        name='mock-grasp-object',
        setup_args={
            'trigger_stream_name': MoveAboveObjectOperator.stream_name,
            'gripper_stream_name': 'gripper_close_stream'
        })
    graph.connect([move_above_object_op, gripper_close_op], [grasp_object_op])
    graph.connect([grasp_object_op], [gripper_close_op])

    # Raises the object.
    raise_object_op = graph.add(
        RaiseObjectOperator,
        name='raise-object',
        setup_args={
            'location_stream_name': MockLocateObjectOperator.stream_name,
            'trigger_stream_name': MockGraspObjectOperator.
            action_complete_stream_name
        })
    goto_xyz_raise_op = graph.add(
        GoToXYZOperator,
        name="goto-xyz-raise",
        init_args={
            'limb_name': 'right',
            'output_stream_name': 'goto_xyz_raise'
        },
        setup_args={
            'input_stream_name': RaiseObjectOperator.stream_name,
            'output_stream_name': 'goto_xyz_raise'
        })
    graph.connect([locate_object_op, grasp_object_op], [raise_object_op])
    graph.connect([raise_object_op], [goto_xyz_raise_op])

    # Predicts whether the grip was successful using the gelsight cameras.
    predict_grip_op = graph.add(
        MockPredictGripOperator,
        name='predict-grip-op',
        setup_args={
            'gel_sight_a_stream_name': 'gelsight-stream-a',
            'gel_sight_b_stream_name': 'gelsight-stream-b',
            'trigger_stream_name': 'goto_xyz_raise'
        })

    graph.connect([gel_sight_a, gel_sight_b, goto_xyz_raise_op],
                  [predict_grip_op])

    # If the grip is successful, we return it to a random location.
    random_position_op = graph.add(
        RandomPositionOperator,
        name="random-pos-op",
        setup_args={
            'locate_object_stream_name': MockLocateObjectOperator.stream_name,
            'trigger_stream_name': MockPredictGripOperator.success_stream_name,
            'goto_xyz_stream_name': 'goto_random_pos'
        })

    goto_xyz_random_op = graph.add(
        GoToXYZOperator,
        name="goto-xyz-random",
        init_args={
            'limb_name': 'right',
            'output_stream_name': 'goto_random_pos'
        },
        setup_args={
            'input_stream_name': RandomPositionOperator.position_stream_name,
            'output_stream_name': 'goto_random_pos'
        })
    graph.connect([locate_object_op, predict_grip_op, goto_xyz_random_op],
                  [random_position_op])
    graph.connect([random_position_op], [goto_xyz_random_op])

    # Now, ungrasp the object.
    gripper_open_op = graph.add(
        MockGripperOperator,
        name="gripper-open-op",
        init_args={
            'gripper_speed': 0.25,
            'output_stream_name': 'gripper_open_stream'
        },
        setup_args={
            'gripper_stream': MockUngraspObjectOperator.gripper_stream,
            'output_stream_name': 'gripper_open_stream'
        })
    ungrasp_object_op = graph.add(
        MockUngraspObjectOperator,
        name = "ungrasp-object-op",
        setup_args = {
            'trigger_stream_name': RandomPositionOperator.\
                                    action_complete_stream_name,
            'gripper_stream_name': 'gripper_open_stream'
        })
    graph.connect([random_position_op, gripper_open_op], [ungrasp_object_op])
    graph.connect([ungrasp_object_op], [gripper_open_op])

    logger.info("Finished constructing the execution graph!")


def main(argv):
    # Create the graph.
    graph = erdos.graph.get_current_graph()
    construct_graph(graph)

    # Execute the graph.
    graph.execute("ros")
    try:
        while True:
            pass
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    app.run(main)
