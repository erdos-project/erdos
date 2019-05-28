import carla

# ERDOS specific imports.
from erdos.op import Op
from erdos.utils import setup_logging

# Pylot specific imports.
import pylot_utils
import simulation.carla_utils


class WaypointVisualizerOperator(Op):
    """ WaypointVisualizerOperator visualizes the waypoints released by a
    global route planner.

    This operator listens on the `wp_debug` feed and draws the waypoints on the
    world simulation screen.

    Attributes:
        _world: A handle to the world to draw the waypoints on.
    """

    def __init__(self, name, flags, log_file_name=None):
        """ Initializes the WaypointVisualizerOperator with the given
        parameters.

        Args:
            name: The name of the operator.
            flags: A handle to the global flags instance to retrieve the
                configuration.
            log_file_name: The file to log the required information to.
        """
        super(WaypointVisualizerOperator, self).__init__(name)
        self._logger = setup_logging(self.name, log_file_name)
        self._flags = flags
        _, self._world = simulation.carla_utils.get_world()
        if self._world is None:
            raise ValueError("Error connecting to the simulator.")

    @staticmethod
    def setup_streams(input_streams):
        """ This method takes in a single input stream called `wp_debug` which
        sends a `planning.messages.WaypointMessage` to be drawn on
        the screen.

        Args:
            input_streams: A list of streams to take the input from (length=2)

        Returns:
            An empty list representing that this operator does not publish
            any output streams.
        """
        if len(input_streams) > 1:
            raise ValueError(
                "The WaypointVisualizerOperator should not receive more than"
                " two inputs. Please check the graph connections.")

        input_streams.filter(pylot_utils.is_waypoints_stream).add_callback(
            WaypointVisualizerOperator.on_wp_update)
        return []

    def on_wp_update(self, msg):
        """ The callback function that gets called upon receipt of the
        waypoint to be drawn on the screen.

        Args:
            msg: A message of type `planning.messages.WaypointMessage` to
                be drawn on the screen.
        """
        loc = carla.Location(msg.waypoint.location.x,
                             msg.waypoint.location.y,
                             msg.waypoint.location.z)
        begin = loc + carla.Location(z=0.5)
        end = begin + carla.Location(msg.waypoint.orientation.x,
                                     msg.waypoint.orientation.y,
                                     msg.waypoint.orientation.z)
        self._world.debug.draw_arrow(begin,
                                     end,
                                     arrow_size=0.3,
                                     life_time=30.0)
