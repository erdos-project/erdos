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

# Get ROS Kinetic for Ubuntu 16.04
RUN sudo sh -c 'echo "deb http://packages.ros.org/ros/ubuntu xenial main" > /etc/apt/sources.list.d/ros-latest.list'
RUN sudo apt-key adv --keyserver hkp://ha.pool.sks-keyservers.net:80 --recv-key 421C365BD9FF1F717815A3895523BAEEB01FA116
RUN sudo apt-get update && sudo apt-get -y install ros-kinetic-desktop-full

# Source the ROS setup script at the init of the shell.
RUN echo "source /opt/ros/kinetic/setup.bash" >> ~/.bashrc

# Get the erdos directory.
RUN mkdir -p /home/erdos/workspace/erdos
ADD . /home/erdos/workspace/erdos
RUN echo "export PYTHONPATH=\$PYTHONPATH:/home/erdos/workspace/erdos" >> ~/.bashrc
WORKDIR /home/erdos/workspace/erdos

# Install pip and get all the requirements.
RUN sudo apt-get install -y python-pip
RUN pip install -r /home/erdos/workspace/erdos/requirements.txt

# Initialize ROS
RUN sudo rosdep init && rosdep update
RUN sudo apt-get -y install python-rosinstall
RUN mkdir -p /home/erdos/workspace/src
RUN source /opt/ros/kinetic/setup.bash && cd ~/workspace && catkin_make

# Install prerequisites for the-feeling-of-success.
RUN sudo apt-get -y install git-core python-argparse python-wstool python-vcstools python-rosdep ros-kinetic-control-msgs ros-kinetic-joystick-drivers ros-kinetic-xacro ros-kinetic-tf2-ros ros-kinetic-rviz ros-kinetic-cv-bridge ros-kinetic-actionlib ros-kinetic-actionlib-msgs ros-kinetic-dynamic-reconfigure ros-kinetic-trajectory-msgs ros-kinetic-rospy-message-converter net-tools
RUN cd ~/workspace/src && wstool init .
RUN cd ~/workspace/src && git clone https://github.com/RethinkRobotics/sawyer_robot.git
RUN cd ~/workspace/src && wstool merge sawyer_robot/sawyer_robot.rosinstall && wstool update
RUN source /opt/ros/kinetic/setup.bash && cd ~/workspace && catkin_make 
RUN cp ~/workspace/src/intera_sdk/intera.sh ~/workspace
RUN cd ~/workspace && sed -i "26cyour_ip=\"$(hostname --ip-address)\"" intera.sh
RUN cd ~/workspace && sed -i "30cros_version=\"kinetic\"" intera.sh

RUN sudo apt-get install -y gazebo7 ros-kinetic-qt-build ros-kinetic-gazebo-ros-control ros-kinetic-gazebo-ros-pkgs ros-kinetic-ros-control ros-kinetic-control-toolbox ros-kinetic-realtime-tools ros-kinetic-ros-controllers ros-kinetic-xacro python-wstool ros-kinetic-tf-conversions ros-kinetic-kdl-parser ros-kinetic-sns-ik-lib wget tmux
RUN cd ~/workspace/src && git clone https://github.com/RethinkRobotics/sawyer_simulator.git
RUN cd ~/workspace/src && wstool merge sawyer_simulator/sawyer_simulator.rosinstall && wstool update
RUN cd ~/workspace && source /opt/ros/kinetic/setup.bash && catkin_make
RUN sudo sh -c 'echo "deb http://packages.osrfoundation.org/gazebo/ubuntu-stable `lsb_release -cs` main" > /etc/apt/sources.list.d/gazebo-stable.list'
RUN wget http://packages.osrfoundation.org/gazebo.key -O - | sudo apt-key add -
RUN sudo apt-get update
RUN sudo apt-get install -y gazebo7

# Issues when running Gazebo on Docker
RUN echo "export QT_X11_NO_MITSHM=1" >> ~/.bashrc

# Apply changes to Sawyer
RUN cp ~/workspace/erdos/examples/the-feeling-of-success/rethink_electric_gripper.xacro ~/workspace/src/intera_common/intera_tools_description/urdf/electric_gripper/
RUN cp ~/workspace/erdos/examples/the-feeling-of-success/sawyer_base.gazebo.xacro ~/workspace/src/sawyer_robot/sawyer_description/urdf/
RUN cp ~/workspace/erdos/examples/the-feeling-of-success/sawyer_world.launch ~/workspace/src/sawyer_simulator/sawyer_gazebo/launch/
