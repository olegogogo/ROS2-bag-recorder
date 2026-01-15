import os
from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch_ros.actions import Node

def generate_launch_description():
    config = os.path.join(
        get_package_share_directory('ros2_bag_recorder'),
        'config',
        'bag_recorder.yaml'
    )

    return LaunchDescription([
        Node(
            package='ros2_bag_recorder',
            executable='bag_session_manager',
            name='bag_session_manager',
            parameters=[config],
            output='screen'
        )
    ])
