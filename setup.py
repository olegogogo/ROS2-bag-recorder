from setuptools import setup
import os
from glob import glob

package_name = 'ros2_bag_recorder'

setup(
    name=package_name,
    version='0.0.1',
    packages=[package_name],
    data_files=[
        ('share/' + package_name, ['package.xml']),
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        (os.path.join('share', package_name, 'launch'), glob('launch/*.py')),
        (os.path.join('share', package_name, 'config'), glob('config/*.yaml')),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='devitt',
    maintainer_email='devittdv@gmail.com',
    description='ROS2 Node for automated bag recording based on Mavros state',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'bag_session_manager = ros2_bag_recorder.bag_session_manager:main',
        ],
    },
)
