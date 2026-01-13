# ROS2-bag-recorder

A ROS 2 package for automated bag recording (Ubuntu 22 + ROS 2 Humble).

## Overview
This package (`bag_session_manager`) manages `ros2 bag record` sessions based on MAVROS state:
- **START**: When `mavros/state` armed becomes `True`.
- **STOP**: When `mavros/state` armed becomes `False` AND `landed_state` is `ON_GROUND`.

It handles session naming (`YYYYmmdd_HHMMSS_flightNNN`) and robust process termination.

## Authorship
This code was created by **Antigravity** (Google DeepMind) in collaboration with the Oleg.

## Installation & Usage
See `walkthrough.md` or package documentation for deployment details.
