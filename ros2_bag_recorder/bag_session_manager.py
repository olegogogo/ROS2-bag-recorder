#!/usr/bin/env python3

import os
import sys
import time
import signal
import subprocess
import glob
import re
from datetime import datetime
from threading import Timer

import rclpy
from rclpy.node import Node
from rclpy.duration import Duration
from rclpy.qos import QoSProfile, QoSHistoryPolicy, QoSReliabilityPolicy, QoSDurabilityPolicy

from mavros_msgs.msg import State, ExtendedState, StatusText
from rcl_interfaces.msg import ParameterDescriptor, ParameterType
import threading

class BagSessionManager(Node):
    def __init__(self):
        super().__init__('bag_session_manager')

        # Parameters
        self.declare_parameter('base_dir', '/media/orangepi/InnoSpector/rosbags')
        self.declare_parameter('stop_retry_interval_sec', 0.5)
        self.declare_parameter('stop_sigint_retries', 20)
        self.declare_parameter('stop_sigterm_timeout_sec', 5.0)
        self.declare_parameter('use_sigkill', False)
        
        # Topics to record
        self.declare_parameter(
            'topics_to_record', 
            [''], 
            ParameterDescriptor(type=ParameterType.PARAMETER_STRING_ARRAY)
        )

        self.base_dir = self.get_parameter('base_dir').get_parameter_value().string_value
        self.stop_retry_interval = self.get_parameter('stop_retry_interval_sec').get_parameter_value().double_value
        self.stop_sigint_retries = self.get_parameter('stop_sigint_retries').get_parameter_value().integer_value
        self.stop_sigterm_timeout = self.get_parameter('stop_sigterm_timeout_sec').get_parameter_value().double_value
        self.use_sigkill = self.get_parameter('use_sigkill').get_parameter_value().bool_value
        self.topics_to_record = self.get_parameter('topics_to_record').get_parameter_value().string_array_value

        # State variables
        self.proc = None
        self.last_armed = None
        self.last_landed_state = None
        self.pending_stop = False
        self.pending_start = False
        self.stop_timer = None
        self.stop_attempts = 0

        # Create QoS profile for state subscriptions (Best to be reliable)
        qos_bucket = QoSProfile(
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            reliability=QoSReliabilityPolicy.BEST_EFFORT, # standard for sensor data/state usually
            durability=QoSDurabilityPolicy.VOLATILE
        )
        
        # MAVROS state is essentially reliable in default profile, but let's check standard usage.
        # Actually ros2 mavros usually uses SystemDefault or SensorData. Let's stick to default (reliable/keep last 10) or explicit reliable.
        # However user didn't specify, so I will use default QoS for now which matches most standard pubs.

        self.sub_state = self.create_subscription(
            State,
            '/mavros/state',
            self.state_cb,
            10
        )
        
        self.sub_extended_state = self.create_subscription(
            ExtendedState,
            '/mavros/extended_state',
            self.extended_state_cb,
            10
        )

        self.pub_statustext = self.create_publisher(
            StatusText,
            '/mavros/statustext/send',
            10
        )

        self.get_logger().info(f'Bag Session Manager initialized. Base dir: {self.base_dir}')

    def state_cb(self, msg: State):
        current_armed = msg.armed
        
        # Detect transition False -> True (ARM START)
        if self.last_armed is not None and not self.last_armed and current_armed:
            self.get_logger().info('Detected ARM transition. Requesting START.')
            self.handle_start_request()

        # Detect transition True -> False (DISARM pending STOP)
        if self.last_armed is not None and self.last_armed and not current_armed:
            self.get_logger().info('Detected DISARM transition. Checking landed state for STOP.')
            self.handle_stop_request()

        self.last_armed = current_armed

    def extended_state_cb(self, msg: ExtendedState):
        self.last_landed_state = msg.landed_state
        
        # If we were waiting for ON_GROUND to stop
        if self.pending_stop:
             if self.last_landed_state == ExtendedState.LANDED_STATE_ON_GROUND:
                 self.get_logger().info('Landed state confirmed ON_GROUND. Executing STOP.')
                 self.pending_stop = False
                 self.stop_recording_routine()

    def handle_start_request(self):
        if self.proc is not None:
             if self.proc.poll() is None:
                 self.get_logger().warn('Start requested but recording is already in progress. Marking pending_start.')
                 self.pending_start = True
                 self.pending_stop = True # Force stop current if we re-armed? 
                 # Wait, user guideline: "Start new session". 
                 # If we re-armed while recording, we should probably stop the current one and start new, or just ignore?
                 # Prompt says: "START не должен запускать новую запись, если текущая запись ещё не остановлена."
                 # "Если пришёл ARM (armed=True), пока STOP ещё в процессе (proc жив) — поставить pending_start=True"
                 # This implies we should wait for stop to finish.
                 return

        self.start_recording()

    def handle_stop_request(self):
        # Trigger STOP logic
        # Check if ON_GROUND
        if self.last_landed_state == ExtendedState.LANDED_STATE_ON_GROUND:
            self.stop_recording_routine()
        else:
            self.get_logger().info('DISARMED but not ON_GROUND (or unknown). Setting pending_stop.')
            self.pending_stop = True

    def find_next_flight_number(self) -> int:
        if not os.path.exists(self.base_dir):
            try:
                os.makedirs(self.base_dir, exist_ok=True)
            except OSError as e:
                self.get_logger().error(f'Failed to create base dir: {e}')
                return 1 # Fallback default
        
        # Pattern: *_flightNNN
        # We need to scan directories
        subdirs = [d for d in os.listdir(self.base_dir) if os.path.isdir(os.path.join(self.base_dir, d))]
        max_flight = 0
        pattern = re.compile(r'_flight(\d{3})$')
        
        for d in subdirs:
            match = pattern.search(d)
            if match:
                try:
                    num = int(match.group(1))
                    if num > max_flight:
                        max_flight = num
                except ValueError:
                    continue
        
        return max_flight + 1

    def start_recording(self):
        flight_num = self.find_next_flight_number()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        bag_name = f'{timestamp}_flight{flight_num:03d}'
        bag_path = os.path.join(self.base_dir, bag_name)
        
        cmd = ['ros2', 'bag', 'record', '--storage', 'mcap', '-o', bag_path] + self.topics_to_record
        
        self.get_logger().info(f'Starting recording: {bag_name}')
        
        try:
            # Create process group for easier signal handling
            self.proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid
            )
            
            # Non-blocking read of stdout/stderr could be complex, for now user requested log file in bag dir.
            # But bag dir doesn't exist until process starts writing.
            # Actually user said: "Stdout/stderr писать в лог-файл внутри папки сессии".
            # ros2 bag record creates the folder immediately.
            # We can spawn a thread or just let it run and capture output later?
            # Better: Redirect process output to a file directly if possible, or use a separate thread logger.
            # Since we need to write to a file inside the bag dir, and we know the path:
            # We should wait a sec for dir creation or just create it ourselves? 
            # ros2 bag record complains if dir exists unless we assume it handles it (it typically creates it).
            # If we create it, ros2 bag record might fail saying "exists".
            # So we rely on rosbag to create it.
            
            # Start a timer to setup logging once dir exists?
            # Or just redirect to /tmp first and move?
            # Actually, `subprocess.PIPE` means we hold the pipes. We should probably create a thread to drain them to a file.
            
            # Let's start a thread to handle logging to avoid blocking main thread
            # Let's start a thread to handle logging to avoid blocking main thread
            # import threading # Moved to top
            t = threading.Thread(target=self.log_writer_thread, args=(self.proc, bag_path))
            t.daemon = True
            t.start()
            
        except Exception as e:
            self.get_logger().error(f'Failed to start ros2 bag record: {e}')
            self.proc = None

    def log_writer_thread(self, proc, bag_path):
        # Wait for directory to appear
        start_time = time.time()
        while time.time() - start_time < 5.0:
            if os.path.exists(bag_path):
                break
            time.sleep(0.1)
            
        log_file_path = os.path.join(bag_path, 'ros2_bag_record.log')
        if not os.path.exists(bag_path):
            # Fallback if bag dir validation failed or delayed
            log_file_path = os.path.join(self.base_dir, f'{os.path.basename(bag_path)}.log')

        try:
            with open(log_file_path, 'wb') as f:
                while proc.poll() is None:
                    line = proc.stdout.readline()
                    if not line:
                        break
                    f.write(line)
                    # Also flush immediately?
                    f.flush()
                # Dump remaining
                rest = proc.stdout.read()
                if rest:
                    f.write(rest)
        except Exception as e:
            self.get_logger().error(f'Log writer failed: {e}')

    def stop_recording_routine(self):
        if self.proc is None:
            self.get_logger().info('Stop requested but no process running.')
            # Check pending start
            if self.pending_start:
                self.pending_start = False
                self.start_recording()
            return
        
        self.get_logger().info('Stopping recording process...')
        self.stop_attempts = 0
        
        # Start STOP timer
        self.stop_timer = self.create_timer(self.stop_retry_interval, self.check_stop_process)

    def check_stop_process(self):
        if self.proc is None:
            self.cancel_stop_timer()
            return

        if self.proc.poll() is not None:
            # Process exited
            self.get_logger().info(f'Recording process exited with code {self.proc.returncode}')
            
            # Send notification
            msg = StatusText()
            msg.header.stamp = self.get_clock().now().to_msg()
            msg.severity = StatusText.NOTICE
            msg.text = "log recorded"
            self.pub_statustext.publish(msg)

            self.proc = None
            self.cancel_stop_timer()
            
            if self.pending_start:
                self.get_logger().info('Executing pending start...')
                self.pending_start = False
                self.start_recording()
            return

        # Still running
        self.stop_attempts += 1
        
        try:
            pgid = os.getpgid(self.proc.pid)
        except ProcessLookupError:
            # already gone
            return

        if self.stop_attempts <= self.stop_sigint_retries:
            self.get_logger().info(f'Sending SIGINT (attempt {self.stop_attempts}/{self.stop_sigint_retries})')
            os.killpg(pgid, signal.SIGINT)
        
        elif self.stop_attempts <= self.stop_sigint_retries + (self.stop_sigterm_timeout / self.stop_retry_interval):
            self.get_logger().warn('SIGINT limit reached. Sending SIGTERM.')
            os.killpg(pgid, signal.SIGTERM)
            
        elif self.use_sigkill:
             self.get_logger().error('SIGTERM timeout. Sending SIGKILL.')
             os.killpg(pgid, signal.SIGKILL)
        else:
             self.get_logger().warn('Process still not stopping, but SIGKILL is disabled (or next cycle).')

    def cancel_stop_timer(self):
        if self.stop_timer:
            self.stop_timer.cancel()
            self.stop_timer = None

    def destroy_node(self):
        # Cleanup on shutdown
        if self.proc is not None and self.proc.poll() is None:
            self.get_logger().info('Node shutdown: forcing bag stop.')
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGINT)
                self.proc.wait(timeout=2.0)
            except Exception:
                pass
        super().destroy_node()

def main(args=None):
    rclpy.init(args=args)
    node = BagSessionManager()
    
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()
