#!/usr/bin/env python3

import os
import time
import signal
import subprocess
import re
import shutil
from datetime import datetime
import threading

import rclpy
from rclpy.node import Node

from mavros_msgs.msg import State, ExtendedState, StatusText
from rcl_interfaces.msg import ParameterDescriptor, ParameterType

LOG_FILENAME = 'ros2_bag_record.log'
LOG_STOP_MARKER = 'Recording stopped'

class BagSessionManager(Node):
    def __init__(self):
        super().__init__('bag_session_manager')

        # Parameters
        self.declare_parameter('base_dir', '/home/orangepi/rosbags')
        self.declare_parameter('stop_retry_interval_sec', 0.5)
        self.declare_parameter('stop_sigint_retries', 20)
        self.declare_parameter('stop_sigterm_timeout_sec', 5.0)
        self.declare_parameter('use_sigkill', False)
        self.declare_parameter('max_storage_size_gb', 15.0)
        
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
        self.max_storage_size_gb = self.get_parameter('max_storage_size_gb').get_parameter_value().double_value
        raw_topics = self.get_parameter('topics_to_record').get_parameter_value().string_array_value
        self.topics_to_record = [t for t in raw_topics if t]

        # State variables
        self.proc = None
        self.last_armed = None
        self.last_landed_state = None
        self.pending_stop = False
        self.pending_start = False
        self.stop_timer = None
        self.stop_delay_timer = None
        self.stop_attempts = 0
        self.state_subscribed = False
        self.current_bag_path = None
        self.current_flight_num = None
        self.log_thread = None
        self.finalizing_stop = False

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
        
        self.check_and_cleanup_storage()

    def publish_statustext(self, text, severity=StatusText.INFO):
        msg = StatusText()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.severity = severity
        msg.text = text
        self.pub_statustext.publish(msg)

    def get_log_paths(self, bag_path):
        bag_log = os.path.join(bag_path, LOG_FILENAME)
        fallback_log = os.path.join(self.base_dir, f'{os.path.basename(bag_path)}.log')
        return [bag_log, fallback_log]

    def extract_flight_number(self, bag_path):
        if not bag_path:
            return None
        match = re.search(r'_flight(\d{3})', os.path.basename(bag_path))
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    def state_cb(self, msg: State):
        if not self.state_subscribed:
            self.state_subscribed = True
            self.publish_statustext('Subscribed to /mavros/state')

        current_armed = msg.armed
        
        if self.last_armed is not None and not self.last_armed and current_armed:
            self.get_logger().info('Detected ARM transition. Requesting START.')
            self.handle_start_request()

        if self.last_armed is not None and self.last_armed and not current_armed:
            self.get_logger().info('Detected DISARM transition. Checking landed state for STOP.')
            self.handle_stop_request()

        self.last_armed = current_armed

    def extended_state_cb(self, msg: ExtendedState):
        self.last_landed_state = msg.landed_state
        
        if self.pending_stop:
            if self.last_landed_state == ExtendedState.LANDED_STATE_ON_GROUND:
                self.get_logger().info('Landed state confirmed ON_GROUND. Executing STOP.')
                self.pending_stop = False
                self.stop_recording_routine()

    def handle_start_request(self):
        if self.finalizing_stop:
            self.get_logger().warn('Start requested while stop is finalizing. Marking pending_start.')
            self.pending_start = True
            return
        if self.proc is not None:
            if self.proc.poll() is None:
                self.get_logger().warn('Start requested but recording is already in progress. Marking pending_start.')
                self.pending_start = True
                self.pending_stop = True
                return

        self.start_recording()

    def handle_stop_request(self):
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
                return 1
        
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
        self.check_and_cleanup_storage()

        if not self.topics_to_record:
            self.get_logger().error('No topics configured for recording. Skipping start.')
            return

        self.finalizing_stop = False
        flight_num = self.find_next_flight_number()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        bag_name = f'{timestamp}_flight{flight_num:03d}'
        bag_path = os.path.join(self.base_dir, bag_name)
        self.current_bag_path = bag_path
        self.current_flight_num = flight_num
        
        cmd = ['ros2', 'bag', 'record', '--storage', 'mcap', '-o', bag_path] + self.topics_to_record
        
        self.get_logger().info(
            f'Starting recording: {bag_name} (flight_num={flight_num:03d}, bag_path={bag_path})'
        )
        
        try:
            self.proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid
            )
            
            t = threading.Thread(target=self.log_writer_thread, args=(self.proc, bag_path))
            t.daemon = True
            t.start()
            self.log_thread = t

            self.publish_statustext(f'Precision landing started. Flight {flight_num:03d}.')
            
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

        log_paths = self.get_log_paths(bag_path)
        log_file_path = log_paths[0] if os.path.exists(bag_path) else log_paths[1]

        try:
            with open(log_file_path, 'wb') as f:
                while proc.poll() is None:
                    line = proc.stdout.readline()
                    if not line:
                        break
                    f.write(line)
                    f.flush()
                rest = proc.stdout.read()
                if rest:
                    f.write(rest)
        except Exception as e:
            self.get_logger().error(f'Log writer failed: {e}')

    def get_latest_mtime(self, path):
        latest = None
        try:
            for root, _, files in os.walk(path):
                for name in files:
                    try:
                        mtime = os.path.getmtime(os.path.join(root, name))
                        if latest is None or mtime > latest:
                            latest = mtime
                    except OSError:
                        continue
        except Exception:
            return None
        return latest

    def wait_for_bag_finalize(self, bag_path, log_thread):
        if log_thread is not None and log_thread.is_alive():
            log_thread.join(timeout=2.0)

        if not bag_path or not os.path.exists(bag_path):
            return

        end_time = time.time() + 5.0
        last_mtime = None
        stable_since = None

        while time.time() < end_time:
            latest_mtime = self.get_latest_mtime(bag_path)
            if latest_mtime is None:
                break

            if last_mtime is not None and latest_mtime == last_mtime:
                if stable_since is None:
                    stable_since = time.time()
                elif time.time() - stable_since >= 0.5:
                    break
            else:
                stable_since = None
                last_mtime = latest_mtime

            time.sleep(0.1)

    def wait_for_log_line(self, bag_path, needle, timeout_sec=None):
        if not bag_path:
            return False

        log_paths = self.get_log_paths(bag_path)

        found_log = False
        end_time = None if timeout_sec is None else time.time() + timeout_sec
        positions = {path: 0 for path in log_paths}

        while end_time is None or time.time() < end_time:
            for path in log_paths:
                if not os.path.exists(path):
                    continue
                try:
                    with open(path, 'r', errors='ignore') as f:
                        f.seek(0, os.SEEK_END)
                        end_pos = f.tell()
                        last_pos = positions.get(path, 0)
                        if last_pos > end_pos:
                            last_pos = 0
                        f.seek(last_pos)
                        data = f.read()
                        positions[path] = f.tell()
                        if needle in data:
                            found_log = True
                            break
                except OSError:
                    continue

            if found_log:
                break

            time.sleep(0.1)

        return found_log

    def notify_when_log_stopped(self, bag_path, log_thread, flight_num):
        self.wait_for_bag_finalize(bag_path, log_thread)

        self.wait_for_log_line(
            bag_path,
            LOG_STOP_MARKER
        )

        if flight_num is not None:
            text = f'Precision landing stopped. Log recorded. Flight {flight_num:03d}.'
        else:
            text = 'Precision landing stopped. Log recorded.'
        self.publish_statustext(text)

    def stop_recording_routine(self):
        if self.proc is None:
            self.get_logger().info('Stop requested but no process running.')
            if self.pending_start:
                self.pending_start = False
                self.start_recording()
            return
        
        if self.stop_timer is not None or self.stop_delay_timer is not None:
            self.get_logger().info('Stop already in progress; ignoring duplicate stop request.')
            return

        self.get_logger().info(
            'Stopping recording process in 1.0s... '
            f'(current_flight_num={self.current_flight_num}, bag_path={self.current_bag_path})'
        )
        self.stop_attempts = 0
        self.finalizing_stop = True
        self.stop_delay_timer = self.create_timer(1.0, self.begin_stop_process)

    def begin_stop_process(self):
        if self.stop_delay_timer:
            self.stop_delay_timer.cancel()
            self.stop_delay_timer = None

        if self.proc is None:
            self.cancel_stop_timer()
            return

        self.stop_timer = self.create_timer(self.stop_retry_interval, self.check_stop_process)

    def check_stop_process(self):
        if self.proc is None:
            self.cancel_stop_timer()
            return

        if self.proc.poll() is not None:
            self.get_logger().info(f'Recording process exited with code {self.proc.returncode}')

            bag_path = self.current_bag_path
            log_thread = self.log_thread
            flight_num = self.current_flight_num
            if not flight_num:
                flight_num = self.extract_flight_number(bag_path)
            self.get_logger().info(
                f'Finalizing stop (flight_num={flight_num}, bag_path={bag_path})'
            )

            self.proc = None
            self.current_bag_path = None
            self.log_thread = None
            self.current_flight_num = None
            self.finalizing_stop = False
            self.cancel_stop_timer()

            t = threading.Thread(target=self.notify_when_log_stopped, args=(bag_path, log_thread, flight_num))
            t.daemon = True
            t.start()

            if self.pending_start:
                self.get_logger().info('Executing pending start...')
                self.pending_start = False
                self.start_recording()
            return

        self.stop_attempts += 1
        
        try:
            pgid = os.getpgid(self.proc.pid)
        except ProcessLookupError:
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
        if self.stop_delay_timer:
            self.stop_delay_timer.cancel()
            self.stop_delay_timer = None

    def get_dir_size(self, path):
        total = 0
        try:
            for entry in os.scandir(path):
                if entry.is_file():
                    total += entry.stat().st_size
                elif entry.is_dir():
                    total += self.get_dir_size(entry.path)
        except Exception:
            pass
        return total

    def check_and_cleanup_storage(self):
        if not os.path.exists(self.base_dir):
            return

        limit_bytes = self.max_storage_size_gb * 1024 * 1024 * 1024
        
        try:
            items = []
            total_size = 0
            
            for d in os.listdir(self.base_dir):
                path = os.path.join(self.base_dir, d)
                if os.path.isdir(path):
                    size = self.get_dir_size(path)
                    mtime = os.path.getmtime(path)
                    items.append({'path': path, 'size': size, 'mtime': mtime, 'name': d})
                    total_size += size
            
            self.get_logger().info(f'Current storage usage: {total_size / (1024**3):.2f} GB / {self.max_storage_size_gb:.2f} GB')
            
            if total_size <= limit_bytes:
                return
                
            items.sort(key=lambda x: x['mtime'])
            
            deleted_count = 0
            for item in items:
                if total_size <= limit_bytes:
                    break
                
                self.get_logger().warn(f'Storage limit exceeded. Deleting old session: {item["name"]} ({item["size"] / (1024**2):.1f} MB)')
                try:
                    shutil.rmtree(item['path'])
                    total_size -= item['size']
                    deleted_count += 1
                except Exception as e:
                    self.get_logger().error(f'Failed to delete {item["name"]}: {e}')
                    
            if deleted_count > 0:
                self.get_logger().info(f'Cleanup complete. Deleted {deleted_count} old sessions.')
                
        except Exception as e:
            self.get_logger().error(f'Error during storage cleanup: {e}')

    def destroy_node(self):
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
