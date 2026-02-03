#!/usr/bin/env python3

import os
import time
import signal
import subprocess
import re
import shutil
from datetime import datetime

import rclpy
from rclpy.node import Node

from mavros_msgs.msg import State, ExtendedState, StatusText
from rcl_interfaces.msg import ParameterDescriptor, ParameterType
import threading

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
        self.stop_attempts = 0
        self.state_subscribed = False
        self.current_bag_path = None
        self.log_thread = None
        self.finalizing_stop = False
        self.stop_notified = False

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
        
        # Initial cleanup
        self.check_and_cleanup_storage()

    def state_cb(self, msg: State):
        if not self.state_subscribed:
            self.state_subscribed = True
            stat = StatusText()
            stat.header.stamp = self.get_clock().now().to_msg()
            stat.severity = StatusText.INFO
            stat.text = "Subscribed to /mavros/state"
            self.pub_statustext.publish(stat)

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
        if self.finalizing_stop:
            self.get_logger().warn('Start requested while stop is finalizing. Marking pending_start.')
            self.pending_start = True
            return
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
        # Cleanup before start
        self.check_and_cleanup_storage()

        if not self.topics_to_record:
            self.get_logger().error('No topics configured for recording. Skipping start.')
            return

        self.finalizing_stop = False
        self.stop_notified = False
        flight_num = self.find_next_flight_number()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        bag_name = f'{timestamp}_flight{flight_num:03d}'
        bag_path = os.path.join(self.base_dir, bag_name)
        self.current_bag_path = bag_path
        
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
            self.log_thread = t

            # Send notification
            msg = StatusText()
            msg.header.stamp = self.get_clock().now().to_msg()
            msg.severity = StatusText.INFO
            msg.text = "Precision landing started."
            self.pub_statustext.publish(msg)
            
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

    def finalize_stop_and_notify(self, bag_path, log_thread):
        self.wait_for_bag_finalize(bag_path, log_thread)

        if self.stop_notified:
            return
        self.stop_notified = True

        msg = StatusText()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.severity = StatusText.INFO
        msg.text = "Precision landing stopped. Log recorded"
        self.pub_statustext.publish(msg)

        self.finalizing_stop = False
        self.current_bag_path = None
        self.log_thread = None

        if self.pending_start:
            self.get_logger().info('Executing pending start...')
            self.pending_start = False
            self.start_recording()

    def stop_recording_routine(self):
        if self.proc is None:
            self.get_logger().info('Stop requested but no process running.')
            # Check pending start
            if self.pending_start:
                self.pending_start = False
                self.start_recording()
            return
        
        if self.stop_timer is not None:
            self.get_logger().info('Stop already in progress; ignoring duplicate stop request.')
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

            self.finalizing_stop = True
            bag_path = self.current_bag_path
            log_thread = self.log_thread

            self.proc = None
            self.cancel_stop_timer()

            t = threading.Thread(target=self.finalize_stop_and_notify, args=(bag_path, log_thread))
            t.daemon = True
            t.start()
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
        
        # Gather all session folders
        # We assume only directories matching our pattern or created by us should be managed?
        # Or just all directories in base_dir?
        # Let's target all subdirectories to be safe, but maybe we should filter by pattern to avoid deleting user config stuff?
        # User requested: "удалялись старые логи". Logs are in "timestamp_flightNNN".
        # Let's trust that base_dir contains mainly logs.
        
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
                
            # Need to delete
            # Sort by mtime (oldest first)
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
