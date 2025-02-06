import asyncio
import time
import psutil
import os
import sys
import traceback
from datetime import datetime, timedelta
import json
import glob
from fiber.logging_utils import get_logger

logger = get_logger(__name__)

#This monitor is designed to detect if the validator is frozen by checking if the log file has been updated recently.
#If the log file has not been updated recently, it will dump the process state and create a detailed dump of the process state.
#The dump will include the last 50 lines of the log file, the python stack trace, the open files, and the network connections.

class ValidatorMonitor:
    def __init__(self):
        self.validator_pid = None
        self.dump_dir = "/var/log/gaia/freezes"
        self.log_dir = "/root/.pm2/logs"
        self.log_prefix = "gaia-validator-out-0"
        self.freeze_threshold = 300
        self.resource_thresholds = {
            'open_files': 1000,  # Max number of open files
            'connections': 100,  # Max number of network connections
            'memory_mb': 1024,   # Max memory usage in MB
            'cpu_percent': 80    # Max CPU usage percentage
        }
        os.makedirs(self.dump_dir, exist_ok=True)
        logger.info("Initialized ValidatorMonitor with freeze threshold: %d seconds", self.freeze_threshold)

    def get_validator_pid(self):
        """Get validator PID from PM2"""
        try:
            pm2_output = os.popen('pm2 jlist').read()
            processes = json.loads(pm2_output)
            for proc in processes:
                if proc['name'] == 'gaia_validator':
                    logger.info("Found validator process with PID: %d", proc['pid'])
                    return proc['pid']
            logger.warning("No validator process found in PM2 list")
        except Exception as e:
            logger.error("Error getting PID: %s", str(e))
        return None

    def get_latest_log_file(self):
        """Get the most recent log file"""
        try:
            log_file = f"{self.log_dir}/{self.log_prefix}.log"
            if not os.path.exists(log_file):
                logger.warning("No log files found in %s", self.log_dir)
                return None
            
            logger.info("Using log file: %s", log_file)
            return log_file
            
        except Exception as e:
            logger.error("Error finding latest log: %s", str(e))
            return None

    def check_log_activity(self):
        """Check if logs have been updated recently"""
        try:
            latest_log = self.get_latest_log_file()
            if not latest_log:
                return False

            last_modified = os.path.getmtime(latest_log)
            time_since_update = time.time() - last_modified

            logger.info("Time since last log update: %.1f seconds", time_since_update)

            if time_since_update > self.freeze_threshold:
                logger.warning("Potential freeze detected - no log updates for %.1f seconds", time_since_update)
                return True

            return False

        except Exception as e:
            logger.error("Error checking log activity: %s", str(e))
            return False

    def _get_stack_trace(self, pid):
        """Get stack trace using GDB command line"""
        try:
            gdb_cmd = f"""gdb -p {pid} -batch -ex "thread apply all bt" 2>/dev/null"""
            return os.popen(gdb_cmd).read()
        except Exception as e:
            logger.error("Error getting stack trace: %s", str(e))
            return f"Error getting stack trace: {e}"

    def dump_process_state(self):
        """Create detailed dump of process state"""
        if not self.validator_pid:
            logger.warning("Cannot dump process state - no validator PID")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        dump_file = f"{self.dump_dir}/freeze_{timestamp}.txt"
        
        try:
            process = psutil.Process(self.validator_pid)
            latest_log = self.get_latest_log_file()
            
            logger.info("Creating process state dump at: %s", dump_file)
            
            with open(dump_file, 'w') as f:
                f.write(f"=== Validator Freeze Detected ===\n")
                f.write(f"Timestamp: {timestamp}\n")
                f.write(f"Process ID: {self.validator_pid}\n")
                f.write(f"Log File: {latest_log}\n")
                f.write(f"Memory: {process.memory_info().rss / 1024 / 1024:.2f} MB\n\n")
                
                f.write("=== Last Log Entries ===\n")
                if latest_log:
                    try:
                        with open(latest_log, 'r') as log_file:
                            last_lines = log_file.readlines()[-50:]
                            f.write(''.join(last_lines))
                    except Exception as e:
                        error_msg = f"Error reading logs: {e}"
                        logger.error(error_msg)
                        f.write(f"{error_msg}\n")
                else:
                    f.write("No log file found\n")

                f.write("\n=== Python Stack Traces ===\n")
                stack_trace = self._get_stack_trace(self.validator_pid)
                f.write(stack_trace)

                f.write("\n=== Open Files ===\n")
                for file in process.open_files():
                    f.write(f"{file.path}\n")

                f.write("\n=== Network Connections ===\n")
                for conn in process.connections():
                    f.write(f"{conn}\n")

            logger.info("Freeze dump created successfully at: %s", dump_file)
            return dump_file
             
        except Exception as e:
            logger.error("Error creating dump: %s", str(e))

    def restart_validator(self):
        """Restart the validator process using PM2"""
        try:
            logger.warning("Attempting to restart validator process")
            os.system('pm2 restart validator')
            logger.info("Validator restart command issued")
            return True
        except Exception as e:
            logger.error("Error restarting validator: %s", str(e))
            return False

    def check_resource_usage(self):
        """Check for potential resource leaks and high usage."""
        try:
            if not self.validator_pid:
                return False

            process = psutil.Process(self.validator_pid)
            
            # Check open files
            open_files = process.open_files()
            if len(open_files) > self.resource_thresholds['open_files']:
                logger.warning(f"High number of open files: {len(open_files)}")
                self._log_resource_warning('open_files', open_files)
                
            # Check network connections
            connections = process.connections()
            if len(connections) > self.resource_thresholds['connections']:
                logger.warning(f"High number of network connections: {len(connections)}")
                self._log_resource_warning('connections', connections)
                
            # Check memory usage
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            if memory_mb > self.resource_thresholds['memory_mb']:
                logger.warning(f"High memory usage: {memory_mb:.2f} MB")
                self._log_resource_warning('memory', memory_info)
                
            # Check CPU usage
            cpu_percent = process.cpu_percent()
            if cpu_percent > self.resource_thresholds['cpu_percent']:
                logger.warning(f"High CPU usage: {cpu_percent}%")
                self._log_resource_warning('cpu', cpu_percent)

            return True

        except Exception as e:
            logger.error(f"Error checking resource usage: {e}")
            return False

    def _log_resource_warning(self, resource_type, details):
        """Log detailed resource warning information."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        warning_file = f"{self.dump_dir}/resource_warning_{resource_type}_{timestamp}.txt"
        
        try:
            with open(warning_file, 'w') as f:
                f.write(f"=== Resource Warning: {resource_type} ===\n")
                f.write(f"Timestamp: {timestamp}\n")
                f.write(f"Process ID: {self.validator_pid}\n\n")
                
                if resource_type == 'open_files':
                    f.write("Open Files:\n")
                    for file in details:
                        f.write(f"  {file.path}\n")
                        
                elif resource_type == 'connections':
                    f.write("Network Connections:\n")
                    for conn in details:
                        f.write(f"  {conn}\n")
                        
                elif resource_type == 'memory':
                    f.write("Memory Information:\n")
                    for key, value in details._asdict().items():
                        f.write(f"  {key}: {value}\n")
                        
                elif resource_type == 'cpu':
                    f.write(f"CPU Usage: {details}%\n")
                
            logger.info(f"Resource warning logged to {warning_file}")
            
        except Exception as e:
            logger.error(f"Error logging resource warning: {e}")

    def run(self):
        logger.info("Starting ValidatorMonitor")
        while True:
            try:
                self.validator_pid = self.get_validator_pid()
                if not self.validator_pid:
                    logger.warning("Validator not running")
                    time.sleep(5)
                    continue

                if self.check_log_activity():
                    logger.warning("Detected validator freeze, creating process dump")
                    self.dump_process_state()
                    logger.warning("Restarting frozen validator process")
                    self.restart_validator()
                
                # Check resource usage
                self.check_resource_usage()
                
                time.sleep(10)

            except Exception as e:
                logger.error("Monitor error: %s", str(e))
                time.sleep(5)

if __name__ == "__main__":
    monitor = ValidatorMonitor()
    monitor.run() 