import asyncio
import time
import psutil
import os
import sys
import traceback
from datetime import datetime, timedelta
import json
import glob

#This monitor is designed to detect if the validator is frozen by checking if the log file has been updated recently.
#If the log file has not been updated recently, it will dump the process state and create a detailed dump of the process state.
#The dump will include the last 50 lines of the log file, the python stack trace, the open files, and the network connections.

class ValidatorMonitor:
    def __init__(self):
        self.validator_pid = None
        self.dump_dir = "/var/log/gaia/freezes"
        self.log_dir = "/root/.pm2/logs"
        self.log_prefix = "vali-out"
        self.freeze_threshold = 300
        os.makedirs(self.dump_dir, exist_ok=True)

    def get_validator_pid(self):
        """Get validator PID from PM2"""
        try:
            pm2_output = os.popen('pm2 jlist').read()
            processes = json.loads(pm2_output)
            for proc in processes:
                if proc['name'] == 'vali':
                    return proc['pid']
        except Exception as e:
            print(f"Error getting PID: {e}")
        return None

    def get_latest_log_file(self):
        """Get the most recent log file"""
        try:
            log_files = glob.glob(f"{self.log_dir}/{self.log_prefix}-*.log")
            if not log_files:
                print("No log files found")
                return None
            
            latest_log = max(log_files, key=os.path.getmtime)
            print(f"Using log file: {latest_log}")
            return latest_log
            
        except Exception as e:
            print(f"Error finding latest log: {e}")
            return None

    def check_log_activity(self):
        """Check if logs have been updated recently"""
        try:
            latest_log = self.get_latest_log_file()
            if not latest_log:
                return False

            last_modified = os.path.getmtime(latest_log)
            time_since_update = time.time() - last_modified

            print(f"Time since last log update: {time_since_update:.1f} seconds")

            if time_since_update > self.freeze_threshold:
                print(f"Potential freeze detected - no log updates for {time_since_update:.1f} seconds")
                return True

            return False

        except Exception as e:
            print(f"Error checking log activity: {e}")
            return False

    def _get_stack_trace(self, pid):
        """Get stack trace using GDB command line"""
        try:
            gdb_cmd = f"""gdb -p {pid} -batch -ex "thread apply all bt" 2>/dev/null"""
            return os.popen(gdb_cmd).read()
        except Exception as e:
            return f"Error getting stack trace: {e}"

    def dump_process_state(self):
        """Create detailed dump of process state"""
        if not self.validator_pid:
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        dump_file = f"{self.dump_dir}/freeze_{timestamp}.txt"
        
        try:
            process = psutil.Process(self.validator_pid)
            latest_log = self.get_latest_log_file()
            
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
                        f.write(f"Error reading logs: {e}\n")
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

            print(f"Freeze dump created: {dump_file}")
            return dump_file

        except Exception as e:
            print(f"Error creating dump: {e}")

    def run(self):
        while True:
            try:
                self.validator_pid = self.get_validator_pid()
                if not self.validator_pid:
                    print("Validator not running")
                    time.sleep(5)
                    continue

                if self.check_log_activity():
                    self.dump_process_state()
                
                time.sleep(10)

            except Exception as e:
                print(f"Monitor error: {e}")
                time.sleep(5)

if __name__ == "__main__":
    monitor = ValidatorMonitor()
    monitor.run() 