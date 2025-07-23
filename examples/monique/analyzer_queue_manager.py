# Another way to run analyzer separately from run_simulation.py with centralized queue manager, so you can:
# Control how many analyzers run at once
# Prevent memory/CPU exhaustion
# Cleanly separate experiment submission from analysis
import os
import time
import subprocess
import sys

QUEUE_DIR = "analyzer_queue"
LOG_DIR = "logs"
MAX_PARALLEL_ANALYZERS = 2
#ANALYZER_SCRIPT = "analyzers/post_ssmt.py"
ANALYZER_SCRIPT = "analyzers/post_analysis.py"

running = {}

def clean_flag(flag_path):
    """
    rename file extension from .ready to .done
    Args:
        flag_path:

    Returns:

    """
    done_path = flag_path.replace(".ready", ".done")
    if os.path.exists(flag_path):
        try:
            os.rename(flag_path, done_path)
        except FileNotFoundError:
            print(f"File already handled: {flag_path}")
    else:
        print(f"Flag file not found when cleaning: {flag_path}")

def launch_analyzer(exp_id, exp_name, exp_type):
    """
    lunch analyzer for experiment with subprocess in background
    Args:
        exp_id: experiment id, str
        exp_name: experiment name, str
        exp_type: experiment type, str

    Returns:

    """
    if "ssmt" in ANALYZER_SCRIPT:
        log_path = os.path.join(LOG_DIR, f"ssmt_{exp_name}_{exp_id}.log")
    else:
        log_path = os.path.join(LOG_DIR, f"local_{exp_name}_{exp_id}.log")
    print(f"Running analyzer for {exp_id}")
    print(f"Log for analyzer with {exp_id} at {str(log_path)}")
    log_file = open(log_path, "w", encoding="utf-8")
    os.environ["NO_COLOR"] = "1"  # disable color in subprocess's log
    proc = subprocess.Popen(
        [
            sys.executable, ANALYZER_SCRIPT,
            "--exp-id", exp_id,
            "--type", exp_type,
            "--name", exp_name
        ],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True
    )
    return proc, log_file

def main():
    """
    Consistently run analyzers until interrupted.
    Only pick files from the queue directory with .ready extension.
    Returns:

    """
    os.makedirs(QUEUE_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

    # run consistently until interrupted
    while True:
        # Check for new .ready files
        ready_files = [f for f in os.listdir(QUEUE_DIR) if f.endswith(".ready")]
        for fname in ready_files:
            if len(running) >= MAX_PARALLEL_ANALYZERS:
                break

            path = os.path.join(QUEUE_DIR, fname)
            with open(path) as f:
                line = f.read().strip()
                exp_id, exp_name, exp_type = line.split(',')

            proc, log_file = launch_analyzer(exp_id, exp_name, exp_type)
            running[proc.pid] = (proc, log_file, path)

        # Check running processes to see if any analyzers have completed
        finished = []
        for pid, (proc, log_file, flag_path) in running.items():
            # If the process has finished  (not None)
            if proc.poll() is not None:
                log_file.close() # Close the log file
                clean_flag(flag_path)  # Rename .ready → .done
                print(f"Analyzer for {flag_path} finished.")
                finished.append(pid)  # Mark this PID for removal from tracking

        for pid in finished:
            del running[pid]

        time.sleep(5)

if __name__ == "__main__":
    print("Analyzer manager running...")
    main()
