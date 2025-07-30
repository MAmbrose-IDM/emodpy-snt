import os
import pandas as pd
from filelock import FileLock
from datetime import datetime

def update_suite_tracking(experiment, suite, experiment_type, tracking_file, time_stamp):
    """
    Update suite tracking file.
    Args:
        experiment:
        suite:
        experiment_type:
        tracking_file:
        time_stamp:

    Returns:

    """
    lock_path = tracking_file + ".lock"
    with FileLock(lock_path, timeout=30):
        # Ensure the file exists with headers
        if not os.path.exists(tracking_file):
            with open(tracking_file, "w") as f:
                f.write("suite_name,suite_id,experiment_name,experiment_id,experiment_type,start_time_stamp,end_time_stamp\n")

        df = pd.read_csv(tracking_file)

        match = df['suite_id'] == str(suite.id)

        if match.any():
            # Update existing row(s)
            df.loc[match, 'experiment_name'] = experiment.name
            df.loc[match, 'experiment_id'] = experiment.id
            df.loc[match, 'end_time_stamp'] = time_stamp
        else:
            # Add new row if suite_id not found
            new_row = {
                'suite_name': suite.name,
                'suite_id': suite.id,
                'experiment_name': experiment.name,
                'experiment_id': experiment.id,
                'experiment_type': experiment_type,
                'start_time_stamp': time_stamp,
                'end_time_stamp': None
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        # Save changes
        df.to_csv(tracking_file, index=False)


def update_scenario_status_to_done(scenario_fname, scen_index):
    """
    Update scenario status to 'done' in scenario_fname. This is set to 'done' when the experiment is done.
    Args:
        scenario_fname: scenario file path
        scen_index: scenario index
    """
    lockfile = scenario_fname + ".lock"
    with FileLock(lockfile, timeout=30):
        df = pd.read_csv(scenario_fname)
        df.loc[scen_index, 'status'] = 'done'
        df.to_csv(scenario_fname, index=False)
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]}: Updated status in scenario index {scen_index} to 'done' in {scenario_fname}")
