import os
import sys
import time
import subprocess
import pandas as pd
from idmtools.core.platform_factory import Platform
from idmtools.core import ItemType
from idmtools.entities.experiment import Experiment

CURRENT_DIRECTORY = os.path.dirname(__file__)
suite_track_file = 'suite_tracking_new.csv'
poll_interval = 60  # seconds

# Initialize platform
platform = Platform('CALCULON')

# Load experiment tracking file
df = pd.read_csv(os.path.join(CURRENT_DIRECTORY, suite_track_file))
exp_id_dict = {
    row['experiment_id']: {
        'type': row.get('experiment_type', ''),
        'name': row.get('experiment_name', '')
    }
    for _, row in df.iterrows()
}

# Track which experiments we've already responded to
processed_experiments = set()

def check_experiment_and_trigger(exp_id, exp_type, exp_name):
    try:
        exp = platform.get_item(exp_id, ItemType.EXPERIMENT)
        sims = exp.simulations
        status = exp.status.name
        succeeded = sum(1 for s in sims if s.status.name.lower() == 'succeeded')
        total = len(sims)

        print(f"[{exp.name}] {exp.id}: {status}")
        print(f"  Total Sims: {total} | Succeeded: {succeeded} | Finished: {succeeded}/{total}")

        # If all sims succeeded and we haven't kicked off follow-up script yet
        if succeeded == total and exp_id not in processed_experiments:
            print(f"✅ All simulations succeeded for {exp.name} ({exp_id}). Kicking off follow-up script...")

            # Call the follow-up script here (edit as needed)
            script_path = os.path.join(CURRENT_DIRECTORY, 'analyzers', 'post_analysis.py')
            subprocess.run([sys.executable, script_path, '--exp-id', exp_id, '--type', exp_type, '--name', exp_name])

            # if run ssmt, uncomment following 2 lines
            # ssmt_script_path = os.path.join(CURRENT_DIRECTORY, 'analyzers', 'post_ssmt.py')
            # subprocess.run([sys.executable, ssmt_script_path, '--exp-id', exp_id, '--type', exp_type, '--name', exp_name])

            processed_experiments.add(exp_id)

    except Exception as e:
        print(f"❌ Error checking experiment {exp_id}: {e}")

if __name__ == "__main__":
    print("📡 Starting experiment monitor...\n")
    for exp_id, info in exp_id_dict.items():
        exp_type = info['type']
        exp_name = info['name']
        check_experiment_and_trigger(exp_id, exp_type, exp_name)
    print(f"⏳ Sleeping {poll_interval} seconds...\n")
    time.sleep(poll_interval)
