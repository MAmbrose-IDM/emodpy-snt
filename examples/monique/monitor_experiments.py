import os
import sys
import time
import subprocess
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from idmtools.core.platform_factory import Platform
from idmtools.core import ItemType

# Configuration
CURRENT_DIRECTORY = os.path.dirname(__file__)
SUITE_TRACK_FILE = os.path.join(CURRENT_DIRECTORY, 'suite_tracking.csv')
POLL_INTERVAL = 60  # seconds
# uncomment for local analyzer script:
#ANALYZER_SCRIPT = os.path.join(CURRENT_DIRECTORY, 'analyzers', 'post_analysis.py')
# Uncomment for SSMT
SSMT_SCRIPT = os.path.join(CURRENT_DIRECTORY, 'analyzers', 'post_ssmt.py')

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def load_suite_tracking():
    try:
        return pd.read_csv(SUITE_TRACK_FILE)
    except Exception as e:
        logging.error(f"Failed to read {SUITE_TRACK_FILE}: {e}")
        print(f"Failed to read {SUITE_TRACK_FILE}: {e}")
        return pd.DataFrame()

def save_suite_tracking(df):
    try:
        df.to_csv(SUITE_TRACK_FILE, index=False)
    except Exception as e:
        logging.error(f"Failed to write to {SUITE_TRACK_FILE}: {e}")
        print(f"Failed to write to {SUITE_TRACK_FILE}: {e}")


def check_experiment_and_trigger(platform, exp_id, info, df):
    try:
        exp = platform.get_item(exp_id, ItemType.EXPERIMENT)
        sims = exp.simulations
        status = exp.status.name
        succeeded = sum(1 for s in sims if s.status.name == 'SUCCEEDED')
        total = len(sims)

        logging.info(f"[{exp.name}] {exp.id}: {status} - {succeeded}/{total} SUCCEEDED")
        print(f"[{exp.name}] {exp.id}: {status} - {succeeded}/{total} SUCCEEDED")

        match = (df['experiment_id'] == exp_id) & (df['suite_id'] == exp.parent.id)

        # Ensure 'analyzer_run_status' exists
        if 'analyzer_run_status' not in df.columns:
            df['analyzer_run_status'] = pd.Series([None] * len(df), dtype='object')
        else:
            df['analyzer_run_status'] = df['analyzer_run_status'].astype('object')

        # Check if analyzer should be triggered
        if succeeded == total and df.loc[match, 'analyzer_run_status'].isnull().all():
            logging.info(f"All simulations succeeded for {exp.name}. Launching analyzer...")
            print(f"All simulations succeeded for {exp.name}. Launching analyzer...")

            # subprocess.Popen([sys.executable, ANALYZER_SCRIPT,
            #                   '--exp-id', exp_id,
            #                   '--type', info['type'],
            #                   '--name', info['name']])
            # Optional SSMT analyzer:
            subprocess.run([sys.executable, SSMT_SCRIPT, '--exp-id', exp_id, '--type', info['type'], '--name', info['name']])

            df.loc[match, 'analyzer_run_status'] = 'triggered'
            return df.loc[match]  # return experiment row
        else:
            logging.info(f"Analyzer already triggered or not ready for: {exp.name}: {exp_id}")
            print(f"Analyzer already triggered or not ready for: {exp.name}: {exp_id}")
            return None

    except Exception as e:
        logging.error(f"Error checking experiment {exp_id}: {e}")
        print(f"Error checking experiment {exp_id}: {e}")
        return None

def main():
    logging.info("Starting experiment monitor...")
    print("Starting experiment monitor...")

    platform = Platform('CALCULON')

    while True:
        df = load_suite_tracking()

        if df.empty:
            logging.warning("No experiments found in tracking file.")
            print("No experiments found in tracking file.")
            return

        # Build dictionary once
        exp_id_dict = {
            row['experiment_id']: {
                'type': row.get('experiment_type', ''),
                'name': row.get('experiment_name', '')
            }
            for _, row in df.iterrows()
        }

        # break if all experiments have been processed
        if df['analyzer_run_status'].notnull().all():
            logging.info("All experiments processed. Monitor exiting.")
            #print("All experiments processed. Monitor exiting.")
            break

        updated_rows = []
        with ThreadPoolExecutor(max_workers=2) as executor:  # you can adjust max_workers number as needed
            future_to_exp = {
                executor.submit(check_experiment_and_trigger, platform, exp_id, info, df.copy()): exp_id
                for exp_id, info in exp_id_dict.items()
            }

            for future in as_completed(future_to_exp):
                try:
                    result = future.result()
                    if result is not None:
                        updated_rows.append(result)
                except Exception as e:
                    logging.error(f"Error in thread for experiment {future_to_exp[future]}: {e}")

        if updated_rows:
            updates_df = pd.concat(updated_rows)
            df['analyzer_run_status'] = df['analyzer_run_status'].astype('object')
            for idx, row in updates_df.iterrows():
                match = (df['experiment_id'] == row['experiment_id']) & (df['suite_id'] == row['suite_id'])
                df.loc[match, 'analyzer_run_status'] = row['analyzer_run_status']
            save_suite_tracking(df)

        print(f"Sleeping {POLL_INTERVAL} seconds...\n")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
