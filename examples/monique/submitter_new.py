import logging
import os
import sys
import subprocess
import pandas as pd
from idmtools.core.platform_factory import Platform
from idmtools.entities import Suite
from idmtools.core import ItemType
from snt.load_paths import load_box_paths

# Configuration
USER_PATH = None
CURRENT_DIRECTORY = os.path.dirname(__file__)
SUITE_TRACK_FILE = os.path.join(CURRENT_DIRECTORY, 'suite_tracking.csv')
FUTURE_PROJECTIONS = False
ANALYZER_STATUS = None
# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def ensure_tracking_file():
    if not os.path.exists(SUITE_TRACK_FILE) or os.path.getsize(SUITE_TRACK_FILE) == 0:
        with open(SUITE_TRACK_FILE, 'w') as f:
            f.write("suite_name,suite_id,experiment_name,experiment_id,experiment_type,analyzer_run_status\n")

def get_or_create_suite(platform, experiment_type):
    try:
        df = pd.read_csv(SUITE_TRACK_FILE)
    except pd.errors.EmptyDataError:
        df = pd.DataFrame(columns=["suite_name", "suite_id", "experiment_name", "experiment_id", "experiment_type",
                                   "analyzer_run_status"])
    except Exception as e:
        print(f"Error reading suite tracking file: {e}")
        df = pd.DataFrame()

    match = df[df['experiment_type'] == experiment_type]

    if not match.empty:
        suite_id = match['suite_id'].iloc[0]
        suite = platform.get_item(suite_id, ItemType.SUITE, raw=True)
        logging.info(f"Reusing suite from file: {suite.name} ({suite.id})")
        print(f"Reusing suite from file: {suite.name} ({suite.id})")
    elif df.empty or df['suite_id'].isnull().all():
        suite = Suite(name='SNT Suite')
        suite.update_tags({'name': f'suite_{experiment_type}'})
        platform.create_items([suite])
        logging.info(f"Created new suite: {suite.name} ({suite.id})")
        print(f"Created new suite: {suite.name} ({suite.id})")
    else:
        suite_id = df['suite_id'].iloc[0]
        suite = platform.get_item(suite_id, ItemType.SUITE, raw=True)
        logging.info(f"Reusing fallback suite: {suite.name} ({suite.id})")
        print(f"Reusing fallback suite: {suite.name} ({suite.id})")

    return suite

def get_scenario_file_and_script(project_path, is_future):
    if is_future:
        scenario_file = os.path.join(project_path, 'simulation_inputs', '_intervention_file_references',
                                     'Interventions_for_projections.csv')
        sim_script = './run_future_scenarios/run_simulations.py'
    else:
        scenario_file = os.path.join(project_path, 'simulation_inputs', '_intervention_file_references',
                                     'Interventions_to_present.csv')
        sim_script = './run_to_present/run_simulations.py'
    return scenario_file, sim_script

def submit_experiment(sim_script, suite_id):
    proc = subprocess.Popen(
        [sys.executable, sim_script, "--suite-id", str(suite_id)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace'
    )

    exp_name = exp_id = None
    for line in proc.stdout:
        print(line, end='')
        if "EXPERIMENT_NAME" in line:
            exp_name = line.split(":", 1)[-1].strip()
        elif "EXPERIMENT_ID" in line:
            exp_id = line.split(":", 1)[-1].strip()
    proc.wait()  # this is only waiting on submission to comps, once commit, it will unblock
    return exp_name, exp_id

def update_tracking_file(suite_name, suite_id, exp_name, exp_id, experiment_type):
    df = pd.read_csv(SUITE_TRACK_FILE)
    if not ((df['experiment_id'] == exp_id) & (df['suite_id'] == suite_id)).any():
        with open(SUITE_TRACK_FILE, 'a') as f:
            f.write(f"{suite_name},{suite_id},{exp_name},{exp_id},{experiment_type},{ANALYZER_STATUS}\n")

def main():
    ensure_tracking_file()
    _, project_path = load_box_paths(user_path=USER_PATH, country_name='Example')
    experiment_type = "future_projections" if FUTURE_PROJECTIONS else "to_present"

    platform = Platform("Calculon")
    suite = get_or_create_suite(platform, experiment_type)
    suite_id, suite_name = str(suite.id), suite.name

    scenario_file, sim_script = get_scenario_file_and_script(project_path, FUTURE_PROJECTIONS)
    df = pd.read_csv(scenario_file)
    df_run = df[df['status'] == 'run'].copy()

    for idx in df_run.index:
        exp_name, exp_id = submit_experiment(sim_script, suite_id)
        if exp_name and exp_id:
            update_tracking_file(suite_name, suite_id, exp_name, exp_id, experiment_type)
        df.loc[idx, 'status'] = 'queued'
        df.to_csv(scenario_file, index=False)

if __name__ == "__main__":
    main()