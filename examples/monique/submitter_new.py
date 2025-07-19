import os
import sys
import subprocess
import pandas as pd
from idmtools.core.platform_factory import Platform
from idmtools.entities import Suite
from idmtools.core import ItemType
from snt.load_paths import load_box_paths

CURRENT_DIRECTORY = os.path.dirname(__file__)
# Specify data location
USER_PATH = None
# USER_PATH = r'C:\Projects\emodpy-snt'
_, project_path = load_box_paths(user_path=USER_PATH, country_name='Example')
future_projections = False
experiment_type = "future_projections" if future_projections else "to_present"

# Initialize platform
platform = Platform("CALCULON")
# Ensure the tracking file exists with header
suite_track_file = os.path.join(CURRENT_DIRECTORY, 'suite_tracking_new.csv')
if not os.path.exists(suite_track_file):
    with open(suite_track_file, 'w') as f:
        f.write("suite_name,suite_id,experiment_name,experiment_id,experiment_type\n")

# Try to load existing suite from tracking file
df_suite = pd.read_csv(suite_track_file)
# Look for a matching suite by experiment type
match = df_suite[df_suite['experiment_type'] == experiment_type]
# Load or create suite
if not match.empty:
    suite_id = match['suite_id'].iloc[0]
    suite = platform.get_item(suite_id, ItemType.SUITE)
    suite_name = suite.name
    print(f"Reusing suite from file: {suite_name} ({suite_id})")

else:
    # No matching suite found — create new or reuse first suite in file
    if df_suite.empty or df_suite['suite_id'].isnull().all():
        suite = Suite(name='SNT Suite')
        suite.update_tags({'name': f'suite_{experiment_type}'})
        platform.create_items([suite])
        print(f"Created new suite: {suite.name} ({suite.id})")
    else:
        # Fall back to reusing the first suite in the file
        suite_id = df_suite['suite_id'].iloc[0]
        suite = platform.get_item(suite_id, ItemType.SUITE)
        suite.update_tags({'name': f'suite_{experiment_type}'})
        print(f"Reusing fallback suite from file: {suite.name} ({suite.id})")

    suite_id = str(suite.id)
    suite_name = suite.name

# Select simulation script
if future_projections:
    scenario_fname = os.path.join(project_path, 'simulation_inputs', '_intervention_file_references',
                                  'Interventions_for_projections.csv')
    sim_script = './run_future_scenarios/run_simulations.py'
else:
    scenario_fname = os.path.join(project_path, 'simulation_inputs', '_intervention_file_references',
                                  'Interventions_to_present.csv')
    sim_script = './run_to_present/run_simulations.py'
df = pd.read_csv(scenario_fname)

# Main loop
df = pd.read_csv(scenario_fname)
while len(df[df['status'] == 'run']) > 0:
    # Run the next experiment
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
    proc.wait()

    # Log experiment if not already tracked
    if exp_name and exp_id:
        df_log = pd.read_csv(suite_track_file)
        if not ((df_log['experiment_id'] == exp_id) & (df_log['suite_id'] == suite_id)).any():
            with open(suite_track_file, 'a') as f:
                f.write(f"{suite_name},{suite_id},{exp_name},{exp_id},{experiment_type}\n")

    # Mark as queued
    i = df[df['status'] == 'run'].index[0]
    df.loc[i, 'status'] = 'queued'
    df.to_csv(scenario_fname, index=False)
