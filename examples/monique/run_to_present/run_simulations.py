import os
import subprocess
import sys
import pandas as pd
from idmtools.core import ItemType
from filelock import FileLock
import manifest
import params
import argparse
from idmtools.core.platform_factory import Platform
from idmtools.entities.experiment import Experiment
from idmtools.entities.templated_simulation import TemplatedSimulations
from datetime import datetime

tracking_file = os.path.join(manifest.CURRENT_DIR, "..", f"suite_tracking_{params.experiment_type}.csv")


def _print_params():
    """
    Just a useful convenient function for the user.
    """
    print("expname: ", params.expname)
    print("population_size: ", params.population_size)
    print("serialize: ", params.serialize)
    print("num_seeds: ", params.num_seeds)
    print("years: ", params.years)
    print("pull_from_serialization: ", params.pull_from_serialization)


def _pre_run(experiment: Experiment, **kwargs):
    """
    Add extra work before run experiment.
    Args:
        experiment: idmtools Experiment
        kwargs: additional parameters
    Return:
        None
    """
    from snt.utility.plugins import initialize_plugins
    initialize_plugins(**kwargs)

def update_suite_tracking(experiment, suite, experiment_type, tracking_file, time_stamp):
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



def _post_run(experiment, suite_id, **kwargs):
    """
    Add extra work after run experiment.
    In this case, we launch the SSMT analyzer after the experiment is done with subprocess in background.
    Logs are written to logs/ssmt_analyzer_{experiment_type}.log.
    Args:
        experiment: idmtools Experiment
        kwargs: additional parameters
    Return:
        None
    """
    if experiment.succeeded:
        suite = platform.get_item(suite_id, ItemType.SUITE, raw=True)
        time_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        update_suite_tracking(experiment, suite, params.experiment_type, tracking_file, time_stamp)

        ssmt_script = os.path.join(manifest.CURRENT_DIR, "..", "analyzers", "post_ssmt.py")
        ssmt_log = os.path.join(manifest.CURRENT_DIR, "..", "logs", f"ssmt_analyzer_{params.experiment_type}.log")

        with open(ssmt_log, "a+", encoding="utf-8") as log_file:
            subprocess.Popen([
                sys.executable,
                ssmt_script,
                "--exp-id", str(experiment.id),
                "--type", str(params.experiment_type),  # or hardcode 'future_projections' etc.
                "--name", str(experiment.name)
            ],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                encoding='utf-8'
            )
        print(f"Launched SSMT analyzer for experiment: {experiment.name}--{experiment.id}")

    print(f"EXPERIMENT_NAME: {experiment.name}")
    print(f"EXPERIMENT_ID: {experiment.id}")


def _config_experiment(**kwargs):
    """
    Build experiment from task and builder. task is EMODTask. builder is SimulationBuilder used for config parameter sweeping.
    Args:
        kwargs: additional parameters
    Return:
        experiment
    """
    from config_task import get_task
    from config_sweep_builders import get_sweep_builders

    builders = get_sweep_builders(**kwargs)

    task = get_task(**kwargs)

    if manifest.sif_id:
        task.set_sif(manifest.sif_id)

    ts = TemplatedSimulations(base_task=task, builders=builders)


    # Set experiment name to include it
    df = pd.read_csv(params.scenario_fname)
    scen_name = df.at[params.scen_index, 'ScenarioName']
    experiment_name = f"{params.experiment_type}_{scen_name}"
    experiment = Experiment.from_template(ts, name=experiment_name)
    return experiment

def update_scenario_status_to_done(scenario_fname, scen_index):
    """
    Update scenario status to 'done' in scenario_fname. This is set to 'done' when the experiment is done.
    Args:
        scenario_fname:
        scen_index:

    Returns:

    """
    lockfile = scenario_fname + ".lock"
    with FileLock(lockfile, timeout=30):
        df = pd.read_csv(scenario_fname)
        df.loc[scen_index, 'status'] = 'done'
        df.to_csv(scenario_fname, index=False)
        print(f"Updated scenario index {scen_index} to 'done' in {scenario_fname}")

def run_experiment(**kwargs):
    """
    Get configured experiment and run.
    Args:
        kwargs: user inputs
    Returns:
        None
    """
    # make sure pass platform through
    kwargs['platform'] = platform
    suite_id = kwargs.pop('suite_id', None)
    _print_params()

    experiment = _config_experiment(**kwargs)
    _pre_run(experiment, **kwargs)
    if suite_id:
        experiment.parent_id = suite_id
    experiment.run(wait_until_done=True)
    #experiment = platform.get_item("0aedf0fe-3567-f011-9f17-b88303912b51", ItemType.EXPERIMENT)  # debug purpose
    update_scenario_status_to_done(params.scenario_fname, params.scen_index)

    _post_run(experiment, suite_id, **kwargs)


if __name__ == "__main__":
    """
    - show_warnings_once=True:  show api warnings for only one simulation
    - show_warnings_once=False: show api warnings for all simulations
    - show_warnings_once=None:  not show api warnings
    """
    parser = argparse.ArgumentParser(description="Run experiment optionally using an existing suite_id")
    parser.add_argument('--suite-id', type=str, help='Optional suite ID to reuse or track')
    parser.add_argument('--scen-index', type=int, required=True, help='Index of scenario to run')
    parser.add_argument('--scenario-fname', type=str, required=True)
    parser.add_argument(
        '--show-warnings-once',
        type=str,
        choices=['True', 'False', 'None'],
        default='True',
        help='True: show warning once, False: for all, None: suppress warnings'
    )
    args = parser.parse_args()
    params.scen_index = args.scen_index
    params.scenario_fname = args.scenario_fname

    def str_to_bool_none(val):
        return {'True': True, 'False': False, 'None': None}[val]

    # Determine warning level
    show_warnings_once = str_to_bool_none(args.show_warnings_once)

    time_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print("\n\n")
    print("============================New Experiment==============================")
    print(f'{time_stamp}: Start experiment with {args.suite_id}')
    platform = Platform('CALCULON')
    # If you don't have Eradication, un-comment out the following to download Eradication
    # import emod_malaria.bootstrap as dtk
    # import pathlib
    # import os
    # dtk.setup(pathlib.Path(manifest.eradication_path).parent)
    # os.chdir(os.path.dirname(__file__))
    # print("...done.")
    #run_experiment(show_warnings_once=True)
    run_experiment(show_warnings_once=show_warnings_once, suite_id=args.suite_id)
