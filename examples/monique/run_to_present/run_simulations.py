from idmtools.core import ItemType
from idmtools.entities import Suite

import manifest
import params
import argparse
from idmtools.core.platform_factory import Platform
from idmtools.entities.experiment import Experiment
from idmtools.entities.templated_simulation import TemplatedSimulations


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


def _post_run(experiment, **kwargs):
    """
    Add extra work after run experiment.
    Args:
        experiment: idmtools Experiment
        kwargs: additional parameters
    Return:
        None
    """
    if experiment.succeeded:
        with open("monique\\run_future_scenarios\\experiment_id.txt", "w") as fd:
            fd.write(experiment.id)
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

    experiment = Experiment.from_template(ts, name=params.expname)

    return experiment


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
    experiment.run(wait_until_done=False)
    _post_run(experiment, **kwargs)


if __name__ == "__main__":
    """
    - show_warnings_once=True:  show api warnings for only one simulation
    - show_warnings_once=False: show api warnings for all simulations
    - show_warnings_once=None:  not show api warnings
    """
    parser = argparse.ArgumentParser(description="Run experiment optionally using an existing suite_id")
    parser.add_argument('--suite-id', type=str, help='Optional suite ID to reuse or track')
    parser.add_argument(
        '--show-warnings-once',
        type=str,
        choices=['True', 'False', 'None'],
        default='True',
        help='True: show warning once, False: for all, None: suppress warnings'
    )
    args = parser.parse_args()


    def str_to_bool_none(val):
        return {'True': True, 'False': False, 'None': None}[val]

    # Determine warning level
    show_warnings_once = str_to_bool_none(args.show_warnings_once)

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
