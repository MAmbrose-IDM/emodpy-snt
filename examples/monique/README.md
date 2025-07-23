# SNT Simulation Launcher

This repository contains a non-blocking experiment submission system for emodpy-snt, designed to manage large-scale simulation workflows efficiently. It includes:

**submitter_parallel.py**: submits one or more experiments to a newly created suite in COMPS based on a scenario file, by launching run_simulations.py as a subprocess for each scenario row.

**run_simulations.py**: runs a single experiment in COMPS and waits for it to finish in the background. Once the experiment is successfully done, it triggers post_ssmt.py as a separate background subprocess.

**post_ssmt.py**: runs the SSMT analyzer in COMPS for the experiment submitted in the previous step.

Analyzer will download needed files to local.

With everything running in the background, each scenario's output is streamed live and written to its own dedicated log file.

Automatic scenario status tracking and optional post-run analyzers (e.g., SSMT or local analysis).

---

##  Usage Overview

### 1. Prepare Your Scenario File

Create a scenario CSV file like: C:\github\emodpy-snt\data\example_files\simulation_inputs\_intervention_file_references\Interventions_to_present.csv

Only rows with `status == "run"` will be launched by the submitter.

---
### 2. Submit Experiments (Non-blocking)

```bash
python submit_parallel.py
```
* Submits each run-marked row from the scenario file. 
* Each submit run only submit experiments in the same scenario file
* Generate a suite to hold all experiments (one row is one experiment) in scenario file 
* Writes logs to logs/scenario_<experiment_type>_<index>.log
* Tracks submitted suite_id in suite_tracking_<experiment_type>.csv
* <experiment_type> is either "to_present" or "future_projections" depending on FUTURE_PROJECTIONS = True

### 3. What Happens Behind the Scenes
For each scenario row:

A subprocess is launched using run_simulations.py

That process submits an experiment to COMPS

Once the experiment is committed:

Scenario CSV is updated with status "done"

And _post_run function in run_simulations.py will kick post-analysis (e.g., SSMT) script in another no-blocking subprocess

Logs for analyzer are streamed to logs/ssmt_<experiment_type>.log

## CLI Arguments (run_simulations.py)
```bash
python run_simulations.py \
    --suite-id <suite_id> \
    --scen-index <row_index> \
    --scenario-fname <scenario_file> \
    --show-warnings-once True|False|None
```

## CLI Arguments (post_ssmt.py or post_analysis.py)
```bash
python post_ssmt.py \
  --exp_id <exp_id> \
  --type <experiment_type> \
  --name <experiment_name>
```

## Output Files
* logs/scenario_<experiment_type>_<index>.log	Per-scenario log file 
* suite_tracking_<type>.csv	Tracks submitted suite ID and timestamp
* Interventions_*.csv	Scenario inputs, automatically updated with status = queued after submission, then update again to "done" once the experiment is finished
* suite/suite_<id>/exp_<exp_id>.txt	Dump of submitted experiment ID
