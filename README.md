# emodpy-snt
Subnational(SNT) Workflow for EMODPY

# Subnational Tailoring (SNT) modeling analyese

This repository is meant to contain core scripts that are used for multiple SNT modeling projects.
It relies on the underlying [EMOD](https://docs.idmod.org/projects/emod-malaria/en/latest/index.html) malaria model and is primarily used by researchers at the Institute for Disease Modeling (IDM).  
There is a parallel package also using EMOD that is used by researchers from NU [here](https://github.com/numalariamodeling/hbhi).



## Pre-requisites and installation
requirements.txt can be run to complete the installation of all requirements.

The scripts that the user runs locally are in both python and R, so please make sure those languages and your preferred IDEs are installed.

<!--- uncomment these instructions after upgrading to emodpy-malaria
## Installation
Note: we recommend upgrading pip to the latest version before installation:
```bash
pip install --upgrade pip
```

Run the following command to install the packages required for the project:
```bash
pip install -e . -r requirements.txt
```
--->

### One option for setting up the project in PyCharm
Note: please use your own preferred approach for setting up projects. For those less familiar, here is one potential way to set this up using PyCharm.
1. Get scripts from repo: on Github, fork the main repo. Then clone your copy of the repo so that it is available on your local machine.
2. Open PyCharm; open the repo as a new project (e.g., in a new window)
3. Set up a virtual enviornment for the project (or choose an existing one) - I have one set up to use Python 3.9 (needed for the version of emodpy I'm using)
  - Can do this on command prompt of with the PyCharm GUI menus (Settings-->Project-->Python Interpreter-->(either choose an existing one or create a new venv)
  - If setting up a new venv, will need to install all necessary packages needed for emodpy-malaria - see emodpy-malaria repo instruction at https://github.com/InstituteforDiseaseModeling/emodpy-malaria.git.
4. Open the directories/repos needed to run this project in PyCharm and attach them to the current project:
  - _{repo used for current project}_
  - emodpy-malaria
  - malaria-snt-projects
5. In File-->Settings--> Project-->Project Dependencies, make sure that all the current project depends on all other projects and that the malaria-snt-projects depends on emodpy-malaria. 
  


## Workflow
The main components of the current SNT workflow are:
- Import or create files with information needed for analyses. For example,
  - Demographics
  - Vector species mix
  - Insecticide resistance
  - Interventions timing and coverage from recent past
  - Intervention mix packages for future projections
- Run scripts to generate input files for the model to used
- Create clusters of administrative regions (i.e., LGAs, DS) that are expected to share similar seasonality and baseline transmission intensity
- Calibrate seasonality of each cluster of admins: consists of running a burnin, followed by iterations of calibration
- Calibrate the intrinsic transmission intensity for each admin
  - Run simulation sweep across larval habitat multiplier values: consists of a long burnin sweeps for each archetype (with serialization), followed by shorter simulations specific to each admin (i.e., with that admin's interventions)
  - Run an analyzer to convert the simulation output to the form needed
  - Compare results against observed microscopy prevalence in children U5 from all recent DHS/MIS surveys; save the appropriate xLH values and pick up from appropriate serialized file in next step
- Run simulations of recent interventions and transmission dynamics (e.g., from 2010-2020)
  - Run analyzers to convert the simulation output to the form needed for analysis and plotting
  - Postprocessing: run post-processing script to include effects from malaria in pregnancy and IPTp and to estimate mortality
  - Validation: compare simulation results against reference data
- Run simulations of predicted future transmission dynamics under a range of scenarios
  - Run analyzers to convert the simulation output to the form needed for analysis and plotting
  - Postprocessing: run post-processing script to include effects from malaria in pregnancy and IPTp and to estimate mortality
  - Analysis and plotting: the exact analyses and plots needed in this stage differ by project

## Organization

### Input and output files
There should be a core directory for input and output files for a specific SNT project (e.g., for each country and analysis year). In particular, two subdirectories are expected:
- /simulation_inputs: input files that are used to specify model behavior
- /simulation_outputs: output files that are created by analyzers, post-processing, and analysis/plotting scripts

### Scripts
The scripts for the project are organized as follows:
1) Generic functions used across SNT projects:
- /r_utilities/data_processing - contains scripts with functions used to get and setup input files that are used later for calibration, simulation, validation, and/or analyses
- /archetype_clustering - contains scripts with functions that are used for some (but not all) projects when creating seasonality/transmission archetypes
- /simulation - contains files for commissioning simulations, running calibration workflows, running analyzers, running postprocessing, plotting and analyzing the results. A few helper scripts are in this directory, along with the scripts to commission simulations of the recent past and future projections. Other scripts are sorted into subdirectories:
  - /calibration - scripts with functons for the seasonality and baseline transmission calibration steps
  - /analyzers - scripts with functions used to convert the simulation output to the form needed for further analysis
  - /IPTp_mortality_postprocessing - scripts with functions used to include effects from malaria in pregnancy and IPTp and estimate mortality
2) Example of project-specific scripts, which depend on functions in the malaria-snt-projects repo - each project has its own separate repository, but example setup and scripts for running the full process are given here
- /example_.../setup_inputs/0_main_DHS_data_to_sim_inputs.R (corresponds with scripts from /data_processing) - once the basic files are set up, the  script will take care of most of the data processing and formatting to create the data files needed downstream
  - note: for this script to succeed, certain input files must already have been created. Several of the other scripts in the /setup_inputs directory can be used to generate those files.
- /example_.../setup_inputs/create_seasonality_archetypes - find groupings of administrative regions that are expected to have similar seasonality calibration results
  - note: the preferred approach varies by project, so example scripts from current/past projects will be included, but will likely need to be modified
- /example_.../simulation - contains files for commissioning simulations, running calibration workflows, running analyzers, running postprocessing, plotting and analyzing the results. A few helper scripts are in this directory, along with the scripts to commission simulations of the recent past and future projections. Other scripts are sorted into subdirectories:
  - /calibration - scripts for the seasonality and baseline transmission calibration steps
  - /simAdjustments_mortality_MiP_IPTp_IPTi.R - include effects from malaria in pregnancy and IPTp and estimate mortality
  - /checks_validation - compare simulation results against reference data
  - /sim_check_params - additional checks of specific parameters for interventions (note: may remove or consolidate with validation in the future)
  - /plots_results_analyses - generate plots and other output needed by stakeholders
- /example_.../shiny - create a shiny app for stakeholders to view results (not used/needed for all projects)


## Example 1: steps to run full workflow

### Setup inputs - example for BDI

#### Vector files
- Insecticide resistance raster (from MAP) - converted into csv by later script
- Relative vector abundance rasters (from MAP) - converted into csv by later script
- Parameters describing the vector bionomics for each species - save as a csv in the project directory under /simulation_inputs/vector_bionomics.csv  

#### Archetype clustering inputs
For Burundi, we use spatial clustering to create archetypes, where the user needs to set up the following files:
- Create /SpatialClustering directory in the project folder. Need to include:
   - /input_shapefiles/...
      - Shapefile with country borders (specify name in create_seasonality_archetypes_BDI.Rmd)
   - /input_layers/...
      - Raster files corresponding to each variable used in clustering (subdirectory names need to match the variable names used for clustering)  
	  
(Note that for Nigeria 2022 SNT, we run the clustering scripts to generate some simulation input files, but we use routine surveillance data to do clustering to form the archetypes)

#### Routine surveillance data to inform seasonality patterns
- Routine incidence data needs to be processed and saved in a csv in /simulation_inputs/incidence/archetype_incidence.csv. The file should contain rows with rescaled incidence estimates for each archetype-month combination. 

#### DHS/MIS files
- Download data from all relevant DHS/MIS surveys from the DHS website. Save in the data folder (copy the address into the example_bdi/setup_inputs/0_main_DHS_data_to_sim_inputs.R script)
- Create a csv file for each survey year describing the filepath and variable names for relevant variables from the DHS dataset and save them in the project folder under /estimates_from_DHS/DHS_{year}_files_recodes_for_sims.csv
   - Can use /data_processing/DHS_code_examination.R script to explore the DHS datasets and determine the filepaths and variable names. For formatting, follow the example from a prior project.

#### Additional population and intervention files
- ITN seasonal use: csv giving the relative ITN use in each month (i.e., if there is seasonal variance in ITN use, specify it here) - save the csv in the project directory under /simulation_inputs/ITN_use_seasonality.csv
- Non-malarial fever rates: csv giving the rates of NMFs in children and adults - save the csv in the project directory under /simulation_inputs/nmf_rates.csv
- Parameters that determine malaria in pregnancy, IPTp, and mortality outcomes - save the csv in the project directory under /simulation_inputs/parameters_mortality_MiP_IPTp_IPTi.csv  
- Population sizes in each adminstrative region. Can be provided in raster form (and converted to csv with the example_.../archetype_clustering/setup_hbhi_admin_input_files.R script) or as a csv.  

Depending on the project, there may be additional csvs describing the intervention history in the country. These may be used to generate the intervention csvs in example_.../setup_inputs/0_main_DHS_data_to_sim_inputs.R


### Clustering to create seasonality archetypes
For Burundi, we use spatial clustering to create archetypes by running example_bdi/archetype_clustering/create_seasonality_archetypes_BDI.Rmd.  
(For Nigeria 2022 SNT, we ran the spatial clustering script example_nga/archetype_clustering/create_archetypes_level1_nigeria_DS.Rmd. While we use some of the intermediate/generated files of the clustering, we ended up creating seasonality archetypes based on surveillance data using the example_nga/archetype_clustering/create_seasonality_archetypes.R script.)

### Generate input files for simulations and downstream analyses
There are a couple scripts to run to make sure that the necessary input files are created/formatted correctly
- example_bdi/archetype_clustering/setup_hbhi_admin_input_files.R (creates population/archetype dataframe, relative vector abundance, and insecticide resistance csvs
- example_bdi/setup_inputs/0_main_DHS_data_to_sim_inputs.R (creates csvs for intervention inputs)

### Seasonality calibration
Seasonality calibration begins with a burnin, followed by the actual calibration runs, which usually take a few rounds with tuning the parameters that determine the size of the jumps. For the Burundi workflow, the scripts in example_bdi/simulation/calibration/seasonality_calibration are run in order:
  - 01_burnin_for_seasonalityCalib.py - burn-in simulations, with the end state saved in serialized files
  - 02_seasonality_calibration.py - pick up from serialized files (user must copy appropriate burnin experiment ID into script) to run shorter simulations, iteratively changing parameter guesses to try to match target dataset. Note: this process is often expedited if the user manually tunes the parameters. It also often requires multiple rounds.
  - 03_save_best_seasonality_fit.py - save the best-fit values for use in future simulations (user must specify the directory of the relevant calibration output)

### Transmission-intensity calibration
A sweep across transmission intensities is used for finding the appropriate transmission intensity for each admin. For the Burundi workflow, several scripts in example_bdi/simulation/calibration/baseline_calibration are run in order:
  - 00_rescale_demog_vector_files.py - if the population size for seasonality calibration was different than what will be used in upcoming simulations, run this script to adjust vector and human population sizes accordingly
  - 01_serialize_transmission_sweep - run burn-ins for each archetype and each sweep value. The serialized files generated in this step are used in both the next step of calibration and as the burn-ins for the main to-present simulations. 
  - 02_run_transmission_sweep - for each admin and each sweep value, run a simulation to see what the parasite prevalence through time looks like. These results will be matched against reference data in future step. User must specify the experiment ID from the burnins run in the previous step.
  - 03_analyze_ssmt_monthly_U5_PfPR.py - run analyzers on the simulation outputs from the prior step
  - 04_find_best_xLH_fits.py - compare simulation results against reference datasets and save the best match for each admin

### To-present simulations
After determining which monthly larval habitat values to use for each archetype (in the seasonality calibration section) and which transmission intensity multiplier to apply to each admin (in the baseline transmission section), we run simulations of recent malaria transmission and interventions in each admin in the to-present section.
  - In the Burundi workflow, the script example_bdi/simulation/run_to_present.py is used to run the main simulations. User must specify the experiment ID from the burnins generated with 01_serialize_transmission_sweep.py in the prior section.
  - Once simulations are finished, the example_bdi/simulation/analyzers/run_analyzers_ssmt.py script is runs SSMT analyzers to extract the relevant simulation output. User must specify the appropriate start and end year as well as experiment id.
  - If relevant to the project, additional scripts analyzing the output can be run at this point, such as post-processing to calculate mortality, severe disease, and the impact of IPTp; validation scripts; counterfactual plotting; etc.

### Future projections
In the future projections, we typically have several scenarios that are compared against one another. All pick up from where the to-present simulations left off, but make different assumptions about the interventions used in the future. To run this part of the workflow for the Burundi example,
  - Run the example_bdi/simulation/run_future_scenarios.py script. User must specify the experiment ID from the to-present simulations (where the serialized files are saved)
    - If there are several future scenarios, rather than submitting each one manually one after the other, the example_bdi/simulation/submitter.py script can be used (user needs to specify appropriate filepaths and simulation set)
  - Once simulations are finished, the example_bdi/simulation/analyzers/run_analyzers_ssmt.py script is runs SSMT analyzers to extract the relevant simulation output. User must specify the appropriate start and end year as well as experiment id.
  - Depending on the needs of the project, additional scripts analyzing the output can be run at this point. These include post-processing (to calculate mortality, severe disease, and the impact of IPTp) and plotting and analysis scripts.






