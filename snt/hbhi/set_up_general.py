import os
import pandas as pd
import numpy as np
from pathlib import Path
import emodpy_malaria.malaria_config as malaria_config
import emod_api.config.default_from_schema_no_validation as dfs
from emodpy_malaria.interventions.outbreak import add_outbreak_individual
from emodpy_malaria.reporters.builtin import add_report_malaria_filtered, add_event_recorder


def initialize_config(config, manifest, years, serialize, yr_plusone=True,
                      ser_time_step=None, x_pop_scale=1):
    """
    Initialize config builder with preset params.

    Start a (default) MALARIA_SIM config builder with presets including:
    durations, logging, demographics, serialization, vector and reports.

    Args:
        config:
        years: int
            Simulation duration in years
        serialize: bool
            To serialize simulation outcome or not
        yr_plusone: bool, default: True
            For serialization, simulation duration needs to plus one day to work
        ser_time_step: list of int, default: None
            A list defining the sim day(s) to serialize the population. When set
            to None, serialization will occur at `years*365`
        x_pop_scale: int, default = 1
            Scaling factor for simulation population,
            applies to x_Base_Population, x_Birth, x_Temporary_Larval_Habitat

    Returns:
        config
    """
    # Run time
    config.parameters.Simulation_Duration = years * 365 + yr_plusone

    # Logging
    config.parameters['logLevel_JsonConfigurable'] = 'ERROR'
    config.parameters['logLevel_VectorHabitat'] = 'ERROR'
    config.parameters['logLevel_StandardEventCoordinator'] = 'ERROR'
    config.parameters['logLevel_SusceptibilityMalaria'] = 'ERROR'

    # Demographics
    config.parameters.Birth_Rate_Dependence = "FIXED_BIRTH_RATE"
    config.parameters.Age_Initialization_Distribution_Type = "DISTRIBUTION_COMPLEX"
    config.parameters.x_Base_Population = x_pop_scale
    config.parameters.x_Birth = x_pop_scale

    # Serialization
    if ser_time_step is None:
        ser_time_step = [365 * years]
    if serialize:
        config.parameters.Serialization_Time_Steps = ser_time_step
        config.parameters.Serialized_Population_Writing_Type = "TIMESTEP"
        config.parameters.Serialization_Mask_Node_Write = 0
        config.parameters.Serialization_Precision = "REDUCED"
    # else: do nothing, we're already not serializing

    # Vector
    malaria_config.add_species(config, manifest, ['arabiensis', 'funestus', 'gambiae'])
    config.parameters.x_Temporary_Larval_Habitat = 0.2 * x_pop_scale
    malaria_config.set_species_param(config, 'arabiensis', 'Anthropophily', 0.65, overwrite=True)
    malaria_config.set_species_param(config, 'arabiensis', 'Indoor_Feeding_Fraction', 0.5, overwrite=True)
    malaria_config.set_species_param(config, 'funestus', 'Anthropophily', 0.35, overwrite=True)
    malaria_config.set_species_param(config, 'funestus', 'Indoor_Feeding_Fraction', 0.92, overwrite=True)
    malaria_config.set_species_param(config, 'gambiae', 'Anthropophily', 0.85, overwrite=True)
    malaria_config.set_species_param(config, 'gambiae', 'Indoor_Feeding_Fraction', 0.95, overwrite=True)

    # Report
    config.parameters.Enable_Default_Reporting = 0
    config.parameters.Enable_Demographics_Risk = 1
    config.parameters.Enable_Property_Output = 0
    config.parameters.Enable_Vector_Species_Report = 0
    config.parameters.Report_Detection_Threshold_Blood_Smear_Parasites = 50
    config.parameters.Report_Parasite_Smear_Sensitivity = 0.02  # 50/uL
    config.parameters.Report_Detection_Threshold_Blood_Smear_Gametocytes = 50
    config.parameters.Report_Gametocyte_Smear_Sensitivity = 0.01 #default

    # Resolving changes between malaria defaults and
    config.parameters.Enable_Demographics_Birth = 1
    config.parameters.Enable_Initial_Prevalence = 1
    config.parameters.Enable_Natural_Mortality = 1
    config.parameters.Enable_Vector_Migration = 0
    config.parameters.Enable_Initial_Prevalence = 1
    config.parameters.Start_Time = 0

    return config


def initialize_reports(task, manifest, event_reporter: bool = False, filtered_report: int = None,
                       years: float = None, yr_plusone: bool = True):
    """
    Initialize reports.

    Args:
        task: Task to which to add the reporter, if left as None, reporter is returned (used for unittests)
        event_reporter: bool, default = False
            If True, switch on 'Report_Event_Recorder' with default event of
            'Received_Severe_Treatment'
        filtered_report: int, default = None
            Number of years (counting from last) to add filtered report on. None
            means no filtered report added
        years: number of years you want filtered report to collect information
        yr_plusone: bool, default: True
            For serialization, simulation duration needs to plus one day to work

    Returns:
        Nothing
    """

    if event_reporter:
        add_event_recorder(task, event_list=['Received_Severe_Treatment',
                                             # 'Received_Treatment', 'NewClinicalCase',
                                             # 'NewSevereCase', 'Received_Campaign_Drugs',
                                             # 'No_SMC_Fever'
                                             ])

    if filtered_report:
        num_year = filtered_report
        start = yr_plusone + (years - num_year) * 365
        end = yr_plusone + years * 365
        # ReportMalariaFiltered
        add_report_malaria_filtered(task, manifest, start_day=int(start), end_day=int(end))


def load_master_csv(projectpath, file=None, country=None):
    if file is None:
        if country in ['burkina', 'guinea']:
            fname = country + "_DS_pop.csv"
            ds_name = 'DS_Name'
        elif country == 'nigeria':
            fname = country + "_LGA_pop.csv"
            ds_name = 'LGA'
        elif country is None:
            raise Exception("Must specify either the file or the country.")
        else:
            raise Exception("Only burkina, guinea and nigeria are supported for country specification.")
    else:
        fname = file
        ds_name = 'DS_Name'

    master_csv = os.path.join(projectpath, fname)
    df = pd.read_csv(master_csv, encoding='latin')
    df['DS_Name'] = df[ds_name].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    df = df.set_index('DS_Name')
    return df


def set_input_files(config, my_ds, archetype_ds=None, demographic_suffix='',
                    climate_suffix='', climate_prefix=True, use_archetype=True):
    if archetype_ds is None:
        archetype_ds = my_ds

    if demographic_suffix is not None:
        if not demographic_suffix.startswith('_') and not demographic_suffix == '':
            demographic_suffix = '_' + demographic_suffix

    if climate_suffix is not None:
        if not climate_suffix.startswith('_') and not climate_suffix == '':
            climate_suffix = '_' + climate_suffix

    if use_archetype:
        ds = archetype_ds
    else:
        ds = my_ds

    # config.parameters['District_Sanitaire'] = 'my_ds'
    # config.parameters['Archetype'] = 'archetype_ds'

    if demographic_suffix is not None:
        config.parameters.Demographics_Filenames = [
            os.path.join(ds, '%s_demographics%s.json' % (ds, demographic_suffix))]

    if climate_suffix is not None:
        if climate_prefix:
            config.parameters.Air_Temperature_Filename = os.path.join(ds, '%s_air_temperature_daily%s.bin' % (
                ds, climate_suffix))
            config.parameters.Land_Temperature_Filename = os.path.join(ds, '%s_air_temperature_daily%s.bin' % (
                ds, climate_suffix))
            config.parameters.Rainfall_Filename = os.path.join(ds, '%s_rainfall_daily%s.bin' % (ds, climate_suffix))
            config.parameters.Relative_Humidity_Filename = os.path.join(ds, '%s_relative_humidity_daily%s.bin' % (
                ds, climate_suffix))

        else:
            config.parameters.Air_Temperature_Filename = os.path.join(ds,
                                                                      'air_temperature_daily%s.bin' % climate_suffix)
            config.parameters.Land_Temperature_Filename = os.path.join(ds,
                                                                       'air_temperature_daily%s.bin' % climate_suffix)
            config.parameters.Rainfall_Filename = os.path.join(ds, 'rainfall_daily%s.bin' % climate_suffix)
            config.parameters.Relative_Humidity_Filename = os.path.join(ds,
                                                                        'relative_humidity_daily%s.bin' % climate_suffix)

    return {'DS_Name': my_ds}


def add_input_files(task, inputpath, my_ds, archetype_ds=None, demographic_suffix='',
                    climate_suffix='', climate_prefix=True, use_archetype=True):
    """
    Add assets corresponding to the filename parameters set in set_input_files.

    Args:
        task:
        iopath:
        my_ds:
        archetype_ds:
        demographic_suffix:
        climate_suffix:
        climate_prefix:
        use_archetype:

    Returns:
        None
    """
    if archetype_ds is None:
        archetype_ds = my_ds

    if demographic_suffix is not None:
        if not demographic_suffix.startswith('_') and not demographic_suffix == '':
            demographic_suffix = '_' + demographic_suffix

    if climate_suffix is not None:
        if not climate_suffix.startswith('_') and not climate_suffix == '':
            climate_suffix = '_' + climate_suffix

    if use_archetype:
        ds = archetype_ds
    else:
        ds = my_ds

    if demographic_suffix is not None:
        demog_path = os.path.join(ds, f'{ds}_demographics{demographic_suffix}.json')
        task.common_assets.add_asset(os.path.join(inputpath, demog_path),
                                     relative_path=str(Path(demog_path).parent), fail_on_duplicate=False)

    if climate_suffix is not None:
        if climate_prefix:
            file_path = os.path.join(ds, f'{ds}_air_temperature_daily{climate_suffix}.bin')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)
            file_path = os.path.join(ds, f'{ds}_air_temperature_daily{climate_suffix}.bin.json')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)

            file_path = os.path.join(ds, f'{ds}_rainfall_daily{climate_suffix}.bin')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)
            file_path = os.path.join(ds, f'{ds}_rainfall_daily{climate_suffix}.bin.json')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)

            file_path = os.path.join(ds, f'{ds}_relative_humidity_daily{climate_suffix}.bin')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)
            file_path = os.path.join(ds, f'{ds}_relative_humidity_daily{climate_suffix}.bin.json')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)
        else:
            file_path = os.path.join(ds, f'air_temperature_daily{climate_suffix}.bin')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)
            file_path = os.path.join(ds, f'air_temperature_daily{climate_suffix}.bin.json')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)

            file_path = os.path.join(ds, f'rainfall_daily{climate_suffix}.bin')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)
            file_path = os.path.join(ds, f'rainfall_daily{climate_suffix}.bin.json')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)

            file_path = os.path.join(ds, f'relative_humidity_daily{climate_suffix}.bin')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)
            file_path = os.path.join(ds, f'relative_humidity_daily{climate_suffix}.bin.json')
            task.common_assets.add_asset(os.path.join(inputpath, file_path),
                                         relative_path=str(Path(file_path).parent), fail_on_duplicate=False)


def setup_ds(config, manifest, platform, my_ds, archetype_ds=None,
             pull_from_serialization=False,
             burnin_id='', ser_date=50 * 365,
             burnin_df=None,
             rel_abund_df=None, lhdf=None, use_arch_burnin=True,
             from_arch=None, demographic_suffix='',
             climate_suffix='',
             climate_prefix=True,
             use_arch_input=True,
             hab_multiplier=1, run_number=-1,
             ds_name='DS_Name',
             serialize_match_tag=None,
             serialize_match_val=None):
    """
    Setting an individual DS up to add to config builder

    Given a DS, pull its archetype, demographic, climate and vector files, 
    its corresponding serialized population (if applicable) and other info
    to feed into the config builder. Usually used within `ModFn` and 
    `ModBuilder`.

    Args:
        config: schema-backed config dictionary, written to config.json
        platform:
        manifest:
        my_ds : str
        archetype_ds : str, default: None
            The archetype DS of `my_ds`
        pull_from_serialization : bool, default: False
        burnin_id : str, default: ''
            Experiment ID to pull serialization from
        ser_date : int, default: 50*365
            Number of sim days in the serialized population
        burnin_fname : str, default: ''
            Path to csv files containing dataframe that specifies where to look
            up for serialized populations; Only needed if `pull_from_serialization`
            is True and ignore if `burnin_id` is specified
        rel_abund_df : pandas.DataFrame, default: None
            Data frame of relative vector abundance by DS (i.e., interspecies
            relative abundance)
        lhdf : pandas.DataFrame, default: None
            Data frame of larval habitats per month per DS
        use_arch_burnin : bool, default: True
            Pull serialization from corresponding archetype instead of DS itself
        from_arch : bool, default: None
            (deprecated) Same as `use_arch_burnin`, supersedes its value if not
            `None`
        demographic_suffix : str, default: ''
            Default demographic input file is '(DSNAME)_demographic', this argument
            adds suffix to input file name. Note: if set to `None`, demographic input
            file will not be added
        climate_suffix : str, default: ''
            Same behaviour as `demographic_suffix`
        climate_prefix : bool, default: True
            Whether to add DS name in front of climate files.
        use_arch_input : bool, default: True
            If True, use archetype DS's demographic and climate input file instead
        hab_multiplier : float, default: 1
        run_number : int, default: -1
            `run_number` will be matched with `run_number` of the serialized population
            from the burnin experiment
        ds_name : str, default: 'DS_Name'
            The variable name that tags a simulation run to a DS; Could be 'LGA' in the
            case of Nigeria
        serialize_match_val:
        serialize_match_tag:
    
    Returns:
        Dictionary of two keys, the DS_Name and the corresponding archetype DS
    
    """

    # For backward compatibility, from_arch supersedes use_arch_burnin
    if from_arch is not None:
        use_arch_burnin = from_arch
    set_input_files(config, my_ds, archetype_ds, demographic_suffix, climate_suffix, climate_prefix, use_arch_input)
    if not archetype_ds:
        archetype_ds = my_ds

    if rel_abund_df is not None:
        set_habitats(config, manifest, rel_abund_df, lhdf, archetype_ds, abs(hab_multiplier))  # To be fixed

    if pull_from_serialization:
        # serialize_match check
        if serialize_match_tag:
            if serialize_match_val:
                if not len(serialize_match_tag) == len(serialize_match_val):
                    raise Exception("serialize_match_tag and serialize_match_val not same length.")
            else:
                raise Exception("serialize_match_val must be specified if serialize_match_tag is not None.")
        else:
            serialize_match_tag = ['Habitat_Multiplier', 'Run_Number']
            serialize_match_val = [hab_multiplier, run_number]

        # serialization
        # print("retrieving burnin")
        if burnin_id:
            ser_df = platform.create_sim_directory_df(burnin_id)  # TODO: or we can pass ser_df in
        else:
            ser_df = burnin_df

        if use_arch_burnin:
            ser_df = ser_df[ser_df[ds_name] == archetype_ds]
        else:
            ser_df = ser_df[ser_df[ds_name] == my_ds]

        sdf = ser_df.copy()
        for t, v in zip(serialize_match_tag, serialize_match_val):
            if type(v) == float:
                sdf[t] = sdf[t].apply(lambda x: np.round(float(x), 5))
                sdf = sdf[sdf[t] == np.round(v, 5)]
            else:
                sdf = sdf[sdf[t] == v]
            config.parameters[t] = v
        ser_path = sdf['outpath'].values[0]

        config.parameters.Serialized_Population_Path = os.path.join(ser_path, 'output')
        config.parameters.Serialized_Population_Filenames = ['state-%05d.dtk' % ser_date]
        config.parameters.Serialized_Population_Reading_Type = "READ"
        config.parameters.Enable_Random_Generator_From_Serialized_Population = 0
        config.parameters.Serialization_Mask_Node_Read = 0
    # else: do nothing, we're already not serializing

    return {ds_name: my_ds,
            'archetype': archetype_ds}


def add_recurring_outbreak(campaign):
    """
        Adds a recurring outbreak that starts on day 182, affects 0.01 of the populations,
        repeats every 365 days and repeats forever.

    Args:
        campaign: campaign object to which the intervention will be added, and schema_path container

    Returns:
        Nothing
    """

    add_outbreak_individual(campaign, start_day=182, demographic_coverage=0.01, repetitions=-1,
                            timesteps_between_repetitions=365)


def set_habitats(config, manifest, hdf, lhdf, archetype_ds, hab_multiplier, my_ds=None):
    if my_ds is not None:
        ds = my_ds
    else:
        ds = archetype_ds
    hdf1 = hdf.loc[ds]
    scale_factors = ['arabiensis_scale_factor', 'funestus_scale_factor', 'gambiae_scale_factor']
    prop_list = list(hdf1[scale_factors] / np.sum(hdf1[scale_factors]))
    fraction = tuple(prop_list)
    my_spline, maxvalue, const = load_spline_and_scale_factors(lhdf, archetype_ds)
    const_mult = 1 if hab_multiplier >= 1 else hab_multiplier

    for (s, sp) in zip(fraction, ['arabiensis', 'funestus', 'gambiae']):
        linear_spline_habitat = malaria_config.configure_linear_spline(manifest,
                                                                       max_larval_capacity=pow(10,
                                                                                               maxvalue) * s * hab_multiplier,
                                                                       capacity_distribution_number_of_years=1,
                                                                       capacity_distribution_over_time={
                                                                           "Times": [0, 31, 59, 90, 120, 151, 181, 212,
                                                                                     243, 273, 304,
                                                                                     334],
                                                                           "Values": my_spline})
        malaria_config.set_species_param(config, sp, "Habitats", linear_spline_habitat, overwrite=True)
        habitat = dfs.schema_to_config_subnode(manifest.schema_file, ["idmTypes", "idmType:VectorHabitat"])
        habitat.parameters.Habitat_Type = "CONSTANT"
        habitat.parameters.Max_Larval_Capacity = pow(10, const) * s * const_mult
        malaria_config.set_species_param(config, sp, "Habitats", habitat.parameters)


def load_spline_and_scale_factors(lhdf, archetype_ds):
    if lhdf is None:
        my_spline = [0] * 12
        maxvalue = 1
        const = 1

    else:
        lhdf = lhdf.set_index('archetype')
        my_spline = [lhdf.at[archetype_ds, 'Month%d' % x] for x in range(1, 13)]
        maxvalue = lhdf.at[archetype_ds, 'MaxHab']
        const = lhdf.at[archetype_ds, 'Constant']

    return my_spline, maxvalue, const


# Deprecated, included here as legacy.
# Input DS_vector_rel_abundance should standardize to the format of GN
def habitat_scales(mdf, projectpath, write=False):
    df = mdf.reset_index()
    try:
        rel_abundance_fname = os.path.join(projectpath, 'SpatialClustering_BF', 'DS_vector_rel_abundance.csv')
    except:
        rel_abundance_fname = os.path.join(projectpath, 'SpatialClustering', 'DS_vector_rel_abundance.csv')
    rdf = pd.read_csv(rel_abundance_fname)
    del rdf['DS_Name']
    rdf = pd.merge(rdf, df[['DS', 'DS_Name']], on='DS')
    rdf = rdf.rename(columns={'Anopheles_arabiensis': 'arabiensis_scale_factor',
                              'Anopheles_coluzzii_gambiae': 'gambiae_scale_factor',
                              'Anopheles_funestus_subgroup': 'funestus_scale_factor'})
    rdf = rdf.set_index('DS_Name')
    if write:
        rdf.to_csv(os.path.join(projectpath, 'simulation_inputs', 'DS_vector_rel_abundance.csv'))
    return rdf


def load_rel_abund_df(projectpath):
    rel_abundance_fname = os.path.join(projectpath, 'simulation_inputs', 'DS_vector_rel_abundance.csv')
    rel_abund_df = pd.read_csv(rel_abundance_fname)
    rel_abund_df = rel_abund_df.set_index('DS_Name')
    return rel_abund_df


def set_spaq_params(config):
    malaria_config.set_drug_param(config, 'Sulfadoxine', "Drug_PKPD_C50", 0.2 * 6)
    malaria_config.set_drug_param(config, 'Sulfadoxine', "Max_Drug_IRBC_Kill", 0.506 * 0.675)

    malaria_config.set_drug_param(config, 'Pyrimethamine', "Drug_PKPD_C50", 8 * 6)
    malaria_config.set_drug_param(config, 'Pyrimethamine', "Max_Drug_IRBC_Kill", 0.6 * 0.6417)

    malaria_config.set_drug_param(config, 'Amodiaquine', "Drug_PKPD_C50", 1)
    malaria_config.set_drug_param(config, 'Amodiaquine', "Max_Drug_IRBC_Kill", 0.23)
    malaria_config.set_drug_param(config, 'Amodiaquine', "Drug_Cmax", 95)
    malaria_config.set_drug_param(config, 'Amodiaquine', "Drug_Decay_T1", 0.775)
    malaria_config.set_drug_param(config, 'Amodiaquine', "Drug_Decay_T2", 37.5)
    malaria_config.set_drug_param(config, 'Amodiaquine', "Drug_Vd", 15.6)
    malaria_config.set_drug_param(config, 'Amodiaquine', "Fractional_Dose_By_Upper_Age",
                                  [{"Upper_Age_In_Years": 1, "Fraction_Of_Adult_Dose": 0.376}])

    return {'Drug': "User-defined (set_spaq_params)"}
