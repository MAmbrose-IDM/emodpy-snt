import argparse
from idmtools.analysis.analyze_manager import AnalyzeManager
from idmtools.analysis.platform_anaylsis import PlatformAnalysis
from idmtools.core import ItemType

from snt.analyzers.analyze_helpers import (
    monthlyU1PfPRAnalyzer, monthlyU5PfPRAnalyzer, monthlyTreatedCasesAnalyzer,
    monthlySevereTreatedByAgeAnalyzer, MonthlyNewInfectionsAnalyzer, monthlyEventAnalyzer,
    MonthlyNewInfectionsAnalyzer_withU5, monthlyUsageLLIN
)
from snt.analyzers.analyze_vector_numbers import VectorNumbersAnalyzer
from idmtools.core.platform_factory import Platform

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run analysis on a submitted experiment.")
    parser.add_argument("--exp-id", required=True, help="Experiment ID to analyze")
    parser.add_argument("--type", required=True, help="Experiment type: 'to_present' or 'future_projections'")
    parser.add_argument("--name", required=True, help="Experiment name (used for logging and metadata)")
    args = parser.parse_args()

    exp_id = args.exp_id
    expt_name = args.name
    exp_type = args.type

    # Set year ranges based on experiment type
    if exp_type == "future_projections":
        start_year = 2022
        end_year = 2029
    else:
        start_year = 2010
        end_year = 2021

    itn_comparison_flag = False
    climate_only_flag = False

    with Platform("CALCULON") as platform:
        print(f"\n>>> Running analysis for {expt_name} ({exp_id}) [{exp_type}]")

        sweep_variables = ["Run_Number", "admin_name"]
        if itn_comparison_flag or climate_only_flag:
            sweep_variables.append("Habitat_Multiplier")

        args_each = {
            'expt_name': expt_name,
            'sweep_variables': sweep_variables,
            'working_dir': '.',
            'start_year': start_year,
            'end_year': end_year
        }
        args_new_infect = {
            **args_each,
            'input_filename_base': 'MalariaSummaryReport_Monthly',
            'output_filename': 'newInfections_PfPR_cases_monthly_byAgeGroup.csv'
        }
        args_new_infect_withU5 = {
            **args_each,
            'input_filename_base': 'MalariaSummaryReport_Monthly',
            'output_filename': 'newInfections_PfPR_cases_monthly_byAgeGroup_withU5.csv'
        }
        args_no_u1 = {**args_each, 'agebins': [5, 200]}

        if itn_comparison_flag:
            args_treat_case = {**args_each, 'channels': []}
        elif 'no_IRS_SMC_ITN_CM' in expt_name:
            args_treat_case = {**args_each, 'channels': ['Received_NMF_Treatment']}
        else:
            args_treat_case = args_each
        if end_year > 2022:
            analyzers = [
                MonthlyNewInfectionsAnalyzer_withU5,
                VectorNumbersAnalyzer
            ]
        else:
            analyzers = [
                monthlyTreatedCasesAnalyzer,
            ]
        wi_name_base = "ssmt_analyzer_"
        wi_name = '%s_%s' % (wi_name_base, expt_name)
        if end_year > 2022:
            analysis = PlatformAnalysis(platform=platform,
                                        experiment_ids=[exp_id],
                                        analyzers=analyzers,
                                        analyzers_args=[
                                            # args_each,
                                            # args_each,
                                            # args_treat_case,
                                            # args_each,
                                            # args_each,
                                            # args_each,
                                            # args_new_infect,
                                            args_new_infect_withU5,
                                            args_each
                                        ],
                                        analysis_name=wi_name)
        else:
            analysis = PlatformAnalysis(platform=platform,
                                        experiment_ids=[exp_id],
                                        analyzers=analyzers,
                                        analyzers_args=[
                                            # args_each,
                                            args_treat_case,
                                            # args_no_u1,
                                            # args_each,
                                            # args_each,
                                            # args_new_infect,
                                            # args_new_infect_withU5,
                                        ],
                                        analysis_name=wi_name)
        analysis.analyze()
