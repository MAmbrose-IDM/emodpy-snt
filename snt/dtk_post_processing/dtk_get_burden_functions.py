import os
import datetime
import json
import pandas as pd
import numpy as np

def monthlyU1PfPRAnalyzer(output_path, start_year, end_year):
    adf = pd.DataFrame()
    for year in range(start_year, (end_year+1)):
        fname = os.path.join(output_path, "MalariaSummaryReport_Monthly%d.json" % year)
        with open(fname, "r") as f:
            data = json.load(f)
        pop_all = data['DataByTimeAndAgeBins']['Average Population by Age Bin'][:12]
        pop = [x[0] for x in pop_all]
        d = data['DataByTimeAndAgeBins']['PfPR by Age Bin'][:12]
        pfpr = [x[0] for x in d]
        d = data['DataByTimeAndAgeBins']['Annual Clinical Incidence by Age Bin'][:12]
        clinical_cases = [d[yy][0] * pop_all[yy][0] * 30 / 365 for yy in range(12)]
        d = data['DataByTimeAndAgeBins']['Annual Severe Incidence by Age Bin'][:12]
        severe_cases = [d[yy][0] * pop_all[yy][0] * 30 / 365 for yy in range(12)]

        simdata = pd.DataFrame({'month': range(1, 13),
                                'PfPR U1': pfpr,
                                'Cases U1': clinical_cases,
                                'Severe cases U1': severe_cases,
                                'Pop U1': pop})
        simdata['year'] = year
        adf = pd.concat([adf, simdata])

    adf.to_csv((os.path.join(output_path, 'U1_PfPR_ClinicalIncidence.csv')), index=False)


def monthlyU5PfPRAnalyzer(output_path, start_year, end_year):
    adf = pd.DataFrame()
    for year in range(start_year, (end_year+1)):
        fname = os.path.join(output_path, "MalariaSummaryReport_Monthly%d.json" % year)
        with open(fname, "r") as f:
            data = json.load(f)
        # use weighted average for combined age groups
        # population size
        pop_all = data['DataByTimeAndAgeBins']['Average Population by Age Bin'][:12]  # remove final five days: assume final five days have same average as rest of month
        pop = [sum(x[:2]) for x in pop_all]
        # PfPR
        d = data['DataByTimeAndAgeBins']['PfPR by Age Bin'][:12]  # remove final five days: assume final five days have same average as rest of month
        pfpr = [((d[yy][0] * pop_all[yy][0]) + (d[yy][1] * pop_all[yy][1])) / (pop_all[yy][0] + pop_all[yy][1]) for yy in range(12)]
        # clinical cases
        d = data['DataByTimeAndAgeBins']['Annual Clinical Incidence by Age Bin'][:12]  # remove final five days: assume final five days have same average as rest of month
        # adjust the per-person annualized number (the reported value) to get the total number of clinical cases in that age group in a month
        clinical_cases = [((d[yy][0] * pop_all[yy][0]) + (d[yy][1] * pop_all[yy][1])) * 30 / 365 for yy in range(12)]
        # severe cases
        d = data['DataByTimeAndAgeBins']['Annual Severe Incidence by Age Bin'][:12]  # remove final five days: assume final five days have same average as rest of month
        # adjust the per-person annualized number (the reported value) to get the total number of severe cases in that age group in a month
        severe_cases = [((d[yy][0] * pop_all[yy][0]) + (d[yy][1] * pop_all[yy][1])) * 30 / 365 for yy in range(12)]

        simdata = pd.DataFrame({'month': range(1, 13),
                                'PfPR U5': pfpr,
                                'Cases U5': clinical_cases,
                                'Severe cases U5': severe_cases,
                                'Pop U5': pop})
        simdata['year'] = year
        adf = pd.concat([adf, simdata])

    adf.to_csv((os.path.join(output_path, 'U5_PfPR_ClinicalIncidence.csv')), index=False)


def monthparser(x):
    if x == 0:
        return 12
    else:
        return datetime.datetime.strptime(str(x), '%j').month


def monthlyTreatedCasesAnalyzer(output_path, start_year):
    channels = ['Received_Treatment', 'Received_Severe_Treatment', 'Received_NMF_Treatment',
                         'Received_Vaccine']
    inset_channels = ['Statistical Population', 'New Clinical Cases', 'New Severe Cases',
                           'PfHRP2 Prevalence', 'Blood Smear Parasite Prevalence']

    fname0 = os.path.join(output_path, "ReportEventCounter.json")
    with open(fname0, "r") as f:
        data0 = json.load(f)
    fname1 = os.path.join(output_path, "ReportMalariaFiltered.json")
    with open(fname1, "r") as f:
        data1 = json.load(f)

    channels_in_expt = [x for x in channels if x in data0['Channels'].keys()]
    simdata = pd.DataFrame({x: data0['Channels'][x]['Data'] for x in channels_in_expt})
    simdata['Time'] = simdata.index

    d = pd.DataFrame({x: data1['Channels'][x]['Data'] for x in inset_channels})
    d['Time'] = d.index

    if len(channels_in_expt) > 0:
        simdata = pd.merge(left=simdata, right=d, on='Time')
    else:
        simdata = d
    for missing_channel in [x for x in channels if x not in channels_in_expt]:
        simdata[missing_channel] = 0

    simdata['Day'] = simdata['Time'] % 365
    simdata['month'] = simdata['Day'].apply(lambda x: monthparser((x + 1) % 365))
    simdata['year'] = simdata['Time'].apply(lambda x: int(x / 365) + start_year)
    simdata = simdata.reset_index(drop=True)
    simdata['date'] = simdata.apply(lambda x: datetime.date(int(x['year']), int(x['month']), 1), axis=1)

    sum_channels = channels + ['New Clinical Cases', 'New Severe Cases']
    mean_channels = ['Statistical Population', 'PfHRP2 Prevalence']

    df = simdata.groupby(['date'])[sum_channels].agg(np.sum).reset_index()
    pdf = simdata.groupby(['date'])[mean_channels].agg(np.mean).reset_index()

    adf = pd.merge(left=pdf, right=df, on=['date'])
    adf.to_csv((os.path.join(output_path, 'All_Age_monthly_Cases.csv')), index=False)


def monthlySevereTreatedByAgeAnalyzer(output_path, start_year):
    event_name = 'Received_Severe_Treatment'
    agebins = [1, 5, 200]
    output_data = pd.read_csv(os.path.join(output_path, "ReportEventRecorder.csv"))
    output_data = output_data[output_data['Event_Name'] == event_name]

    simdata = pd.DataFrame()
    if len(output_data) > 0:  # there are events of this type
        output_data.loc[:, 'Day'] = output_data['Time'] % 365
        output_data.loc[:, 'month'] = output_data['Day'].apply(lambda x: monthparser((x + 1) % 365))
        output_data.loc[:, 'year'] = output_data['Time'] // 365 + start_year
        output_data.loc[:, 'age in years'] = output_data['Age'] / 365
        for agemax in agebins:
            if agemax < 200:
                agelabel = 'U%d' % agemax
            else:
                agelabel = 'all_ages'
            agemin = 0
            d = output_data[(output_data['age in years'] < agemax) & (output_data['age in years'] > agemin)]
            g = d.groupby(['year', 'month'])['Event_Name'].agg(len).reset_index()
            g = g.rename(columns={'Event_Name': 'Num_%s_Received_Severe_Treatment' % agelabel})
            if simdata.empty:
                simdata = g
            else:
                if not g.empty:
                    simdata = pd.merge(left=simdata, right=g, on=['year', 'month'], how='outer')
                    simdata = simdata.fillna(0)

    else:
        simdata = pd.DataFrame(columns=['year', 'month',
                                        'Num_U1_Received_Severe_Treatment',
                                        'Num_U5_Received_Severe_Treatment',
                                        'Num_all_ages_Received_Severe_Treatment'])
    simdata.fillna(0)
    simdata.to_csv(os.path.join(output_path, 'Treated_Severe_Monthly_Cases_By_Age.csv'), index=False)

    included_child_bins = ['U%i' % x for x in agebins if x < 20]
    for agelabel in included_child_bins:
        severe_treat_df = simdata[['year', 'month', 'Num_%s_Received_Severe_Treatment' % agelabel]]
        # cast to int65 data type for merge with incidence df
        severe_treat_df = severe_treat_df.astype({'month': 'int64', 'year': 'int64'})

        # combine with existing columns of the U5 clinical incidence and PfPR dataframe
        incidence_df = pd.read_csv(os.path.join(output_path, '%s_PfPR_ClinicalIncidence.csv' % agelabel))
        merged_df = pd.merge(left=incidence_df, right=severe_treat_df,
                             on=['year', 'month'],
                             how='left')
        merged_df = merged_df.fillna(0)

        # fix any excess treated cases!
        merged_df['num severe cases %s' % agelabel] = merged_df['Severe cases %s' % agelabel]
        merged_df['excess sev treat %s' % agelabel] = merged_df['Num_%s_Received_Severe_Treatment' % agelabel] - \
                                                      merged_df['num severe cases %s' % agelabel]

        for r, row in merged_df.iterrows():
            if row['excess sev treat %s' % agelabel] < 1:
                continue
            # fix Jan 2020 (start of sim) excess treated severe cases
            if row['year'] == start_year and row['month'] == 1:
                merged_df.loc[(merged_df['year'] == start_year) & (merged_df['month'] == 1),
                              'Num_%s_Received_Severe_Treatment' % agelabel] = np.sum(
                    merged_df[(merged_df['year'] == start_year) &
                              (merged_df['month'] == 1)]['num severe cases %s' % agelabel])
            else:
                # figure out which is previous month
                newyear = row['year']
                newmonth = row['month'] - 1
                if newmonth < 1:
                    newyear -= 1
                excess = row['excess sev treat %s' % agelabel]
                merged_df.loc[(merged_df['year'] == start_year) & (merged_df['month'] == 1), 'Num_%s_Received_Severe_Treatment' % agelabel] = \
                    merged_df.loc[(merged_df['year'] == start_year) & (merged_df['month'] == 1),
                                  'Num_%s_Received_Severe_Treatment' % agelabel] - excess
                merged_df.loc[(merged_df['year'] == start_year) & (merged_df['month'] == 1), 'Num_%s_Received_Severe_Treatment' % agelabel] = \
                    merged_df.loc[(merged_df['year'] == start_year) & (merged_df['month'] == 1),
                                  'Num_%s_Received_Severe_Treatment' % agelabel] + excess
        merged_df['excess sev treat %s' % agelabel] = merged_df['Num_%s_Received_Severe_Treatment' % agelabel] - \
                                                      merged_df['num severe cases %s' % agelabel]
        merged_df.loc[
            merged_df['excess sev treat %s' % agelabel] > 0.5, 'Num_%s_Received_Severe_Treatment' % agelabel] = \
            merged_df.loc[merged_df['excess sev treat %s' % agelabel] > 0.5, 'num severe cases %s' % agelabel]

        del merged_df['num severe cases %s' % agelabel]
        del merged_df['excess sev treat %s' % agelabel]
        merged_df.to_csv(os.path.join(output_path, '%s_PfPR_ClinicalIncidence_severeTreatment.csv' % agelabel), index=False)


def MonthlyNewInfectionsAnalyzer_byAgeGroup_withU1U5(output_path, start_year, end_year):
    adf = pd.DataFrame()
    for year in range(start_year, (end_year+1)):
        fname = os.path.join(output_path, "MalariaSummaryReport_Monthly%d.json" % year)
        with open(fname, "r") as f:
            data = json.load(f)

        # from the 30-day reporting interval, all months have 30 days, plus a final 'month' at the end of the year with 5 days

        # population size
        pop = data['DataByTimeAndAgeBins']['Average Population by Age Bin'][:12]  # remove final five days: assume final five days have same average as rest of month
        pop_Under1 = [x[0] for x in pop]
        pop_Under5 = [sum(x[:2]) for x in pop]
        pop_5to15 = [x[2] for x in pop]
        pop_15to30 = [x[3] for x in pop]
        pop_30to50 = [x[4] for x in pop]
        pop_50plus = [x[5] for x in pop]
        pop_allAges = [sum(x[:6]) for x in pop]

        # new infections
        d = data['DataByTimeAndAgeBins']['New Infections by Age Bin'][:13]
        d[11] = [sum(x) for x in zip(d[11], d[12])]  # add final five days to last month
        del d[-1]  # remove final five days
        new_infections_Under1 = [x[0] for x in d]
        new_infections_Under5 = [sum(x[:2]) for x in d]
        new_infections_5to15 = [x[2] for x in d]
        new_infections_15to30 = [x[3] for x in d]
        new_infections_30to50 = [x[4] for x in d]
        new_infections_50plus = [x[5] for x in d]
        new_infections_allAges = [sum(x[:6]) for x in d]

        # PfPR
        d = data['DataByTimeAndAgeBins']['PfPR by Age Bin'][:12]  # remove final five days: assume final five days have same average as rest of month
        # use weighted average for combined age groups
        pfpr_Under1 = [x[0] for x in d]
        pfpr_Under5 = [((d[yy][0] * pop[yy][0]) + (d[yy][1] * pop[yy][1])) / (pop[yy][0] + pop[yy][1]) for yy in range(12)]
        pfpr_5to15 = [x[2] for x in d]
        pfpr_15to30 = [x[3] for x in d]
        pfpr_30to50 = [x[4] for x in d]
        pfpr_50plus = [x[5] for x in d]
        pfpr_allAges = [((d[yy][0] * pop[yy][0]) + (d[yy][1] * pop[yy][1]) + (d[yy][2] * pop[yy][2]) +
                         (d[yy][3] * pop[yy][3]) + (d[yy][4] * pop[yy][4]) + (d[yy][5] * pop[yy][5])) /
                        (pop[yy][0] + pop[yy][1] + pop[yy][2] + pop[yy][3] + pop[yy][4] + pop[yy][5]) for yy in range(12)]

        # clinical cases
        d = data['DataByTimeAndAgeBins']['Annual Clinical Incidence by Age Bin'][:12]  # remove final five days: assume final five days have same average as rest of month
        # adjust the per-person annualized number (the reported value) to get the total number of clinical cases in that age group in a month
        clinical_cases_Under1 = [d[yy][0] * pop[yy][0] * 30 / 365 for yy in range(12)]
        clinical_cases_Under5 = [((d[yy][0] * pop[yy][0]) + (d[yy][1] * pop[yy][1])) * 30 / 365 for yy in range(12)]
        clinical_cases_5to15 = [d[yy][2] * pop[yy][2] * 30 / 365 for yy in range(12)]
        clinical_cases_15to30 = [d[yy][3] * pop[yy][3] * 30 / 365 for yy in range(12)]
        clinical_cases_30to50 = [d[yy][4] * pop[yy][4] * 30 / 365 for yy in range(12)]
        clinical_cases_50plus = [d[yy][5] * pop[yy][5] * 30 / 365 for yy in range(12)]
        clinical_cases_allAges = [((d[yy][0] * pop[yy][0]) + (d[yy][1] * pop[yy][1]) + (d[yy][2] * pop[yy][2]) +
                                   (d[yy][3] * pop[yy][3]) + (d[yy][4] * pop[yy][4]) + (d[yy][5] * pop[yy][5])) *
                                  30 / 365 for yy in range(12)]

        # severe cases
        d = data['DataByTimeAndAgeBins']['Annual Severe Incidence by Age Bin'][:12]  # remove final five days: assume final five days have same average as rest of month
        # adjust the per-person annualized number (the reported value) to get the total number of severe cases in that age group in a month
        severe_cases_Under1 = [d[yy][0] * pop[yy][0] * 30 / 365 for yy in range(12)]
        severe_cases_Under5 = [((d[yy][0] * pop[yy][0]) + (d[yy][1] * pop[yy][1])) * 30 / 365 for yy in range(12)]
        severe_cases_5to15 = [d[yy][2] * pop[yy][2] * 30 / 365 for yy in range(12)]
        severe_cases_15to30 = [d[yy][3] * pop[yy][3] * 30 / 365 for yy in range(12)]
        severe_cases_30to50 = [d[yy][4] * pop[yy][4] * 30 / 365 for yy in range(12)]
        severe_cases_50plus = [d[yy][5] * pop[yy][5] * 30 / 365 for yy in range(12)]
        severe_cases_allAges = [((d[yy][0] * pop[yy][0]) + (d[yy][1] * pop[yy][1]) + (d[yy][2] * pop[yy][2]) +
                                 (d[yy][3] * pop[yy][3]) + (d[yy][4] * pop[yy][4]) + (d[yy][5] * pop[yy][5])) *
                                30 / 365 for yy in range(12)]

        # order is [under1, under5, 5-15, 15-30, 30-50, over 50]
        simdata = pd.DataFrame({'month': list(range(1, 13)) * 7,  # cycle through months for each age range
                                'AgeGroup': np.repeat(['Under1', 'Under5', '5to15', '15to30', '30to50', '50plus', 'allAges'], 12),
                                'Pop': (pop_Under1 + pop_Under5 + pop_5to15 + pop_15to30 + pop_30to50 + pop_50plus + pop_allAges),
                                'New Infections': (new_infections_Under1 + new_infections_Under5 + new_infections_5to15 + new_infections_15to30 + new_infections_30to50 + new_infections_50plus + new_infections_allAges),
                                'PfPR': (pfpr_Under1 + pfpr_Under5 + pfpr_5to15 + pfpr_15to30 + pfpr_30to50 + pfpr_50plus + pfpr_allAges),
                                'Clinical cases': (clinical_cases_Under1 + clinical_cases_Under5 + clinical_cases_5to15 + clinical_cases_15to30 + clinical_cases_30to50 + clinical_cases_50plus + clinical_cases_allAges),
                                'Severe cases': (severe_cases_Under1 + severe_cases_Under5 + severe_cases_5to15 + severe_cases_15to30 + severe_cases_30to50 + severe_cases_50plus + severe_cases_allAges)
                                })
        simdata['year'] = year
        adf = pd.concat([adf, simdata])
        adf = adf.reset_index(drop=True)
        adf.to_csv(os.path.join(output_path, 'newInfections_PfPR_cases_monthly_byAgeGroup_withU1U5.csv'), index=False)


# class MonthlyNewInfectionsAnalyzerByAge(IAnalyzer):
#
#     def __init__(self, expt_name, sweep_variables=None, working_dir=".", start_year=2020, end_year=2026,
#                  input_filename_base='MalariaSummaryReport_Monthly',
#                  output_filename='newInfections_PfPR_cases_monthly_byAgeGroup.csv'):
#
#         super(MonthlyNewInfectionsAnalyzerByAge, self).__init__(working_dir=working_dir,
#                                                                 filenames=[
#                                                                     "output/%s%d.json" % (input_filename_base, x)
#                                                                     for x in range(start_year, end_year + 1)]
#                                                                 )
#         self.sweep_variables = sweep_variables or ["Run_Number"]
#         self.expt_name = expt_name
#         self.start_year = start_year
#         self.end_year = end_year
#         self.output_filename = output_filename
#
#     # def filter(self, simulation):
#     #     return simulation.status.name == 'Succeeded'
#
#     def map(self, data, simulation):
#
#         adf = pd.DataFrame()
#         for year, fname in zip(range(self.start_year, self.end_year + 1), self.filenames):
#             # from the 30-day reporting interval, all months have 30 days, plus a final 'month' at the end of the year with 5 days
#
#             # iterate through age bins, extracting the monthly values of each metric and then appending into data frame
#             simdata_allAges = pd.DataFrame()
#
#             for aa in range(len(data[fname]['Metadata']['Age Bins'])):
#                 # population size
#                 pop = data[fname]['DataByTimeAndAgeBins']['Average Population by Age Bin'][
#                       :12]  # remove final five days: assume final five days have same average as rest of month
#                 pop_monthly = [x[aa] for x in pop]
#
#                 # new infections
#                 d = data[fname]['DataByTimeAndAgeBins']['New Infections by Age Bin'][:13]
#                 d[11] = [sum(x) for x in zip(d[11], d[12])]  # add final five days to last month
#                 del d[-1]  # remove final five days
#                 new_infections_monthly = [x[aa] for x in d]
#
#                 # PfPR
#                 d = data[fname]['DataByTimeAndAgeBins']['PfPR by Age Bin'][
#                     :12]  # remove final five days: assume final five days have same average as rest of month
#                 pfpr_monthly = [x[aa] for x in d]
#
#                 # clinical cases
#                 d = data[fname]['DataByTimeAndAgeBins']['Annual Clinical Incidence by Age Bin'][
#                     :12]  # remove final five days: assume final five days have same average as rest of month
#                 # adjust the per-person annualized number (the reported value) to get the total number of clinical cases in that age group in a month
#                 clinical_cases_monthly = [d[yy][aa] * pop[yy][aa] * 30 / 365 for yy in range(12)]
#
#                 # severe cases
#                 d = data[fname]['DataByTimeAndAgeBins']['Annual Severe Incidence by Age Bin'][
#                     :12]  # remove final five days: assume final five days have same average as rest of month
#                 # adjust the per-person annualized number (the reported value) to get the total number of severe cases in that age group in a month
#                 severe_cases_monthly = [d[yy][aa] * pop[yy][aa] * 30 / 365 for yy in range(12)]
#
#                 # order is [under 15, 15-30, 30-50, over 50]
#                 simdata = pd.DataFrame({'month': list(range(1, 13)),
#                                         'AgeGroup': np.repeat([data[fname]['Metadata']['Age Bins'][aa]], 12),
#                                         'Pop': pop_monthly,
#                                         'New Infections': new_infections_monthly,
#                                         'PfPR': pfpr_monthly,
#                                         'Clinical cases': clinical_cases_monthly,
#                                         'Severe cases': severe_cases_monthly
#                                         })
#                 simdata['year'] = year
#                 simdata_allAges = pd.concat([simdata_allAges, simdata])
#             adf = pd.concat([adf, simdata_allAges])
#
#         for sweep_var in self.sweep_variables:
#             if sweep_var in simulation.tags.keys():
#                 adf[sweep_var] = simulation.tags[sweep_var]
#         return adf
#
#     def reduce(self, all_data):
#
#         selected = [data for sim, data in all_data.items()]
#         if len(selected) == 0:
#             print("No data have been returned... Exiting...")
#             return
#
#         output_dir = os.path.join(self.working_dir, self.expt_name)
#         os.makedirs(output_dir, exist_ok=True)
#
#         adf = pd.concat(selected).reset_index(drop=True)
#         adf.to_csv((os.path.join(self.working_dir, self.expt_name, self.output_filename)), index=False)

# if __name__ == "__main__":
#     from idmtools.analysis.analyze_manager import AnalyzeManager
#     from idmtools.core import ItemType
#     from idmtools.core.platform_factory import Platform
#     from snt.load_paths import load_box_paths
#
#     platform = Platform('Calculon')
#
#     data_path, project_path = load_box_paths(country_name='Nigeria')
#
#     # working_dir = os.path.join(project_path, 'simulation_output', 'simulations_to_present')
#     # start_year = 2010  # simulation starts in January of this year
#     # end_year = 2024  # simulation ends in December of this year
#     working_dir = os.path.join(project_path, 'simulation_outputs', 'simulations_future')
#     start_year = 2025  # simulation starts in January of this year
#     end_year = 2031  # simulation ends in December of this year
#
#     expt_ids = {
#         # 'NGA25_toPresent_allInter': '0b3921cd-bf7f-f011-aa25-b88303911bc1',
#         'test': '828927f3-d57f-f011-aa25-b88303911bc1'
#     }
#
#     for expname, expid in expt_ids.items():
#         print('running expt %s' % expname)
#         # report_count_channels = ['Received_Treatment', 'Received_Severe_Treatment', 'Received_NMF_Treatment',
#         #                           'Bednet_Got_New_One', 'Bednet_Using',  # 'Received_Self_Medication',
#         #                          'Received_Campaign_Drugs', 'Received_IRS'
#         #                          ]
#         report_count_channels = None
#
#         analyzers = [
#             # MonthlyNewInfectionsAnalyzer_byAgeGroup_withU1U5(expt_name=expname,
#             #                                   sweep_variables=["Run_Number", "admin_name"],
#             #                                   working_dir=working_dir,
#             #                                   start_year=start_year,
#             #                                   end_year=end_year,
#             #                                   input_filename_base='MalariaSummaryReport_Monthly',
#             #                                   output_filename='newInfections_PfPR_cases_monthly_byAgeGroup_withU1U5.csv'),
#             # monthlySevereTreatedAnalyzer_byAgeGroup_withU1U5(expt_name=expname,
#             #                                   sweep_variables=["Run_Number", "admin_name"],
#             #                                   working_dir=working_dir,
#             #                                   start_year=start_year,
#             #                                   end_year=end_year),
#             monthlyU5PfPRAnalyzer(expt_name=expname,
#                                   sweep_variables=["Run_Number", "admin_name"],
#                                   working_dir=working_dir,
#                                   start_year=start_year,
#                                   end_year=end_year),
#         ]
#         am = AnalyzeManager(platform=platform, ids=[(expid, ItemType.EXPERIMENT)], analyzers=analyzers,
#                             analyze_failed_items=True)
#         am.analyze()
#
#         # if 'no_IRS_SMC_ITN_CM' in expname:
#         #     cur_monthlyTreatedCasesAnalyzer = monthlyTreatedCasesAnalyzer(expt_name=expname,
#         #                                                                   channels=['Received_NMF_Treatment'],
#         #                                                                   sweep_variables=["Run_Number",
#         #                                                                                    "admin_name"],
#         #                                                                   working_dir=working_dir,
#         #                                                                   start_year=start_year,
#         #                                                                   end_year=end_year)
#         # else:
#         #     cur_monthlyTreatedCasesAnalyzer = monthlyTreatedCasesAnalyzer(expt_name=expname,
#         #                                                                   channels=report_count_channels,
#         #                                                                   sweep_variables=["Run_Number",
#         #                                                                                    "admin_name"],
#         #                                                                   working_dir=working_dir,
#         #                                                                   start_year=start_year,
#         #                                                                   end_year=end_year)
#         #
#         # analyzers = [
#         #     monthlyU5PfPRAnalyzer(expt_name=expname,
#         #                           sweep_variables=["Run_Number", "admin_name"],
#         #                           working_dir=working_dir,
#         #                           start_year=start_year,
#         #                           end_year=end_year),
#         #     # # ==== <- remove U1 for 2010-2020 if no IPTi
#         #     # monthlyU1PfPRAnalyzer(expt_name=expname,
#         #     #                       sweep_variables=["Run_Number", "admin_name"],
#         #     #                       working_dir=working_dir,
#         #     #                       start_year=start_year,
#         #     #                       end_year=end_year),
#         #     # # =====
#         #     cur_monthlyTreatedCasesAnalyzer,
#         #     monthlyEventAnalyzer(expt_name=expname,
#         #                          channels=report_count_channels,
#         #                          sweep_variables=["Run_Number", "admin_name"],
#         #                          working_dir=working_dir,
#         #                          start_year=start_year,
#         #                          end_year=end_year),
#         #     monthlySevereTreatedByAgeAnalyzer(expt_name=expname,
#         #                                       sweep_variables=["Run_Number", "admin_name"],
#         #                                       working_dir=working_dir,
#         #                                       start_year=start_year,
#         #                                       end_year=end_year,
#         #                                       agebins=[5, 120]),
#         #     MonthlyNewInfectionsAnalyzer(expt_name=expname,
#         #                                  sweep_variables=["Run_Number", "admin_name"],
#         #                                  working_dir=working_dir,
#         #                                  start_year=start_year,
#         #                                  end_year=end_year,
#         #                                  input_filename_base='MalariaSummaryReport_Monthly',
#         #                                  output_filename='newInfections_PfPR_cases_monthly_byAgeGroup.csv'),
#         #     MonthlyNewInfectionsAnalyzer_withU5(expt_name=expname,
#         #                                         sweep_variables=["Run_Number", "admin_name"],
#         #                                         working_dir=working_dir,
#         #                                         start_year=start_year,
#         #                                         end_year=end_year,
#         #                                         input_filename_base='MalariaSummaryReport_Monthly',
#         #                                         output_filename='newInfections_PfPR_cases_monthly_byAgeGroup_withU5.csv'),
#         #     MonthlyNewInfectionsAnalyzerByAge(expt_name=expname,
#         #                                       sweep_variables=["Run_Number", "admin_name"],
#         #                                       working_dir=working_dir,
#         #                                       start_year=start_year,
#         #                                       end_year=end_year,
#         #                                       input_filename_base='MalariaSummaryReport_Monthly',
#         #                                       output_filename='newInfections_PfPR_cases_monthly_byAgeGroup.csv')
#         #
#         # ]
#         # am = AnalyzeManager(platform=platform, ids=[(expid, ItemType.EXPERIMENT)], analyzers=analyzers,
#         #                     analyze_failed_items=True)
#         # am.analyze()
#
