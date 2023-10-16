import emod_api.campaign as campaign
import numpy as np
import pandas as pd
from snt.hbhi.set_up_interventions import InterventionSuite
from emod_api.interventions.common import BroadcastEvent, DelayedIntervention
from emodpy_malaria.interventions.common import add_triggered_campaign_delay_event
from snt.hbhi.set_up_interventions import add_all_interventions
from emodpy_malaria.interventions.vaccine import add_scheduled_vaccine, add_triggered_vaccine
import manifest

def non_snt_camp():
    import emod_api.campaign as campaign
    campaign.schema_path = manifest.schema_file

    delay_distribution = {"Delay_Period_Distribution": "CONSTANT_DISTRIBUTION",
                          "Delay_Period_Constant": 274}
    event_name = "RTSS_1_eligible"
    broadcast_event = BroadcastEvent(campaign, event_name)
    delayed_event = DelayedIntervention(campaign, Configs=[broadcast_event],
                                        Delay_Dict=delay_distribution)
    add_triggered_campaign_delay_event(campaign, start_day=1,
                                       trigger_condition_list=['Births'],
                                       demographic_coverage=0.504,
                                       individual_intervention=delayed_event)
    add_triggered_vaccine(campaign,
                          start_day=1,
                          trigger_condition_list=["RTSS_1_eligible"],
                          intervention_name='RTSS',
                          broadcast_event='Received_Vaccine',
                          vaccine_type="AcquisitionBlocking",
                          vaccine_initial_effect=0.8,
                          vaccine_box_duration=0,
                          vaccine_decay_time_constant=592.4067,
                          efficacy_is_multiplicative=False)

    return campaign


def snt_camp():
    import emod_api.campaign as campaign
    campaign.schema_path = manifest.schema_file

    int_suite =  InterventionSuite()
    int_suite.rtss_auto_changeips = False

    rtss_df = pd.DataFrame({
        'DS_Name': ['Yomou'],
        'coverage_levels': [0.504],
        'round': [3],
        'vaccine': ['simple'],
        'initial_killing': [0.8],
        'decay_time_constant': [592.4067],
        'rtss_touchpoints': [274],
        'decay_class': ['WaningEffectExponential'],
        'RTSS_day': [1],
        'deploy_type': ['EPI'],
        'distribution_name': ['CONSTANT_DISTRIBUTION'],
        'distribution_std': [1]
    })
    print(rtss_df)
    add_all_interventions(campaign, int_suite, my_ds="Yomou",
                          rtss_df=rtss_df)

    return campaign


camp1 = non_snt_camp()
camp2 = snt_camp()

camp1.save("camp1.json")
camp2.save("camp2.json")
