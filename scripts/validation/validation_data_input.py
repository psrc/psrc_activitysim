import os
import toml
import pandas as pd
import numpy as np
import plotly.express as px

config = toml.load(os.path.join(os.getcwd(), 'validation_configuration.toml'))

# read hh, person, tour data from activitysim results for all model results, survey data and unweighted survey
def get_households_data(uncloned = True, col_list=None):

    if col_list is None:
        model_cols = None
        survey_cols = None
    else:
        model_cols = col_list + ['household_id']
        survey_cols = col_list + ['household_id', 'hhid_elmer', 'hh_weight']

    # model data
    model = pd.read_parquet(config['p_model_households'], columns=model_cols).reset_index()
    model['hh_weight'] = np.repeat(1, len(model))
    model['source'] = "model results"

    # survey data
    if uncloned:
        survey = pd.read_csv(config['p_survey_households_uncloned'], usecols=survey_cols).groupby('hhid_elmer').first().reset_index() # remove duplicates
        survey['source'] = "survey data"
    else:
        survey = pd.read_csv(config['p_survey_households'], usecols=survey_cols) 
        survey['source'] = "survey data"

    # unweighted survey data
    survey_unweighted = survey.copy()
    survey_unweighted['hh_weight'] = np.repeat(1, len(survey_unweighted))
    survey_unweighted['source'] = "unweighted survey"

    hh_data = pd.concat([model, survey, survey_unweighted])

    return hh_data


def get_persons_data(uncloned=True, col_list=None):

    if col_list is None:
        model_cols = None
        survey_cols = None
    else:
        model_cols = col_list + ['person_id', 'household_id']
        survey_cols = col_list + ['person_id', 'household_id', 'person_id_elmer_original', 'person_weight']

    # model data
    model = pd.read_parquet(config['p_model_persons'], columns=model_cols).reset_index()
    model['person_weight'] = np.repeat(1, len(model))
    model['source'] = "model results"

    # survey data
    if uncloned:
        survey = pd.read_csv(config['p_survey_persons_uncloned'], usecols=survey_cols).\
        groupby('person_id_elmer_original').first().reset_index() # remove duplicates
    else: 
        survey = pd.read_csv(config['p_survey_persons'], usecols=survey_cols)
    survey['source'] = "survey data"

    # unweighted survey data
    survey_unweighted = survey.copy()
    survey_unweighted['person_weight'] = np.repeat(1, len(survey_unweighted))
    survey_unweighted['source'] = "unweighted survey"

    per_data = pd.concat([model, survey, survey_unweighted])

    return per_data


def get_tours_data(uncloned = True, col_list=None):

    if col_list is None:
        model_cols = None
        survey_cols = None
    else:
        model_cols = col_list + ['person_id', 'household_id', 'tour_id']
        survey_cols = col_list + ['person_id', 'household_id', 'tour_id']

    # model data
    model = pd.read_parquet(config['p_model_tours'], columns=model_cols).reset_index()
    model['trip_weight_2017_2019'] = np.repeat(1, len(model))
    model['source'] = "model results"

    # survey data
    # get tour weights from average trip weights
    if uncloned:
        trip_data = pd.read_csv(config['p_survey_trips_uncloned'], usecols=['tour_id', 'trip_weight_2017_2019'])
    else:
        trip_data = pd.read_csv(config['p_survey_trips'], usecols=['tour_id', 'trip_weight_2017_2019'])
    tour_weights = trip_data.groupby(['tour_id'], group_keys=False)['trip_weight_2017_2019'].mean().reset_index()

    if uncloned:
        survey = pd.read_csv(config['p_survey_tours_uncloned'], usecols=survey_cols).\
        merge(tour_weights, how="left", on='tour_id').reset_index()
    else: 
        survey = pd.read_csv(config['p_survey_tours'], usecols=survey_cols).\
        merge(tour_weights, how="left", on='tour_id').reset_index()
    survey['source'] = "survey data"

    # unweighted survey data
    survey_unweighted = survey.copy()
    survey_unweighted['trip_weight_2017_2019'] = np.repeat(1, len(survey_unweighted))
    survey_unweighted['source'] = "unweighted survey"

    tour_data = pd.concat([model, survey, survey_unweighted])

    return tour_data


def get_trips_data(uncloned=True, col_list=None):

    if col_list is None:
        model_cols = None
        survey_cols = None
    else:
        model_cols = col_list + ['person_id', 'household_id', 'trip_id']
        survey_cols = col_list + ['person_id', 'household_id', 'trip_id']

    # model data
    model = pd.read_parquet(config['p_model_trips'], columns=model_cols).reset_index()
    model['trip_weight_2017_2019'] = np.repeat(1, len(model))
    model['source'] = "model results"

    # survey data
    if uncloned:
        survey = pd.read_csv(config['p_survey_trips_uncloned'], usecols=survey_cols).reset_index()
    else:
        survey = pd.read_csv(config['p_survey_trips'], usecols=survey_cols).reset_index()
    survey['source'] = "survey data"

    # unweighted survey data
    survey_unweighted = survey.copy()
    survey_unweighted['trip_weight_2017_2019'] = np.repeat(1, len(survey_unweighted))
    survey_unweighted['source'] = "unweighted survey"

    trip_data = pd.concat([model, survey, survey_unweighted])

    return trip_data
