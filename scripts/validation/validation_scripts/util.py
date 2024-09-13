from typing import Any
import pandas as pd
from pathlib import Path
import plotly.express as px
import numpy as np

class ValidationData():
    def __init__(self, config) -> None:
        self.config = config
        self.hh_data_uncloned = self._get_hh_data()
        self.hh_data = self._get_hh_data(False)
        self.persons_data_uncloned = self._get_persons_data()
        self.persons_data = self._get_persons_data(False)
        self.land_use = self._get_landuse_data()
        self.tours = self._get_tours_data(False)
        self.tours_cleaned = self._get_tours_data(cleaned=True)
        self.trips = self._get_trips_data(False)

    def _get_hh_data(self, uncloned = True):
        # if col_list is None:
        #     model_cols = None
        #     survey_cols = None
        # else:
        #     model_cols = col_list + ['household_id']
        survey_cols = self.config['hh_columns'] + ['household_id', 'hhid_elmer', 'hh_weight']

        # model data
        #model = pd.read_parquet(self.config['p_model_households'], columns=model_cols).reset_index()
        model = pd.read_parquet(Path(self.config['p_model_path']) / self.config['p_model_households'], columns=self.config['hh_columns']).reset_index()
        model['hh_weight'] = np.repeat(1, len(model))
        model['source'] = "model results"

        # survey data
        if uncloned:
            survey = pd.read_csv(self.config['p_survey_households_uncloned'], usecols=survey_cols).groupby('hhid_elmer').first().reset_index() # remove duplicates
            #survey = pd.read_csv(self.config['p_survey_households_uncloned']).groupby('hhid_elmer').first().reset_index() # remove duplicates
            survey['source'] = "survey data"
        else:
            survey = pd.read_csv(self.config['p_survey_households'], usecols=survey_cols)
            #survey = pd.read_csv(self.config['p_survey_households']) 
            survey['source'] = "survey data"

        # unweighted survey data
        survey_unweighted = survey.copy()
        survey_unweighted['hh_weight'] = np.repeat(1, len(survey_unweighted))
        survey_unweighted['source'] = "unweighted survey"

        hh_data = pd.concat([model, survey, survey_unweighted])

        return hh_data
    
    def _get_persons_data(self, uncloned=True):

        # if col_list is None:
        #     model_cols = None
        #     survey_cols = None
        # else:
        #     model_cols = col_list + ['person_id', 'household_id']
        #     survey_cols = col_list + ['person_id', 'household_id', 'person_id_elmer', 'person_weight']

        # model data
        model = pd.read_parquet(Path(self.config['p_model_path']) / self.config['p_model_persons'], columns=self.config['persons_columns']).reset_index()
        #model = pd.read_parquet(self.config['p_model_persons']).reset_index()
        model['person_weight'] = np.repeat(1, len(model))
        model['source'] = "model results"

        survey_cols = self.config['persons_columns'] + ['person_id_elmer_original', 'person_weight']  

        # survey data
        if uncloned:
            survey = pd.read_csv(self.config['p_survey_persons_uncloned'], usecols=survey_cols).\
            groupby('person_id_elmer_original').first().reset_index() # remove duplicates
        else: 
            survey = pd.read_csv(self.config['p_survey_persons'], usecols=survey_cols)
        survey['source'] = "survey data"
        
        # unweighted survey data
        survey_unweighted = survey.copy()
        survey_unweighted['person_weight'] = np.repeat(1, len(survey_unweighted))
        survey_unweighted['source'] = "unweighted survey"

        per_data = pd.concat([model, survey, survey_unweighted])

        return per_data
    
    def _get_landuse_data(self):
        return pd.read_parquet(Path(self.config['p_model_path']) / self.config['p_landuse']).reset_index()
    
    def _get_tours_data(self, uncloned=True, cleaned=False):
        
        # model data
        model_cols = self.config['tours_columns'] + ['tour_id']
        model = pd.read_parquet(Path(self.config['p_model_path']) / self.config['p_model_tours'], columns=model_cols).reset_index()
        model['tour_weight'] = np.repeat(1, len(model))
        model['source'] = "model results"

        # survey data
        # get tour weights from average trip weights
        # TODO: config['tours_survey_columns'] = config['tours_columns'] without 'atwork_subtour_frequency'
        survey_cols = self.config['tours_survey_columns'] + ['survey_tour_id','tour_weight']

        if uncloned:
            # survey = pd.read_csv(self.config['p_survey_tours_uncloned'], usecols=survey_cols). \
            #     rename(columns={"survey_tour_id": "tour_id"})
            if cleaned:
                survey_cols = self.config['tours_survey_columns'] + ['survey_tour_id']
                survey = pd.read_csv(self.config['p_survey_tours_cleaned'], usecols=survey_cols). \
                    rename(columns={"survey_tour_id": "tour_id"})
                # TODO: no 'tour_weight' in cleaned data
                survey['tour_weight'] = np.repeat(1, len(survey))
            else:
                survey = pd.read_csv(self.config['p_survey_tours_uncloned'], usecols=survey_cols). \
                    rename(columns={"survey_tour_id": "tour_id"})
        else:
            survey = pd.read_csv(self.config['p_survey_tours'], usecols=survey_cols). \
                rename(columns={"survey_tour_id": "tour_id"})

        survey['source'] = "survey data"

        
        # unweighted survey data
        survey_unweighted = survey.copy()
        survey_unweighted['tour_weight'] = np.repeat(1, len(survey_unweighted))
        survey_unweighted['source'] = "unweighted survey"

        tour_data = pd.concat([model, survey, survey_unweighted])

        return tour_data

    def _get_trips_data(self, uncloned=True):
        
        # model data
        model_cols = self.config['trips_columns'] + ['trip_id', 'tour_id']
        model = pd.read_parquet(Path(self.config['p_model_path']) / self.config['p_model_trips'], columns=model_cols).reset_index()
        model['trip_weight'] = np.repeat(1, len(model))
        model['source'] = "model results"

        # survey data
        # get tour weights from average trip weights
        survey_cols = self.config['trips_survey_columns'] + ['survey_tour_id','survey_trip_id','trip_weight']

        if uncloned:
            survey = pd.read_csv(self.config['p_survey_trips_uncloned'], usecols=survey_cols). \
                rename(columns={"survey_tour_id": "tour_id",
                                "survey_trip_id": "trip_id"})
        else:
            survey = pd.read_csv(self.config['p_survey_trips'], usecols=survey_cols). \
                rename(columns={"survey_tour_id": "tour_id",
                                "survey_trip_id": "trip_id"})

        survey['source'] = "survey data"

        
        # unweighted survey data
        survey_unweighted = survey.copy()
        survey_unweighted['trip_weight'] = np.repeat(1, len(survey_unweighted))
        survey_unweighted['source'] = "unweighted survey"

        trip_data = pd.concat([model, survey, survey_unweighted])

        return trip_data

def plot_segments(df:pd.DataFrame, summary_var, segment_var:str, title, title_cat:str,sub_name:str):
    # print(f"n=\n"
    #       f"{df.loc[df['source']=='model results',var].value_counts()[df[var].sort_values().unique()]}")
    df_plot = df.groupby(['source',segment_var,summary_var])['person_weight'].sum().reset_index()
    df_plot['percentage'] = df_plot.groupby(['source',segment_var], group_keys=False)['person_weight'].\
        apply(lambda x: 100 * x / float(x.sum()))

    fig = px.bar(df_plot, x=summary_var, y="percentage", color="source",
                 facet_col=segment_var, barmode="group",template="simple_white",
                 title=title+ title_cat)
    fig.for_each_annotation(lambda a: a.update(text = sub_name + "=<br>" + a.text.split("=")[-1]))
    fig.update_xaxes(title_text=f"n of {title}")
    fig.update_layout(height=400, width=800, font=dict(size=11))
    return fig
