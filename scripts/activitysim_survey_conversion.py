####################################
# Preprocess Daysim-formatted survey files for Activitysim
# The outputs of this script must be processed by infer.py to produce the final estimation input dataset for Activitysim
# The prefix 'survey_' refers to outputs from this script, which servers as input for infer.py
# The prefix 'override_' refers to the output of infer.py, which will be read by Activitysim in estimation mode.

# Requires py3
####################################

import os
import pandas as pd
import numpy as np
import urllib
import pyodbc
import sqlalchemy

# Survey input files, in Daysim format
survey_input_dir = r'R:\e2projects_two\SoundCast\Inputs\dev\base_year\2018\survey'
output_dir = r'C:\users\bnichols\documents'

# Example survey data for formatting template
example_survey_dir = r'https://raw.githubusercontent.com/ActivitySim/activitysim/master/activitysim/examples/example_estimation/data_sf/survey_data/'
parcel_block_file = r'R:\e2projects_two\activitysim\inputs\data\psrc\two_zone_maz\psrc_data_mtc_model\parcel_block_lookup.csv'
parcel_block = pd.read_csv(parcel_block_file)
parcel_file = r'R:\e2projects_two\SoundCast\Inputs\dev\landuse\2018\with_race\parcels_urbansim.txt'

#zone_type_list = ['TAZ','MAZ','parcel']
zone_type_list = ['MAZ']

race_dict = {
    'African American': 1,
    'Asian': 2,
    'Child': 3,
    'Hispanic': 4,
    'Missing': 5,
    'Other': 6,
    'White Only': 7}

def process_hh(df, parcel_block, zone_type):

    # Add data about whether household lives on a parcel with multi or single family use
    raw_parcels_df = pd.read_csv(parcel_file, delim_whitespace=True, usecols=['parcelid', 'sfunits', 'mfunits']) 
    df = df.merge(raw_parcels_df, left_on = 'hhparcel', right_on = 'parcelid')
    df['is_mf'] = np.where(df['mfunits']>0, 1, 0)

    df.rename(columns={'hhincome': 'income',
                         'hhwkrs': 'num_workers',
                      'hhvehs': 'auto_ownership'}, inplace=True)
    df['household_id'] = df['hhno'].copy()

    # Set household type to 1 for now
    df['HHT'] = 1

    # Household type
    # Integer, 
    # 1 - family household: married-couple; 
    # 2 - family household: male householder, no wife present; 
    # 3 - family household: female householder, no husband present; 
    # 4 - non-family household: male householder living alone; 
    # 5 - non-family household: male householder, not living alone; 
    # 6 - non-family household female householder, living alone; 
    # 7 - non-family household: female householder, not living alone

    if zone_type != 'TAZ':
        parcel_block = parcel_block[-parcel_block['psrc_block_id'].isnull()]
        parcel_block['psrc_block_id'] = parcel_block['psrc_block_id'].astype('int')
        df = df.merge(parcel_block, left_on='hhparcel', right_on='parcel_id', how='left')

    if zone_type == 'MAZ':
        # Use psrc_block_id as the MAZ definition
        df.rename(columns={'psrc_block_id':'home_zone_id'}, inplace=True)
        print(df['home_zone_id'].head())
    elif zone_type == 'parcel':
        df.rename(columns={'hhparcel': 'home_zone_id'}, inplace=True)
    elif zone_type == 'TAZ':
        print('hit')
        df.rename(columns={'hhtaz': 'home_zone_id'}, inplace=True)

    # Add household race from original survey file
    conn_string = "DRIVER={ODBC Driver 17 for SQL Server}; SERVER=AWS-PROD-SQL\Sockeye; DATABASE=Elmer; trusted_connection=yes"
    sql_conn = pyodbc.connect(conn_string)
    params = urllib.parse.quote_plus(conn_string)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    df_orig_survey = pd.read_sql(sql='SELECT hhid, hh_race_category FROM HHSurvey.household_dim_2017_2019', con=engine)
    df = df.merge(df_orig_survey, left_on='household_id', right_on='hhid', how='left')
    df['hh_race'] = df['hh_race_category'].map(race_dict)
        
    return df

# Person
def process_person(df, parcel_block, df_tour, zone_type):

    # For MAZ geography, merge parcel to MAZ lookup
    if zone_type != 'TAZ':
        parcel_block = parcel_block[-parcel_block['psrc_block_id'].isnull()]
        parcel_block['psrc_block_id'] = parcel_block['psrc_block_id'].astype('int')
        # Work
        df = df.merge(parcel_block[['parcel_id','psrc_block_id']], left_on='pwpcl', right_on='parcel_id', how='left')
        df.rename(columns={'psrc_block_id':'pwmaz'},inplace=True)
        df.drop('parcel_id', axis=1, inplace=True)
        # School
        df = df.merge(parcel_block[['parcel_id','psrc_block_id']], left_on='pspcl', right_on='parcel_id', how='left')
        df.rename(columns={'psrc_block_id':'psmaz'},inplace=True)

    # These output columns tend to change, need to be referenced below
    school_col = 'school_taz'
    work_col = 'workplace_taz'

    # define which columns for school and work location (TAZ, parcel, or MAZ)
    col_dict = {
        'TAZ':{
            'school': 'pstaz',
             'work': 'pwtaz'},
        'MAZ':{
            'school': 'psmaz',
            'work': 'pwmaz',
           },
        'parcel':{
            'school': 'pspcl',
            'work': 'pwpcl'}
        }

    df.rename(columns={
        'hhno':'household_id',
        'pagey':'age',
        'pno': 'PNUM',
        'pgend': 'sex',
        col_dict[zone_type]['work']: work_col,
        col_dict[zone_type]['school']: school_col,
    }, inplace=True)

    # Create new df id by concatenating household id and pno
    df['person_id'] = df['household_id'].astype('str') + df['PNUM'].astype('str')

    df.loc[df['pwtyp'] == 1,'pemploy'] = 1
    df.loc[df['pwtyp'] == 2,'pemploy'] = 2
    df.loc[df['pwtyp'] == 0,'pemploy'] = 3
    df.loc[df['age'] < 16,'pemploy'] = 4
    df['pemploy'] = df['pemploy'].astype('int')

    # Some people make work trips but are not classified as workers
    df['pstudent'] = 3
    df.loc[df['pptyp'].isin([7,6]),'pstudent'] = 1
    df.loc[df['pptyp']==5 ,'pstudent'] = 2

    # There are some people with -1 pptyp that should be corrected for
    # Reclassify
    _filter = df['pptyp'] == -1
    df.loc[(df['pptyp'] == -1) & (df['pwtyp'] == 0) & (df['age']>=65), 'pptyp'] = 3   # Non-worker over 65
    df.loc[(df['pptyp'] == -1) & (df['pstyp'] > 0) & (df['age']>18), 'pptyp'] = 5    # university student 
    df.loc[(df['pptyp'] == -1) & (df['pstyp'] > 0) & (df['age']<=18), 'pptyp'] = 6 # high school student
    df.loc[(df['pptyp'] == -1) & (df['pwtyp'] == 0) & (df['age']<65), 'pptyp'] = 4 # non-working adult under 65

    # assign values used in activitysim
    df['ptype'] = df['pptyp']
    df.loc[df['pptyp'] == 3, 'ptype'] = 5
    df.loc[df['pptyp'] == 5, 'ptype'] = 3

    # Free parking at work
    df.loc[df['ppaidprk'] == 1, 'free_parking_at_work'] = True
    df.loc[df['ppaidprk'] < 1, 'free_parking_at_work'] = False

    # Add race from original survey file
    conn_string = "DRIVER={ODBC Driver 17 for SQL Server}; SERVER=AWS-PROD-SQL\Sockeye; DATABASE=Elmer; trusted_connection=yes"
    sql_conn = pyodbc.connect(conn_string)
    params = urllib.parse.quote_plus(conn_string)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    df_orig_survey = pd.read_sql(sql='SELECT hhid, pernum, race_category FROM HHSurvey.person_dim_2017_2019', con=engine)
    df = df.merge(df_orig_survey, left_on=['household_id','PNUM'], right_on=['hhid','pernum'], how='left')
    # Code race variables as values

    df['race'] = df['race_category'].map(race_dict)

    df_tour.rename(columns={'pstaz': school_col, 'pwtaz': work_col}, inplace=True)

    # If person makes a school tour but has no school_col field, set as tour destination zone for school tours
    # Select tours that have purpose of school and people who do not have a school zone
    #df_school = df_tour.loc[(df_tour['pdpurp'] == 2) & (df_tour[school_col]==-1)]
    df_school = df_tour[(df_tour['pdpurp'] == 2) & (df_tour['destination'] != -1)]
    if zone_type == 'TAZ':
        dest_col = 'taz'
    else:
        dest_col = 'pcl'
    df_school['updated_school_taz'] = df_school['td'+dest_col]
    
    # If MAZ, update the parcel destination to MAZ
    if zone_type == 'MAZ':
        df_school = df_school.merge(parcel_block[['parcel_id','psrc_block_id']], left_on='updated_school_taz', right_on='parcel_id', how='left')
        df_school.drop('updated_school_taz',inplace=True, axis=1)
        df_school.rename(columns={'psrc_block_id':'updated_school_taz'},inplace=True)
    if zone_type == 'parcel':
        df_school = df_school.merge(parcel_block[['parcel_id','psrc_block_id']], left_on='updated_school_taz', right_on='parcel_id', how='left')
        df_school.drop('updated_school_taz',inplace=True, axis=1)
        df_school.rename(columns={'parcel_id':'updated_school_taz'},inplace=True)
        
    # take the most frequent value for school zone (mode)
    ##df_school = df_school.groupby('person_id').agg(lambda x:x.value_counts().index[0])[['updated_school_taz']]   
    # Not working so take the first record
    df_school = df_school.groupby('person_id').first()[['updated_school_taz']]
    df_school.reset_index(inplace=True)
    
    df = df.merge(df_school, how='left',on='person_id')
    if zone_type != 'TAZ':
        df[['updated_school_taz',school_col]] = df[['updated_school_taz',school_col]].fillna(-1)
        df[['updated_school_taz',school_col]] = df[['updated_school_taz',school_col]].astype('int')
    else:
        df['updated_school_taz'] = df['updated_school_taz'].fillna(-1)
        df['updated_school_taz'] = df['updated_school_taz'].astype('int')
    df[school_col].replace(-1, df['updated_school_taz'], regex=True, inplace=True)
    

    # Similarly, workplace is missing for people who make work tours
    df_work = df_tour.loc[(df_tour['pdpurp'] == 1) & (df_tour['destination']!=-1)]
    df_work['updated_work_taz'] = df_work['td'+dest_col]

    # If MAZ, update the parcel destination to MAZ
    if zone_type == 'MAZ':
        df_work = df_work.merge(parcel_block[['parcel_id','psrc_block_id']], left_on='updated_work_taz', right_on='parcel_id', how='left')
        df_work.drop('updated_work_taz',inplace=True, axis=1)
        df_work.rename(columns={'psrc_block_id':'updated_work_taz'},inplace=True)
    if zone_type == 'parcel':
        df_work = df_work.merge(parcel_block[['parcel_id','psrc_block_id']], left_on='updated_work_taz', right_on='parcel_id', how='left')
        df_work.drop('updated_school_taz',inplace=True, axis=1)
        df_work.rename(columns={'parcel_id':'updated_work_taz'},inplace=True)

    #df_work = df_work.groupby('person_id').agg(lambda x:x.value_counts().index[0])[['updated_work_taz']]    
    df_work = df_work.groupby('person_id').first()[['updated_work_taz']]
    df_work.reset_index(inplace=True)
    
    df = df.merge(df_work, how='left',on='person_id')
    if zone_type != 'TAZ':
        df[['updated_work_taz',work_col]] = df[['updated_work_taz',work_col]].fillna(-1)
        df[['updated_work_taz',work_col]] = df[['updated_work_taz',work_col]].astype('int')
    else:
        df['updated_work_taz'] = df['updated_work_taz'].fillna(-1)
        df['updated_work_taz'] = df['updated_work_taz'].astype('int')
    #df[work_col] = df[work_col].replace(-1, df['updated_work_taz'], regex=True, inplace=True)
    df.loc[df[work_col] < 0, work_col] = df['updated_work_taz']
    df[work_col] = df[work_col].fillna(-1).astype('int')

    # If this person has no usual workplace and doesn't have work trips, remove them from the dataset?

    # Ensure that this person is coded as a student, based on age (even if they are part-time students)
    # Find where school TAZ != -1 but student type is non-student
    df['age'] = df['age'].astype('int')
    df['pstudent'] = df['pstudent'].astype('int')
    df['pemploy'] = df['pemploy'].astype('int')
    df.loc[(df[school_col] > 0) & (df['pstudent'] == 3) & (df['age'] < 18), 'pstudent'] = 1
    df.loc[(df[school_col] > 0) & (df['pstudent'] == 3) & (df['age'] >= 18), 'pstudent'] = 2

    # However, if a person is a student that does not have a school TAZ (and makes no school tours)
    # set them as a non-student for the purpose of estimation
    filter1 = df[school_col] < 0
    filter2 = df['pstudent'] != 3
    df.loc[(filter1 & filter2), 'pstudent'] = 3
    # FIXME: ideally this is handled upstream - students should be placed on their nearest relevant school
    # if no other information was provided. Or they should be tossed out completely
    # because this biases short school trips. 

    # Ensure person is coded as some type of worker
    # For now, make them part-time workers...
    df.loc[(df[work_col] > 0) & (df['pemploy'] >= 3) & (df['age'] >= 16), 'pemploy'] = 2
    # Update the person type field so it corresponds with this change
    # Person must be either a college student or a full-time worker; change these to part-time worker
    df.loc[(df['ptype'] == 3) & (df['pemploy'] == 1), 'pemploy'] = 2
    df.loc[(df['ptype'] == 4) & (df['pemploy'] == 2), 'ptype'] = 2  # record non-working adult to PT worker
    df.loc[(df['ptype'] == 5) & (df['pemploy'] == 2), 'ptype'] = 2 # retired w/part time job -> PT worker
    df.loc[(df['ptype'] == 6) & (df['pstudent'] == 3) & (df['pemploy'] == 2), 'ptype'] = 2  # coded as a student but pstudent not a student -> part time work

    # Make sure pre-school age kids are not classified as a student in pemploy if they are not students
    df.loc[(df['age'] < 16) & (df['pstudent'] == 3), 'pemploy'] = 3

    return df

def process_trip(df, template):

    # Create new person id by concatenating household id and pno
    df['person_id'] = df['hhno'].astype('str') + df['pno'].astype('str')

    df.rename(columns={
        'tour':'tour_id',
        'id':'trip_id',
        'hhno':'household_id',
    #     'dpurp':'purpose',
        'otaz':'origin',
        'dtaz':'destination',
    #     'mode':'trip_mode',
    #     'deptm': 'depart'
    }, inplace=True)
    # purpose
    #trip_template.groupby('purpose').count()[['trip_id']]
    purp_map = {
        0: 'Home',
    1: 'work',
    2:'school',
    3:'escort',
    4:'othmaint',
    5:'shopping',
    6:'eatout',
    7:'social'}

    df['purpose'] = df['dpurp'].map(purp_map)

    # assign some modes directly
    mode_map = {
        1: 'WALK',
        2: 'BIKE',
        3: 'DRIVEALONEPAY',
        4: 'SHARED2FREE',
        5: 'SHARED3FREE',
        10: 'TNC_SINGLE'
    }
    df['trip_mode'] = df['mode'].map(mode_map)

    # Assign transit submodes based on PATHTYPE field

    # all walk access assumed
    df.loc[df['pathtype']==3, 'trip_mode'] = 'WALK_LOC' # local bus, 
    df.loc[df['pathtype']==4, 'trip_mode'] = 'WALK_HVY' # Assign light rail as heavy rail (no such thing as light rail in MTC TM1)
    df.loc[df['pathtype']==6, 'trip_mode'] = 'WALK_COM'
    # Fix me!!!
    # NO FERRY MODE; use the EXP mode?
    df.loc[df['pathtype']==7, 'trip_mode'] = 'WALK_EXP'  # No ferry mode...

    # Fix me!!!
    # Drop the null trips (other and school bus) for now
    df = df[-df['trip_mode'].isnull()]

    # departure time departure hour
    df['depart'] = np.floor(df['deptm']/60)

    # outbound based on df half
    df['outbound'] = False
    df.loc[df['half']==1,'outbound'] = True

    return df


def process_tour(df, df_person, parcel_block, template, zone_type, raw_survey=True):
    # concatentate tour ID
    df['tour_id'] = df['hhno'].astype('str') + df['pno'].astype('str') + df['tour'].astype('str')
    df['person_id'] = df['hhno'].astype('str') + df['pno'].astype('str')
    df['household_id'] = df['hhno']

    # FIXME
    # We have tour data for multiple days for people
    # If they have more than one travel day, select day randomly?
    for person in df['person_id'].unique():
        _df = df[df['person_id'] == person]
        if len(_df['day'].unique()) > 1:
            # Just take the first day for now? 
            # Other options are to randomize this? Take day with average number of tours?
            keep_day = _df['day'].unique()[0]
            df = df.drop(df[(df['person_id'] == person) & (df['day'] != keep_day)].index)

    # Filter out children taking work tours
    # Incorrectly coded for trips when child is with parent. Not sure how to classify these, so removing
    # Merge to get person attributes
    if raw_survey:
        df_person['person_id'] = df_person['hhno'].astype('str') + df_person['pno'].astype('str')
        df = df.merge(df_person[['person_id','pagey']], on='person_id', how='left')
        df = df.loc[-((df['pagey'] < 16) & (df['pdpurp'] == 1))]

    # For MAZ geography, merge parcel to MAZ lookup
    if zone_type == 'MAZ':
        parcel_block = parcel_block[-parcel_block['psrc_block_id'].isnull()]
        parcel_block['psrc_block_id'] = parcel_block['psrc_block_id'].astype('int')
        # Work
        df = df.merge(parcel_block[['parcel_id','psrc_block_id']], left_on='topcl', right_on='parcel_id', how='left')
        df.rename(columns={'psrc_block_id':'tomaz'},inplace=True)
        df.drop('parcel_id', axis=1, inplace=True)
        # School
        df = df.merge(parcel_block[['parcel_id','psrc_block_id']], left_on='tdpcl', right_on='parcel_id', how='left')
        df.rename(columns={'psrc_block_id':'tdmaz'},inplace=True)

    # tour purpose type
    purp_map = {
        0: 'home',
    1: 'work',
    2:'school',
    3:'escort',
    4:'othmaint',
    5:'shopping',
    6:'eatout',
    7:'social',
    10: 'othmaint'}

    df['tour_type'] = df['pdpurp'].map(purp_map)

    # Tour category based on tour type
    df['tour_category'] = 'non_mandatory'
    df.loc[df['tour_type'].isin(['work','school']),'tour_category'] = 'mandatory'

    col_dict = {
        'TAZ':{
            'origin': 'totaz',
             'destination': 'tdtaz'},
        'MAZ':{
            'origin': 'tomaz',
            'destination': 'tdmaz',
           },
        'parcel':{
            'school': 'topcl',
            'destination': 'tdpcl'}
        }


    df.rename(columns={
        col_dict[zone_type]['origin']: 'origin',
        col_dict[zone_type]['destination']: 'destination'
    }, inplace=True)

    df['start'] = np.floor(df['tlvorig']/60)
    df['end'] = np.floor(df['tardest']/60)

    # Set start hour to time within modeled hours 
    # Group anything from midnight to 5 together 
    df.loc[(df['start'] < 5) & (df['end'] < 5), 'start'] = 4.0
    df.loc[(df['start'] < 5) & (df['end'] < 5), 'end'] = 4.0
    df.loc[(df['start'] < 5) & (df['end'] >= 5), 'start'] = 4.0
    df.loc[(df['start'] > 5) & (df['end'] < 5), 'end'] = 23.0

    # assign some modes directly
    mode_map = {
        1: 'WALK',
        2: 'BIKE',
        3: 'DRIVEALONEPAY',
        4: 'SHARED2FREE',
        5: 'SHARED3FREE',
        10: 'TNC_SINGLE'
    }
    df['tour_mode'] = df['tmodetp'].map(mode_map)

    # Assign transit submodes based on PATHTYPE field

    # all walk access assumed
    df.loc[df['tpathtp']==3, 'tour_mode'] = 'WALK_LOC' # local bus, 
    df.loc[df['tpathtp']==4, 'tour_mode'] = 'WALK_HVY' # Assign light rail as heavy rail (no such thing as light rail in MTC TM1)
    df.loc[df['tpathtp']==6, 'tour_mode'] = 'WALK_COM'
    # Fix me!!!
    # NO FERRY MODE; use the EXP mode?
    df.loc[df['tpathtp']==7, 'tour_mode'] = 'WALK_EXP'  # No ferry mode...

    # Fix me!!!
    # Drop the null trips (other and school bus) for now
    df = df[-df['tour_mode'].isnull()]

    # Need to find some way to identify joint tours
    df['parent_tour_id'] = np.nan

    # Enforce canonical tours
    # There cannot be more than 2 mandatory work tours
    # Delete all tours for people who have this issue
    group_cols = ['person_id', 'tour_category', 'tour_type']
    df['tour_type_num'] = df.sort_values(by=group_cols).groupby(group_cols).cumcount() + 1

    # Import canoncial tour list from activitysim
    possible_tours = list(pd.read_csv('canonical_tours.csv')['0'])
    possible_tours_count = len(possible_tours)
    tour_num_col = 'tour_type_num'
    df['tour_type_id'] = df.tour_type + df['tour_type_num'].map(str)
    df.tour_type_id = df.tour_type_id.replace(to_replace=possible_tours,
                                        value=list(range(possible_tours_count)))

    # Non-numeric tour_type_id results are non-canonical and should be removed. 
    # FIXME: For now just remove the offensive tours
    # We may want to remove all of this person/households records
    df = df[pd.to_numeric(df['tour_type_id'], errors='coerce').notnull()]

    # In order to estimate, we need to enforce the mandatory tour totals
    # these can only be: ['work_and_school', 'school1', 'work1', 'school2', 'work2']
    # If someone has 2 work trips and 1 school trip, must decide a heirarchy of 
    # which of those tours to delete
    # if any number of work and school, remove all but 1 one of each

    #FIXME
    # Note: for now we are just removing all tours by people that don't fit in the hierarchy
    # This removes tours for a few people, but alternatives could include selectively removing trips
    # Curretnly removes all tours for 3 people
    # to make a person-day fit the tour hierarchy (eliminate all but 1 school and work trip for isntance)

    drop_list = []
    df_mandatory = df[df['tour_category'] == 'mandatory'].groupby(['tour_type','person_id']).size().reset_index()
    for person in df_mandatory['person_id'].unique():
        _df = df_mandatory[df_mandatory['person_id'] == person]

        # If only school trips and no work trips:
        if ('work' not in _df['tour_type'].values) & ('school' in _df['tour_type'].values):
            #Case 1: more than 2 school trips and no work trips -> remove school trips to max of school2
            if (_df.loc[_df['tour_type'] == 'school',0].values[0] > 2):
                df = df.drop(df[df['person_id'] == person].index)
                drop_list.append(person)

        # If only work trips and no school trips
        #Case 2: more than 2 work trips and no school trips ->  remove work trips to max of work2
        elif ('work' in _df['tour_type'].values) & ('school' not in _df['tour_type'].values):
            if (_df.loc[_df['tour_type'] == 'work',0].values[0] > 2):
                df = df.drop(df[df['person_id'] == person].index)
                drop_list.append(person)

        elif ('work' in _df['tour_type'].values) & ('school' in _df['tour_type'].values):
        # Case 1: at least 2 school and work trips both, 
            if (_df.loc[_df['tour_type'] == 'school',0].values[0] >= 2) & (_df.loc[_df['tour_type'] == 'work',0].values[0] >= 2):
                df = df.drop(df[df['person_id'] == person].index)
                drop_list.append(person)

            # Case 4: 2 or more work trips and 1 school trip -> remove work trips to yield work_and_school (1 of each only)
            elif (_df.loc[_df['tour_type'] == 'school',0].values[0] == 1) & (_df.loc[_df['tour_type'] == 'work',0].values[0] >= 2):
                df = df.drop(df[df['person_id'] == person].index)
                drop_list.append(person)

            # Case 5: more than 2 school trips and 1 work trip -> remove school trips to yield work_and_school (1 of each only)
            elif (_df.loc[_df['tour_type'] == 'school',0].values[0] >= 2) & (_df.loc[_df['tour_type'] == 'work',0].values[0] == 1):
                df = df.drop(df[df['person_id'] == person].index)
                drop_list.append(person)

    # Drop all of the tour IDs in drop_list
    print('------------------')
    print('dropped: ' + str(len(drop_list)))

    return df

def process_joint_tour(df, template):

    # Joint Tour
    # Find the joint tours
    # each of these tours occur more than once (assuming more than 1 person is on this same tour)
    joint_tour = 1
    for index, row in _df.iterrows():
        filter = (tour.day==row.day)&(tour.pdpurp==row.pdpurp)&(tour.topcl==row.topcl)&\
                      (tour.tdpcl==row.tdpcl)&(tour.origin==row.origin)&(tour.destination==row.destination)&\
                      (tour.tmodetp==row.tmodetp)&(tour.tpathtp==row.tpathtp)&(tour.start==row.start)&\
                      (tour.end==row.end)
        # Get total number of participatns (total number of matching tours) and assign a participant number
        # NOTE: this may need to be given a heirarchy of primary tour maker?
        participants = len(tour[filter])
        tour.loc[filter,'joint_tour'] = joint_tour
        tour.loc[filter,'participant_num'] = xrange(1,participants+1)
        joint_tour += 1

    # Use the joint_tour field to identify joint tour participants
    df = tour[-tour['joint_tour'].isnull()]
    # FIXME: not sure how participant ID varies from person ID, so just duplicate that for now
    df['participant_id'] = df['person_id']
    df = df[['person_id','tour_id','household_id','participant_num','participant_id']]

    return df


results_dict = {}
template_dict = {}

# Note: trip and joint tour files not yet available for estimation; add to this list later
for table in ['household','person','tour']:
    results_dict[table] = pd.read_csv(os.path.join(survey_input_dir,'_'+table+'.tsv'), delim_whitespace=True)
    template_dict[table] = pd.read_csv(os.path.join(example_survey_dir, 'survey_'+table+'s.csv'))

# Do some clean up
# Fix this in daysim outputs
#tour_input_df = results_dict['tour']
#tour_input_df['person_id'] = tour_input_df['hhno'].astype('str') + tour_input_df['pno'].astype('str')
#results_dict['person']['person_id'] = results_dict['person']['hhno'].astype('str') + results_dict['person']['pno'].astype('str')
#tour_input_df = tour_input_df.merge(results_dict['person'], on=['hhno','pno','person_id'], how='left')

# Process results for all zone types
for zone_type in zone_type_list:
    print(zone_type)
    # if subfolder for zone type does not exist in output directory, create it
    if not os.path.exists(os.path.join(output_dir,zone_type)):
        os.makedirs(os.path.join(output_dir,zone_type))

    ####################
    # Household
    ####################
    hh = process_hh(results_dict['household'], parcel_block, zone_type)
    print(hh.columns)
    output_cols = list(template_dict['household'].columns.values)

    # The template TAZ field should be updated as "home_zone_id"
    output_cols = [x if x != 'TAZ' else 'home_zone_id' for x in output_cols]
    output_cols += ['hh_race','is_mf','hownrent','hrestype']
    #output_cols = np.append(output_cols,'hh_race')
    #output_cols = np.append(output_cols,'is_mf')
    print(output_cols)
    hh[output_cols].to_csv(os.path.join(output_dir, zone_type, 'survey_households.csv'), index=False)

    ####################
    # Tour
    ####################
    tour = process_tour(results_dict['tour'], results_dict['person'], parcel_block, template_dict['tour'], zone_type)
    tour[template_dict['tour'].columns].to_csv(os.path.join(output_dir, zone_type, 'survey_tours.csv'), index=False)

    ####################
    # Person
    ####################
    person = process_person(results_dict['person'], parcel_block, tour, zone_type)
    output_cols = template_dict['person'].columns.values
    output_cols = np.append(output_cols,'race')
    output_cols[output_cols == 'workplace_taz'] = 'workplace_zone_id'
    output_cols[output_cols == 'school_taz'] = 'school_zone_id'    # These column names are changed for some reason in estimation
    person.rename(columns={'school_taz': 'school_zone_id','workplace_taz':'workplace_zone_id'}, inplace=True)
    
    person[output_cols].to_csv(os.path.join(output_dir, zone_type, 'survey_persons.csv'), index=False)
    
    ####################
    # Trip
    ####################
    #trip = process_trip(results_dict['trip'], template_dict['trip'])
    #trip[template_dict['trip'].columns].to_csv(os.path.join(output_dir,'survey_trips.csv'), index=False)

    
    ####################
    # Joint Tour
    ####################
    #joint_tour = process_joint_tour(results_dict['joint_tour'], template_dict['joint_tour'])
    #joint_tour[template_dict['joint_tour'].columns].to_csv(os.path.join(output_dir,'survey_joint_tour_participants.csv'), index=False)