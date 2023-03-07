# This script converts raw PSRC travel survey data into Activitysim format.
# This includes geolocation to MAZs, tour creation, reformatting and data clean up.

import os, errno
import time
import numpy as np
import pandas as pd
import geopandas as gpd
import urllib
import pyodbc
import yaml
import sqlalchemy
from shapely import wkt
from scipy.spatial import cKDTree
from shapely.geometry import Point
from sqlalchemy.engine import URL
from pymssql import connect
import logging
import logcontroller
import datetime
pd.options.mode.chained_assignment = None  # default='warn'
from activitysim.abm.models.util import canonical_ids as ci
from config_activitysim import *

##############################
# Inputs and controls
##############################

# Records must be geolocated for work/school location and proper MAZ assignment
# This can be skipped for trips and households if geolocated data already available
geocode_households = False
geocode_trips = False
geocode_persons = False

# Raw survey files can be read locally instead of from PSRC database
read_survey_from_db = False

# Perform primary function of tour creation and activitysim formatting
convert_survey = True

# List of days to process and include
day_list = ['Tuesday','Wednesday','Thursday']  

# Define inputs/outputs
output_dir = r'data\survey_data'
geolocated_output = r'data\survey_conversion'

parcel_file = r'data\survey_conversion\parcels_urbansim.txt'
parcel_maz_file = r'data\survey_conversion\parcel_taz_block_lookup.csv'

# Synthetic household used to impute income if available
# Set to "None" to skip imputation
synthetic_hh_dir = r'R:\e2projects_two\activitysim\inputs\data\data_full\households.csv' 

# If not reading raw survey files directly from db, set read_survey_from_db to False and define the following paths:
survey_trip_file = r'data\survey_conversion\elmer_trip.csv'
survey_person_file = r'data\survey_conversion\elmer_person.csv'
survey_household_file = r'data\survey_conversion\elmer_hh.csv'

day_dict = {'Monday': 1,
            'Tuesday': 2,
            'Wednesday': 3,
            'Thursday': 4,
            'Friday': 5}

# Constants and variable definitions
# from constants.yaml
# FIXME: import this from activitysim run dir
PSTUDENT_GRADE_OR_HIGH = 1
PSTUDENT_UNIVERSITY = 2
PSTUDENT_NOT = 3
GRADE_SCHOOL_MAX_AGE = 14
GRADE_SCHOOL_MIN_AGE = 5

# Geoprocessing info
psrc_crs = 'EPSG:2285'

# Variable definitions
person_id_col = 'person_id'
day_col = 'daynum'
trip_id = 'trip_id' # unique trip ID
hhid = 'household_id'

# Usual school and workplace variables
home = 'Home'    # destination value for Home
school_taz ='school_zone_id'
work_taz = 'workplace_zone_id' 
school_parcel = 'school_loc_parcel'
work_parcel = 'work_parcel'
missing_school_zone = 9999999999999

# houeshold weight
home_parcel = 'final_home_parcel'
hh_weight = 'hh_weight_2017_2019'

# Departure/arrival times in minutes after midnight
deptm = 'depart_time_mam'
arrtm = 'arrival_time_mam'

# trip columns
otaz = 'origin'
dtaz = 'destination'
opcl = 'origin_parcel_dim_id'    # not used in activysim
dpcl = 'dest_parcel_dim_id'
opurp = 'origin_purpose_cat'
dpurp = 'dest_purpose_cat'
oadtyp = 'oadtyp'    # origin land use type
dadtyp = 'dadtyp'    # destination land use type
adtyp_school = 'School'
adtyp_work = 'Work'
purp_work = 'Work'
purp_home = 'Home'
purp_school = 'School'
trip_weight = 'trip_weight_2017_2019'

# tour columns
totaz = 'origin'
tdtaz = 'destination'
topcl = 'topcl'
tdpcl = 'tdpcl'
tour_id_col = 'tour_id'
toadtyp = 'toadtyp'
tdadtyp = 'tdadtyp'
work_based_subtour = 'atwork'
parent = 'parent_tour_id'
topurp = 'topurp'
tdpurp = 'tour_type'
tour_mode = 'tour_mode'

# Start log file
logger = logcontroller.setup_custom_logger('main_logger')
logger.info('--------------------NEW RUN STARTING--------------------')
start_time = datetime.datetime.now()

def load_elmer_geo_table(feature_class_name, con, crs):
    """ Load ElmerGeo table as geoDataFrame, applying a specified coordinate reference system (CRS)
    """
    geo_col_stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=" + "\'" + feature_class_name + "\'" + " AND DATA_TYPE='geometry'"
    geo_col = str(pd.read_sql(geo_col_stmt, con).iloc[0,0])
    query_string = 'SELECT *,' + geo_col + '.STGeometryN(1).ToString()' + ' FROM ' + feature_class_name

    df = pd.read_sql(query_string, con)
    df.rename(columns={'':'geometry'}, inplace = True)
    df['geometry'] = df['geometry'].apply(wkt.loads)

    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.crs = crs

    return gdf

def convert_hhmm_to_mam(x):

    if x == -1:
        mam = -1
    else:
        # Convert string of time in HH:MM AM/PM format to minutes after minute
        ampm = x.split(' ')[-1]
        hr = int(x.split(':')[0])
        if (ampm == 'PM') & (hr != 12):
            hr += 12
        min = int(x.split(':')[-1].split(' ')[0])

        mam = (hr*60) + min

    return mam

def assign_tour_mode(_df, tour_dict, tour_id, mode_heirarchy=mode_heirarchy):
    """ Get a list of transit modes and identify primary mode
        Primary mode is the first one from a heirarchy list found in the tour.   """
    mode_list = _df['mode'].unique()
    for mode in mode_heirarchy:
        if mode in mode_list:
            return mode

def find_nearest(gdA, gdB):
    """ Find nearest value between two geodataframes.
        Returns "dist" for distance between nearest points.
    """

    nA = np.array(list(gdA.geometry.apply(lambda x: (x.x, x.y))))
    nB = np.array(list(gdB.geometry.apply(lambda x: (x.x, x.y))))
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    gdB_nearest = gdB.iloc[idx].drop(columns="geometry").reset_index(drop=True)
    gdf = pd.concat(
        [
            gdA.reset_index(drop=True),
            gdB_nearest,
            pd.Series(dist, name='dist')
        ], 
        axis=1)

    return gdf

def edit_school_location(person, gdf_lu, school_segment, enrollment_col, student_type_col):

    person = person.merge(gdf_lu[['MAZ','geometry',enrollment_col]], left_on='school_zone_id', right_on='MAZ', how='left')

    person.loc[(person['school_zone_id'] > 0) & (person[enrollment_col] == 0) & 
               (person[student_type_col] == 1), 'edit_school_loc'] = 1
    df = person[person['edit_school_loc'] == 1]

    # Eligible MAZs
    school_maz_gdf = gdf_lu[gdf_lu[enrollment_col] > 0]
    nearest_df = find_nearest(df[['person_id','school_zone_id','geometry']], school_maz_gdf[['MAZ','geometry']])
    nearest_df.rename(columns={'MAZ': 'updated_school_maz'}, inplace=True)
    nearest_df[['person_id','school_zone_id','updated_school_maz','dist']].to_csv(os.path.join(output_dir,'temp',school_segment+'_location_edit.csv'))

    person = person.merge(nearest_df[['person_id','updated_school_maz']], on='person_id', how='left')
    person['updated_school_maz'] = person['updated_school_maz'].fillna(-1).astype('int')
    person.loc[person['edit_school_loc'] == 1, 'school_zone_id'] = person['updated_school_maz']
    person.drop(['MAZ','geometry',enrollment_col,'updated_school_maz','edit_school_loc'], inplace=True, axis=1)

    return person

################################
# Initialize
################################

for dirname in [output_dir, os.path.join(output_dir,'temp')]:
    if not os.path.exists(dirname ):
        os.makedirs(dirname )

# Clear any existing log files
try:
    os.remove('log.txt')
except OSError:
    pass

if read_survey_from_db:
    # Database connections for PSRC Elmer DB
    conn_string = "DRIVER={ODBC Driver 17 for SQL Server}; SERVER=AWS-PROD-SQL\Sockeye; DATABASE=Elmer; trusted_connection=yes"
    sql_conn = pyodbc.connect(conn_string)
    params = urllib.parse.quote_plus(conn_string)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

# Load parcel GDF with MAZ lookup
if geocode_households or geocode_trips or geocode_persons:
    parcels_df = pd.read_csv(parcel_file, delim_whitespace=True)
    parcels_gdf = gpd.GeoDataFrame(parcels_df, geometry=gpd.points_from_xy(parcels_df.xcoord_p, parcels_df.ycoord_p))
    parcels_gdf.crs = psrc_crs
    # Merge parcel-MAZ lookup
    parcel_maz = pd.read_csv(parcel_maz_file)
    parcels_gdf = parcels_gdf.merge(parcel_maz, left_on='parcelid', right_on='parcel_id', how='left')

###################################
# Geocode trip ends on MAZs
###################################

if geocode_trips:
    
    if read_survey_from_db:
        trip = pd.read_sql(sql='SELECT * FROM HHSurvey.v_trips WHERE survey_year IN (2017, 2019)', con=engine)
    else:
        trip = pd.read_csv(survey_trip_file)

    # Only include trips that start and end within the region
    # Use a standard TAZ file for this 
    elmergeo_conn_string = 'AWS-Prod-SQL\Sockeye'
    elmergeo_con = connect('AWS-Prod-SQL\Sockeye', database="ElmerGeo")
    taz_gdf = load_elmer_geo_table('taz2010', elmergeo_con, psrc_crs)
    taz_gdf.crs = psrc_crs

    # Filter trips without XY origins and destinaations
    _filter = (trip['origin_lat'].isnull()) | (trip['dest_lat'].isnull())
    logger.info(f'Dropped {len(trip[_filter])} trips: null lat/lng')
    trip = trip[~_filter]

    ####################
    # Origins
    ####################
    trip_o_gdf = gpd.GeoDataFrame(trip[['trip_id','origin_lat','origin_lng']],geometry=gpd.points_from_xy(trip.origin_lng, trip.origin_lat))
    trip_o_gdf.crs = 'EPSG:4326'
    trip_o_gdf = trip_o_gdf.to_crs(psrc_crs)

    # Select trip origins within the region
    prev_len = len(trip_o_gdf)
    trip_o_gdf =  gpd.overlay(trip_o_gdf, taz_gdf, how='intersection')
    logger.info(f'Dropped {prev_len - len(trip_o_gdf)} trips: origins outside region')

    # Snap to nearest parcel
    nearest_df = find_nearest(trip_o_gdf, parcels_gdf[['parcel_id','maz_id','geometry']])
    nearest_df.to_csv(os.path.join(output_dir,'temp','trip_o_maz_location.csv', index=False))

    trip = trip.merge(nearest_df[['trip_id','maz_id']], on='trip_id', how='left')
    trip.rename(columns={'maz_id': otaz}, inplace=True)
    # Remove null values
    _filter = trip[otaz].isnull()
    logger.info(f'Dropped {len(trip[_filter])} trips: null origin MAZ ')
    trip = trip[~_filter]
    trip[otaz] = trip[otaz].astype('int')

    ####################
    # Destinations
    ####################
    trip_d_gdf = gpd.GeoDataFrame(trip[['trip_id','dest_lat','dest_lng']],geometry=gpd.points_from_xy(trip.dest_lng, trip.dest_lat))
    trip_d_gdf.crs = 'EPSG:4326'
    trip_d_gdf = trip_d_gdf.to_crs(psrc_crs)

    # Select trip destinations within the region
    prev_len = len(trip_d_gdf)
    trip_d_gdf = gpd.overlay(trip_d_gdf, taz_gdf, how='intersection')
    logger.info(f'Dropped {prev_len - len(trip_d_gdf)} trips: destinations outside region')

    # Snap to nearest parcel
    nearest_df = find_nearest(trip_d_gdf, parcels_gdf[['parcel_id','maz_id','geometry']])
    nearest_df.to_csv(os.path.join(output_dir,'temp','trip_d_maz_location.csv', index=False))

    trip = trip.merge(nearest_df[['trip_id','maz_id']], on='trip_id', how='left')
    trip.rename(columns={'maz_id': dtaz}, inplace=True)
    # Remove null values
    _filter = trip[dtaz].isnull()
    logger.info(f'Dropped {len(trip[_filter])} trips: null destination MAZ ')
    trip = trip[~_filter]
    trip[dtaz] = trip[dtaz].astype('int')

    trip.to_csv(os.path.join(geolocated_output,'elmer_trip_geocoded.csv'), index=False)

#####################################
# Geocode Households on MAZs
#####################################

if geocode_households:

    if read_survey_from_db:
        hh = pd.read_sql(sql='SELECT * FROM HHSurvey.v_households WHERE survey_year IN (2017, 2019)', con=engine)  
    else:
        hh = pd.read_csv(survey_household_file)

    # Load households as geodatagrame based on provided XY locations
    hh_gdf = gpd.GeoDataFrame(hh[['household_id','final_home_lat','final_home_lng']],geometry=gpd.points_from_xy(hh.final_home_lng, hh.final_home_lat))
    hh_gdf.crs = 'EPSG:4326'
    hh_gdf = hh_gdf.to_crs(psrc_crs)

    # Snap to nearest parcel with households and get MAZ
    hh_parcels = parcels_gdf[parcels_gdf['hh_p'] > 0]
    nearest_df = find_nearest(hh_gdf, hh_parcels[['parcel_id','maz_id','geometry']])
    nearest_df.to_csv(os.path.join(output_dir,'temp','household_maz_location.csv', index=False))

    hh = hh.merge(nearest_df[['household_id','maz_id']], on='household_id', how='left')
    hh.rename(columns={'maz_id': 'home_zone_id'}, inplace=True)
    # Remove null values
    _filter = hh['home_zone_id'].isnull()
    logger.info(f'Dropped {len(hh[_filter])} households: null home MAZ ')
    hh = hh[~_filter]
    hh['home_zone_id'] = hh['home_zone_id'].astype('int')

    hh.to_csv(os.path.join(geolocated_output,'elmer_hh_geocoded.csv'), index=False)

if geocode_persons:

    # Load raw person file from database or file
    if read_survey_from_db:
       person = pd.read_sql(sql='SELECT * FROM HHSurvey.v_persons WHERE survey_year IN (2017, 2019)', con=engine)
    else:
       person = pd.read_csv(survey_person_file)

    # Geocoded trips required as an input
    trip = pd.read_csv(os.path.join(geolocated_output,'elmer_trip_geocoded.csv'))

    ######################################
    # Process Person Files
    ######################################

    # Person files need to be geolocated, but their school/work location 
    # is dependent on other calculations, so this is done as part of reformatting

    # Calculate fields using an expression file
    expr_df = pd.read_csv(r'scripts\survey_conversion\person_expr_activitysim.csv')

    for index, row in expr_df.iterrows():
        expr = 'person.loc[' + row['filter'] + ', "' + row['result_col'] + '"] = ' + str(row['result_value'])
        print(row['index'])
        exec(expr)

    # Check that all person types are filled in
    assert person['ptype'].count() == len(person)

    # Calculate initial PERNUM (person sequence in household)
    # NOTE: this must be updated after accounting for joint tours
    # Activitysim's joint tour estimation procedure requires a specific PNUM ordering that will be changed at the end of this script
    person[['person_id_str','household_id_str']] = person[['person_id','household_id']].astype('str')
    person['PNUM'] = person.apply(lambda x: x['person_id_str'].replace(x['household_id_str'], '').strip(), axis=1).astype('int')

    ########################################
    # Geolocate school location
    ########################################

    # If a person makes a school trip, make sure they are coded as a student
    school_trip_makers  = trip.loc[(trip['dest_purpose'] == "Went to school/daycare (e.g., daycare, K-12, college)")].person_id
    person.loc[person['person_id'].isin(school_trip_makers) & (person.age <= 18) , 'pstudent'] = PSTUDENT_GRADE_OR_HIGH
    person.loc[person['person_id'].isin(school_trip_makers) & (person.age > 18) , 'pstudent'] = PSTUDENT_UNIVERSITY

    # Snap school zone ID to nearest parcel/maz with enrollement by student type
    # Filter parcels for locations with students
    gradeschool_parcels = parcels_gdf[parcels_gdf['stugrd_p'] > 0]
    highschool_parcels = parcels_gdf[parcels_gdf['stuhgh_p'] > 0]
    university_parcels = parcels_gdf[parcels_gdf['stuuni_p'] > 0]

    # Define school types (borrowed from annotate_persons)
    person.loc[(person.pstudent == PSTUDENT_GRADE_OR_HIGH) & (person.age <= GRADE_SCHOOL_MAX_AGE), 'is_gradeschool'] = 1
    person.loc[(person.pstudent == PSTUDENT_GRADE_OR_HIGH) & (person.pstudent == PSTUDENT_GRADE_OR_HIGH) & (person.age > GRADE_SCHOOL_MAX_AGE), 'is_highschool'] = 1
    person.loc[(person.pstudent == PSTUDENT_UNIVERSITY), 'is_university'] = 1

    # Impute information for people not providing usual school location

    # First, look at all trip records to identify most common school trip end and set as usual location 
    person.loc[(person['is_gradeschool'] == 1) | (person['is_highschool'] == 1) | (person['is_university'] == 1), 'is_student'] = 1
    person.loc[(person['is_student'] == 1) & (person['school_loc_lat'].isnull()), 'student_missing_school_loc'] = 1
    person_list = person[person['student_missing_school_loc'] == 1].person_id
    df = trip.loc[(trip['person_id'].isin(person_list)) & (trip['dest_purpose'] == "Went to school/daycare (e.g., daycare, K-12, college)")]
    df_imputed = pd.DataFrame()

    df_imputed['imputed_school_loc_lat'] = df.groupby('person_id')['dest_lat'].agg(lambda x: pd.Series.mode(x)[0])
    df_imputed['imputed_school_loc_lng'] = df.groupby('person_id')['dest_lng'].agg(lambda x: pd.Series.mode(x)[0])
    # Merge back to person table
    person = person.merge(df_imputed, how='left', on='person_id')
    person['school_loc_lat'].fillna(person['imputed_school_loc_lat'], inplace=True)
    person['school_loc_lng'].fillna(person['imputed_school_loc_lng'], inplace=True)

    # Recalculate students missing school locations and drop persons without sufficient information
    # FIXME: change students to be different ptype if no school info instead of dropping?
    person['student_missing_school_loc'] = 0
    person.loc[(person['is_student'] == 1) & (person['school_loc_lat'].isnull()), 'student_missing_school_loc'] = 1
    logger.info(f'Dropped {len(person[person["student_missing_school_loc"] == 1])} persons for students without school location information')
    person = person[person['student_missing_school_loc'] != 1]

    _filter = person['school_loc_lat'] > 0
    person_gdf = gpd.GeoDataFrame(person[_filter][['person_id','school_loc_lat','school_loc_lng','is_gradeschool','is_highschool','is_university']],
                                  geometry=gpd.points_from_xy(person[_filter].school_loc_lng, person[_filter].school_loc_lat))
    person_gdf.crs = 'EPSG:4326'
    person_gdf = person_gdf.to_crs(psrc_crs)

    # Locate grade school locations
    gdf_gradeschool = person_gdf[person_gdf['is_gradeschool'] == 1]
    nearest_df = find_nearest(gdf_gradeschool, gradeschool_parcels[['parcel_id','maz_id','geometry']])
    nearest_df.to_csv(os.path.join(output_dir,'temp','gradeschool_maz_location.csv'), index=False)
    person = person.merge(nearest_df[['person_id','maz_id']], on='person_id', how='left')
    person.loc[person['is_gradeschool'] == 1, 'school_zone_id'] = person['maz_id']
    person.drop('maz_id', inplace=True, axis=1)

    # Locate high school locations
    gdf_highschool = person_gdf[person_gdf['is_highschool'] == 1]
    nearest_df = find_nearest(gdf_highschool, highschool_parcels[['parcel_id','maz_id','geometry']])
    nearest_df.to_csv(os.path.join(output_dir,'temp','highschool_maz_location.csv'), index=False)
    person = person.merge(nearest_df[['person_id','maz_id']], on='person_id', how='left')
    person.loc[person['is_highschool'] == 1, 'school_zone_id'] = person['maz_id']
    person.drop('maz_id', inplace=True, axis=1)

    # Locate university locations
    gdf_university = person_gdf[person_gdf['is_university'] == 1]
    nearest_df = find_nearest(gdf_university, university_parcels[['parcel_id','maz_id','geometry']])
    nearest_df.to_csv(os.path.join(output_dir,'temp','university_maz_location.csv'), index=False)
    person = person.merge(nearest_df[['person_id','maz_id']], on='person_id', how='left')
    person.loc[person['is_university'] == 1, 'school_zone_id'] = person['maz_id']
    person.drop('maz_id', inplace=True, axis=1)

    # FIXME: Daycare students; excluding for now, want to add back in?

    # If a person is not coded as a student but has a school zone, set their school zone to null
    person.loc[person['pstudent'] == 3, 'school_zone_id'] = np.nan

    ########################################
    # Geolocate Usual Workplace
    ########################################

    person.loc[(person['pemploy'].isin([1,2])) & (person['work_lat'].isnull()), 'worker_missing_work_loc'] = 1
    worker_list = person[person['worker_missing_work_loc'] == 1].person_id
    df = trip.loc[(trip['dest_purpose'].isin(['Went to work-related place (e.g., meeting, second job, delivery)',
                                           'Went to other work-related activity',
                                           'Went to primary workplace'])) & 
                  (trip['person_id'].isin(worker_list))]
    df_imputed = pd.DataFrame()

    df_imputed['imputed_work_lat'] = df.groupby('person_id')['dest_lat'].agg(lambda x: pd.Series.mode(x)[0])
    df_imputed['imputed_work_lng'] = df.groupby('person_id')['dest_lng'].agg(lambda x: pd.Series.mode(x)[0])

    # Merge back to person table
    person = person.merge(df_imputed, how='left', on='person_id')
    person['work_lat'].fillna(person['imputed_work_lat'], inplace=True)
    person['work_lng'].fillna(person['imputed_work_lng'], inplace=True)

    _filter = person['work_lat'] > 0
    person_gdf = gpd.GeoDataFrame(person[_filter][['person_id','work_lat','work_lng']],
                                  geometry=gpd.points_from_xy(person[_filter].work_lng, person[_filter].work_lat))
    person_gdf.crs = 'EPSG:4326'
    person_gdf = person_gdf.to_crs(psrc_crs)

    # Place work locations on MAZ 
    work_parcels = parcels_gdf[parcels_gdf['emptot_p'] > 0]
    nearest_df = find_nearest(person_gdf, work_parcels[['parcel_id','maz_id','geometry']])
    nearest_df.to_csv(os.path.join(output_dir,'temp','work_location.csv'), index=False)
    person = person.merge(nearest_df[['person_id','maz_id']], on='person_id', how='left')
    person.loc[person['pemploy'].isin([1,2]), 'workplace_zone_id'] = person['maz_id']
    person.drop('maz_id', inplace=True, axis=1)

    #  If someone is coded as a worker but doesn't make a work trip or have a usual location, change their worker type
    # FIXME: make sure this doesn't have any unexpected downstream effects
    person.loc[person['pemploy'].isin([1,2]) & (person['workplace_zone_id'].isnull()), 'pemploy'] = 3

    # Recalculate person type again with expression file
    expr_df = pd.read_csv(r'scripts\survey_conversion\person_expr_activitysim_2.csv')

    for index, row in expr_df.iterrows():
        expr = 'person.loc[' + row['filter'] + ', "' + row['result_col'] + '"] = ' + str(row['result_value'])
        print(row['index'])
        exec(expr)
    
    person.to_csv(os.path.join(geolocated_output,'elmer_person_geocoded.csv'), index=False)

if convert_survey:

    # Load geocoded trip, person, and household files
    hh = pd.read_csv(os.path.join(geolocated_output,'elmer_hh_geocoded.csv'))
    trip = pd.read_csv(os.path.join(geolocated_output,'elmer_trip_geocoded.csv'))
    person = pd.read_csv(os.path.join(geolocated_output,'elmer_person_geocoded.csv'))

    # Person file is recoded during geocoding; export as standard output file
    person.to_csv(os.path.join(output_dir,r'survey_persons.csv'), index=False)

    ###################################
    # Convert Household Variables
    ###################################

    expr_df = pd.read_csv(r'scripts\survey_conversion\hh_expr_activitysim.csv')

    # Merge parcel files to households to get data on single/multi-family homes
    #raw_parcels_df = pd.read_csv(parcel_file, delim_whitespace=True, usecols=['parcelid', 'sfunits', 'mfunits']) 
    #hh = hh.merge(raw_parcels_df, left_on=home_parcel, right_on='parcelid')

    for index, row in expr_df.iterrows():
        expr = 'hh.loc[' + row['filter'] + ', "' + row['result_col'] + '"] = ' + str(row['result_value'])
        print(row['index'])
        exec(expr)

    # Calculate the total number of people in each person_type
    person_type_field = 'ptype'
    hhid_col = 'household_id'
    for person_type in person[person_type_field].unique():
        print(person_type_dict[person_type])
        df = person[person['ptype'] == person_type]
        df = df.groupby('household_id').count().reset_index()[[hh_weight,hhid_col]]
        df.rename(columns={hh_weight: person_type_dict[person_type]}, inplace=True)
    
         # Join to households
        hh = pd.merge(hh, df, how='left', on=hhid_col)
        hh[person_type_dict[person_type]].fillna(0, inplace=True)
        hh[person_type_dict[person_type]] = hh[person_type_dict[person_type]].astype('int')

    # Impute household income 
    # Use synthetic household data if available, aggregate by PUMA and number of workers
    # Otherwise use survey median
    # FIXME: consider using smaller geographies and other constraints, or a model
    if synthetic_hh_dir is not None:
        syn_hh_df = pd.read_csv(synthetic_hh_dir, usecols=['workers','income','PUMA5'])
        syn_hh_df = syn_hh_df.groupby(['PUMA5','workers']).median()[['income']].reset_index()
        syn_hh_df.rename(columns={'income': 'imputed_income'}, inplace=True)
        hh = hh.merge(syn_hh_df, how='left', left_on=['final_home_puma10','numworkers'],
                 right_on=['PUMA5','workers'])
        # Fill -1 values with imputed income
        hh.loc[hh['income'] == -1, 'income'] = hh['imputed_income']
    else:
        hh.loc[hh['income'] == -1, 'income'] = hh[hh.imputed_income >= 0].imputed_income.median()

    #########################################
    # Trip
    #########################################

    expr_df = pd.read_csv(r'scripts\survey_conversion\trip_expr_activitysim.csv')

    # Merge some required values from the person file
    trip = trip.merge(person[[person_id_col,school_taz,work_taz]], how='left', on=person_id_col)

    for index, row in expr_df.iterrows():
        expr = 'trip.loc[' + row['filter'] + ', "' + row['result_col'] + '"] = ' + str(row['result_value'])
        print(row['index'])
        exec(expr)

    # This trip file serves as input for the tour file and will be updated with tour information
    # After both the tour file is created and the trip file is updated they will be written to file

    # Filter out any change_mode trips

    filter = trip['purpose'] == 'change_mode'
    logger.info(f"Dropped {len(trip[filter])} trips with purpose 'Change mode'")
    trip = trip[~filter]
    # FIXME: perform trip linking here to combine the adjacent trips instead of deleting...

    filter = trip['purpose'].isnull()
    logger.info(f"Dropped {len(trip[filter])} trips with null purpose")
    trip = trip[~filter]

    # Remove any intrazonal trips (in the same MAZ)
    filter = (trip['origin'] == trip['destination'])
    logger.info(f"Dropped {len(trip[filter])} trips with same origin and destination MAZ")
    trip = trip[~filter]

    ######################################
    # Create Multi-Day Households
    ######################################
    # Since ActivitySim estimation only processes single days of estimation data, we have to 
    # create "additional" households to represent trips on different days. 
    # This is done by creating a new household and person ID for separate travel days.

    trip['original_household_id'] = trip['household_id'].copy()
    trip['original_person_id'] = trip['person_id'].copy()
    hh['original_household_id'] = hh['household_id'].copy()
    hh['flag'] = 0
    person['original_household_id'] = person['household_id'].copy()
    person['original_person_id'] = person['person_id'].copy()
    person_copy = person.copy()
    person = pd.DataFrame()

    for day in day_list:
        trip.loc[trip['dayofweek'] == day, 'household_id'] = trip['original_household_id'].astype('str') + '_' + str(day_dict[day])

        hh_day = hh[hh['household_id'].isin(trip.loc[trip['dayofweek'] == day, 'original_household_id'])].copy()
        hh_day['household_id'] = hh_day['original_household_id'].astype('str') + '_' + str(day_dict[day])
        hh_day['flag'] = 1
        hh = hh.append(hh_day)

        person_day = person_copy[person_copy['original_person_id'].isin(trip[trip['dayofweek'] == day]['person_id'])]
        person_day['household_id'] = person_day['original_household_id'].astype('str') + '_' + str(day_dict[day])
        person_day['person_id'] = person_day['original_person_id'].astype('str') + '_' + str(day_dict[day])
        person_day['flag'] = 1
        person = person.append(person_day)

        trip.loc[trip['dayofweek'] == day, 'person_id'] = trip['original_person_id'].astype('str') + '_' + str(day_dict[day])

    hh = hh[hh['flag'] == 1]
    trip = trip[trip['dayofweek'].isin(day_list)]

    person.to_csv(os.path.join(output_dir,'survey_persons.csv'), index=False)
    hh.to_csv(os.path.join(output_dir, 'survey_households.csv'), index=False)

    ######################################
    # Tour
    ######################################

    # Remove trips from anyone not available in the latest person file (e.g., people removed for missing school locations)
    # FIXME: try to find ways to retain these people but exclude them for location choice models only so we can retain their travel data
    logger.info(f"Dropped {len(trip[~trip['person_id'].isin(person['person_id'])])} trips because person was removed from data")
    trip = trip[trip['person_id'].isin(person['person_id'])]

    bad_trips = ()

    # Filter trips and make note of why they were excluded
    # FIXME: send these to the log file
    def flag_trips(df, bad_trips, msg):

        for i in df[trip_id].to_list():
            bad_trips +=(msg, i)

        return bad_trips

    # Filter out trips that have the same origin and destination of home
    filter = ((trip[opurp] == trip[dpurp]) & (trip[opurp] == home))
    if len(trip[filter]) > 0:
        bad_trips = flag_trips(trip[filter], bad_trips, 'trips have the same origin and destination of home')
        trip = trip[~filter]

    filter = ~trip[opurp].isin(purpose_map.keys())
    if len(trip[filter]) > 0:
        bad_trips = flag_trips(trip[filter], bad_trips, 'missing trip origin purpose')
        trip = trip[~filter]

    filter = ~trip[dpurp].isin(purpose_map.keys())
    if len(trip[filter]) > 0:
        bad_trips = flag_trips(trip[filter], bad_trips, 'missing trip destination purpose')
        trip = trip[~filter]

    # FIXME: some trips have odd departure and arrival times, probably an issue in scheduling models

    tour_dict = {}
    tour_id = 1
    counter = 0

    for personid in trip[person_id_col].unique():
    #for personid in [17100652]:
        print(counter)
        counter += 1 
        #person_df = trip.loc[trip[person_id_col] == personid]
        df = trip.loc[trip[person_id_col] == personid]
        # Loop through each day
        #for day in person_df[day_col].unique():

            #df = person_df.loc[person_df[day_col] == day]

        # First O and last D of person's travel day should be home
        if (df.groupby(person_id_col).first()[opurp].values[0] != 'Home') or df.groupby(person_id_col).last()[dpurp].values[0] != 'Home':
        #    # Flag this set
            for i in df[trip_id].to_list():
                bad_trips += ('travel day does not start or end at home', i)
            continue

        # Identify home-based tours 
        # These will be used as the bookends for creating tours and subtours
        home_tours_start = df[df[opurp] == home]
        home_tours_end = df[df[dpurp] == home]

        ## skip person if they have a different number of tour starts/ends at home
        if len(home_tours_start) != len(home_tours_end):
            for i in df[trip_id].to_list():
                bad_trips += ('different number of tour starts/ends at home', i)
            continue
        
        # Loop through each set of home-based tours
        # These trips will be scanned for any subtours and assigned trip components
        for tour_start_index in range(len(home_tours_start)):

            tour_dict[tour_id] = {}       
    
            # start/end row for this set
            start_row_id = home_tours_start.index[tour_start_index]
            end_row_id = home_tours_end.index[tour_start_index]

            # Tterate between the start row id and the end row id to build the tour
            # Select slice of trips that correspond to a trip set
            _df = df.loc[start_row_id:end_row_id]

            # calculate duration at location, as difference between arrival at a place and start of next trip
            _df.loc[:,'duration'] = _df.shift(-1).iloc[:-1][deptm]-_df.iloc[:-1][arrtm]

            # First row contains origin information for the primary tour
            tour_dict[tour_id]['tlvorig'] = _df.iloc[0][deptm]            # Time leaving origin
            tour_dict[tour_id][totaz] = _df.iloc[0][otaz]                 # Tour origin TAZ
            tour_dict[tour_id][topcl] = _df.iloc[0][opcl]                 # Tour origin parcel
            tour_dict[tour_id][toadtyp] = _df.iloc[0][oadtyp]             # Tour origin address type

            # Last row contains return information
            tour_dict[tour_id]['tarorig'] = _df.iloc[-1][arrtm]           # Tour arrive time at origin (return time)

            # Household and person info
            tour_dict[tour_id][hhid] = _df.iloc[0][hhid]
            tour_dict[tour_id][person_id_col] = _df.iloc[0][person_id_col]
            tour_dict[tour_id]['day'] = day

            # For sets with only 2 trips, the halves are simply the first and second trips
            if len(_df) == 2:

                # ----- Generate Tour Record -----
                # Apply standard rules for 2-leg tours
                tour_dict[tour_id][tdpurp] = _df.iloc[0]['purpose']
                tour_dict[tour_id]['tripsh1'] = 1
                tour_dict[tour_id]['tripsh2'] = 1
                tour_dict[tour_id][tdadtyp] =  _df.iloc[0][dadtyp]
                tour_dict[tour_id][toadtyp] =  _df.iloc[0][oadtyp]
                tour_dict[tour_id][tdtaz] = _df.iloc[0][dtaz]
                tour_dict[tour_id][tdpcl] = _df.iloc[0][dpcl]
                tour_dict[tour_id]['tardest'] = _df.iloc[0][arrtm]
                tour_dict[tour_id]['tlvdest'] = _df.iloc[-1][deptm]
                tour_dict[tour_id]['tarorig'] = _df.iloc[-1][arrtm]
                tour_dict[tour_id][parent] = 0    # No subtours for 2-leg trips
                tour_dict[tour_id]['subtrs'] = 0    # No subtours for 2-leg trips
                tour_dict[tour_id][tour_id_col] = tour_id

                # ----- Update Related Trip Record -----
                # Set tour half and tseg within half tour for trips
                # For tour with only two records, there will always be two halves with tseg=1 for both
                trip.loc[trip[trip_id] == _df.iloc[0][trip_id], 'half'] = 1
                trip.loc[trip[trip_id] == _df.iloc[-1][trip_id], 'half'] = 2
                trip.loc[trip[trip_id].isin(_df[trip_id]),'tseg'] = 1
                tour_dict[tour_id][tour_mode] = assign_tour_mode(_df, tour_dict, tour_id)
                trip.loc[trip[trip_id].isin(_df[trip_id].values),'tour'] = tour_id

                # Done with this tour; increment tour IDs
                tour_id += 1

            # For tour groups with > 2 trips, calculate primary purpose and halves; first deal with subtours
            
            else: 
                # Could be dealing with work-based subtours...
                # Subtours exist if tour contain destinations at usual workplace more than 2 times
                # Minimum trips required for a subtour is 4 (2 legs to/from home and 2 legs to/from work for the subtour)
                if (len(_df) >= 4) & (len(_df[_df[oadtyp] == adtyp_work]) >= 2) & (len(_df[_df[opurp] == purp_work]) >= 2) & \
                    (len(_df[(_df[oadtyp] == adtyp_work) & (~_df[dadtyp].isin([purp_work,purp_home]))]) >= 1):

                    subtour_index_start_values = _df[(((_df[oadtyp] == adtyp_work) & (~_df[dadtyp].isin([purp_work,purp_home]))) | 
                                                        ((_df[opurp] == purp_work) & (~_df[dpurp].isin([purp_work,purp_home]))))].index.values

                    print('processing subtour ---------------')
                    subtours_df = pd.DataFrame()

                    # Loop through each potential subtour
                    # The following trips must eventually return to work for this to qualify as a subtour
                    # Subtour ID will start as one index higher than the parent tour
                    subtour_count = 0

                    parent_tour_id = tour_id

                    for subtour_start_value in subtour_index_start_values:

                        # Potential subtour; loop through the index from subtour start
                        next_row_index_start = np.where(_df.index.values == subtour_start_value)[0][0]+1
                        for i in _df.index.values[next_row_index_start:]:
                            next_row = _df.loc[i]
                            if next_row[dadtyp] == adtyp_work:    # Assuming we only have work-based subtours

                                tour_id += 1

                                subtour_df = _df.loc[subtour_start_value:i]

                                tour_dict[tour_id] = {}

                                # Create a new tour record for the subtour
                                subtour_df['tour_id'] = tour_id     # need a unique ID
                                #subtours_df = subtours_df.append(subtour_df)
                                subtours_df = pd.concat([subtours_df, subtour_df])

                                # add this as a tour
                                tour_dict[tour_id][tour_id_col] = tour_id
                                tour_dict[tour_id][hhid] = subtour_df.iloc[0][hhid]
                                tour_dict[tour_id][person_id_col] = subtour_df.iloc[0][person_id_col]
                                tour_dict[tour_id]['day'] = day
                                tour_dict[tour_id]['tlvorig'] = subtour_df.iloc[0][deptm]
                                tour_dict[tour_id]['tarorig'] = subtour_df.iloc[-1][arrtm]
                                tour_dict[tour_id][totaz] = subtour_df.iloc[0][otaz]
                                tour_dict[tour_id][topcl] = subtour_df.iloc[0][opcl]
                                tour_dict[tour_id][toadtyp] = subtour_df.iloc[0][oadtyp]
                                tour_dict[tour_id][parent] = parent_tour_id    # Parent is the main tour ID
                                tour_dict[tour_id]['subtrs'] = 0    # No subtours for subtours
                                tour_dict[tour_id][tdpurp] = work_based_subtour

                                trip.loc[trip[trip_id].isin(subtour_df[trip_id].values),'tour'] = tour_id

                                if len(subtour_df) == 2:

                                    tour_dict[tour_id][tdpurp] = subtour_df.iloc[0]['purpose']
                                    tour_dict[tour_id]['tripsh1'] = 1
                                    tour_dict[tour_id]['tripsh2'] = 1
                                    tour_dict[tour_id][tdadtyp] =  subtour_df.iloc[0][dadtyp]
                                    tour_dict[tour_id][toadtyp] =  subtour_df.iloc[0][oadtyp]
                                    tour_dict[tour_id]['tdtaz'] = subtour_df.iloc[0][dtaz]
                                    tour_dict[tour_id][tdpcl] = subtour_df.iloc[0][dpcl]
                                    tour_dict[tour_id]['tlvdest'] = subtour_df.iloc[-1][deptm]
                                    tour_dict[tour_id]['tardest'] = subtour_df.iloc[0][arrtm]
                                    tour_dict[tour_id][tour_mode] = assign_tour_mode(subtour_df, tour_dict, tour_id)

                                    # Set tour half and tseg within half tour for trips
                                    # for tour with only two records, there will always be two halves with tseg=1 for both
                                    trip.loc[trip[trip_id] == subtour_df.iloc[0][trip_id], 'half'] = 1
                                    trip.loc[trip[trip_id] == subtour_df.iloc[-1][trip_id], 'half'] = 2
                                    trip.loc[trip[trip_id].isin(_df[trip_id]),'tseg'] = 1

                                # If subtour length > 2, find the primary purpose/destination
                                else:
                                    subtour_df['duration'] = subtour_df.shift(-1).iloc[:-1][deptm]-subtour_df.iloc[:-1][arrtm]
                                    # Assume location with longest time spent at location (duration) is main subtour purpose
                                    primary_subtour_purp_index = subtour_df[subtour_df[dpurp]!='Change mode']['duration'].idxmax()
                                    tour_dict[tour_id][tdpurp] = subtour_df.loc[primary_subtour_purp_index]['purpose']

                                    # Get subtour data based on the primary destination trip
                                    # We know the tour destination parcel/TAZ field from that primary trip, as well as destination type
                                    tour_dict[tour_id][tdtaz] = subtour_df.loc[primary_subtour_purp_index][dtaz]
                                    tour_dict[tour_id][tdpcl] = subtour_df.loc[primary_subtour_purp_index][dpcl]
                                    tour_dict[tour_id][tdadtyp] = subtour_df.loc[primary_subtour_purp_index][dadtyp]

                                    # Calculate tour halves
                                    tour_dict[tour_id]['tripsh1'] = len(subtour_df.loc[0:primary_subtour_purp_index])
                                    tour_dict[tour_id]['tripsh2'] = len(subtour_df.loc[primary_subtour_purp_index+1:])

                                    # Set tour halves on trip records
                                    trip.loc[trip[trip_id].isin(subtour_df.loc[0:primary_subtour_purp_index].trip_id),'half'] = 1
                                    trip.loc[trip[trip_id].isin(subtour_df.loc[primary_subtour_purp_index+1:].trip_id),'half'] = 2

                                    # set trip segment within half tours
                                    trip.loc[trip[trip_id].isin(subtour_df.loc[0:primary_subtour_purp_index].trip_id),'tseg'] = range(1,len(subtour_df.loc[0:primary_subtour_purp_index])+1)
                                    trip.loc[trip[trip_id].isin(subtour_df.loc[primary_subtour_purp_index+1:].trip_id),'tseg'] = range(1,len(subtour_df.loc[primary_subtour_purp_index+1:])+1)

                                    # Departure/arrival times
                                    tour_dict[tour_id]['tlvdest'] = subtour_df.loc[primary_subtour_purp_index][deptm]
                                    tour_dict[tour_id]['tardest'] = subtour_df.loc[primary_subtour_purp_index][arrtm]
                                        
                                    tour_dict[tour_id][tour_mode] = assign_tour_mode(subtour_df, tour_dict, tour_id)

                                # Done with this subtour 
                                subtour_count += 1
                                break
                            else:
                                continue
                        
                    if len(subtours_df) < 1:
                        # No subtours found
                        # FIXME: make this a function, because it's called multiple times
                        tour_dict[tour_id]['subtrs'] = 0
                        tour_dict[tour_id][parent] = 0
                        tour_dict[tour_id][tour_id_col] = tour_id

                        # Identify the primary purpose
                        #primary_purp_index = _df[-_df['purpose'].isin(full_purpose_map.values())]['duration'].idxmax()
                        primary_purp_index = _df['duration'].idxmax()

                        tour_dict[tour_id][tdpurp] = _df.loc[primary_purp_index]['purpose']
                        tour_dict[tour_id]['tlvdest'] = _df.loc[primary_purp_index][deptm]
                        tour_dict[tour_id][tdtaz] = _df.loc[primary_purp_index][dtaz]
                        tour_dict[tour_id][tdpcl] = _df.loc[primary_purp_index][dpcl]
                        tour_dict[tour_id][tdadtyp] = _df.loc[primary_purp_index][dadtyp]

                        tour_dict[tour_id]['tardest'] = _df.iloc[-1][arrtm]
                   
                        tour_dict[tour_id]['tripsh1'] = len(_df.loc[0:primary_purp_index])
                        tour_dict[tour_id]['tripsh2'] = len(_df.loc[primary_purp_index+1:])

                        # Set tour halves on trip records
                        trip.loc[trip[trip_id].isin(_df.loc[0:primary_purp_index].trip_id),'half'] = 1
                        trip.loc[trip[trip_id].isin(_df.loc[primary_purp_index+1:].trip_id),'half'] = 2

                        # set trip segment within half tours
                        trip.loc[trip[trip_id].isin(_df.loc[0:primary_purp_index].trip_id),'tseg'] = range(1,len(_df.loc[0:primary_purp_index])+1)
                        trip.loc[trip[trip_id].isin(_df.loc[primary_purp_index+1:].trip_id),'tseg'] = range(1,len(_df.loc[primary_purp_index+1:])+1)

                        trip.loc[trip[trip_id].isin(_df[trip_id].values),'tour'] = tour_id

                        # Extract main mode 
                        tour_dict[tour_id][tour_mode] = assign_tour_mode(_df, tour_dict, tour_id)  
                        
                        tour_id += 1
                        
                    else:
                        # The main tour destination arrival will be the trip before subtours
                        # the main tour destination departure will be the trip after subtours
                        # trip when they arrive to work -> always the previous trip before subtours_df index begins

                        # Modify the parent tour results
                        main_tour_start_index = _df.index.values[np.where(_df.index.values == subtours_df.index[0])[0][0]-1]   
                        # trip when leave work -> always the next trip after the end of the subtours_df
                        main_tour_end_index = _df.index.values[np.where(_df.index.values == subtours_df.index[-1])[0][0]+1]    
                        # If there were subtours, this is a work tour
                        tour_dict[parent_tour_id][tdpurp] = 'Work'
                        tour_dict[parent_tour_id][tdtaz] = _df.loc[main_tour_start_index][dtaz]
                        tour_dict[parent_tour_id][tdpcl] = _df.loc[main_tour_start_index][dpcl]
                        tour_dict[parent_tour_id][tdadtyp] = _df.loc[main_tour_start_index][dadtyp]

                        # Pathtype is defined by a heirarchy, where highest number is chosen first
                        # Ferry > Commuter rail > Light Rail > Bus > Auto Network
                        # Note that tour pathtype is different from trip path type (?)
                        subtours_excluded_df = pd.concat([df.loc[start_row_id:main_tour_start_index], df.loc[main_tour_end_index:end_row_id]])

                        # Calculate tour halves, etc
                        tour_dict[parent_tour_id]['tripsh1'] = len(_df.loc[0:main_tour_start_index])
                        tour_dict[parent_tour_id]['tripsh2'] = len(_df.loc[main_tour_end_index:])

                        # Set tour halves on trip records
                        trip.loc[trip[trip_id].isin(_df.loc[0:main_tour_start_index].trip_id),'half'] = 1
                        trip.loc[trip[trip_id].isin(_df.loc[main_tour_end_index:].trip_id),'half'] = 2

                        # set trip segment within half tours
                        trip.loc[trip[trip_id].isin(_df.loc[0:main_tour_start_index].trip_id),'tseg'] = range(1,len(_df.loc[0:main_tour_start_index])+1)
                        trip.loc[trip[trip_id].isin(_df.loc[main_tour_end_index:].trip_id),'tseg'] = range(1,len(_df.loc[main_tour_end_index:])+1)

                        # Departure/arrival times
                        tour_dict[parent_tour_id]['tlvdest'] = _df.loc[main_tour_end_index][deptm]
                        tour_dict[parent_tour_id]['tardest'] = _df.loc[main_tour_start_index][arrtm]

                        # ID and Number of subtours 
                        tour_dict[parent_tour_id]['tour_id'] = parent_tour_id
                        tour_dict[parent_tour_id]['subtrs'] = subtour_count
                        tour_dict[parent_tour_id][parent] = 0

                        # Mode
                        tour_dict[parent_tour_id][tour_mode] = assign_tour_mode(_df, tour_dict, tour_id)
                        
                        # add tour ID to the trip records (for trips not in the subtour_df)
                        df_unique_no_subtours = [i for i in _df[trip_id].values if i not in subtours_df[trip_id].values]
                        df_unique_no_subtours = _df[_df[trip_id].isin(df_unique_no_subtours)]
                        trip.loc[trip[trip_id].isin(df_unique_no_subtours[trip_id].values),'tour'] = parent_tour_id

                        tour_id += 1
                else:
                    # No subtours
                    tour_dict[tour_id]['subtrs'] = 0
                    tour_dict[tour_id][parent] = 0
                    tour_dict[tour_id]['tour_id'] = tour_id

                    # Identify the primary purpose
                    # FIXME: need to find the primary purpose here
                    #primary_purp_index = _df[-_df[dpurp].isin([0,10])]['duration'].idxmax()
                    primary_purp_index = _df['duration'].idxmax()

                    tour_dict[tour_id][tdpurp] = _df.loc[primary_purp_index]['purpose']
                    tour_dict[tour_id]['tlvdest'] = _df.loc[primary_purp_index][deptm]
                    tour_dict[tour_id][tdtaz] = _df.loc[primary_purp_index][dtaz]
                    tour_dict[tour_id][tdpcl] = _df.loc[primary_purp_index][dpcl]
                    tour_dict[tour_id][tdadtyp] = _df.loc[primary_purp_index][dadtyp]

                    tour_dict[tour_id]['tardest'] = _df.iloc[-1][arrtm]
                   
                    tour_dict[tour_id]['tripsh1'] = len(_df.loc[0:primary_purp_index])
                    tour_dict[tour_id]['tripsh2'] = len(_df.loc[primary_purp_index+1:])

                    # Set tour halves on trip records
                    trip.loc[trip[trip_id].isin(_df.loc[0:primary_purp_index].trip_id),'half'] = 1
                    trip.loc[trip[trip_id].isin(_df.loc[primary_purp_index+1:].trip_id),'half'] = 2

                    # set trip segment within half tours
                    trip.loc[trip[trip_id].isin(_df.loc[0:primary_purp_index].trip_id),'tseg'] = range(1,len(_df.loc[0:primary_purp_index])+1)
                    trip.loc[trip[trip_id].isin(_df.loc[primary_purp_index+1:].trip_id),'tseg'] = range(1,len(_df.loc[primary_purp_index+1:])+1)

                    trip.loc[trip[trip_id].isin(_df[trip_id].values),'tour'] = tour_id

                    # Extract main mode 
                    tour_dict[tour_id][tour_mode] = assign_tour_mode(_df, tour_dict, tour_id)                

                    tour_id += 1
                            

    tour = pd.DataFrame.from_dict(tour_dict, orient='index')

    tour.value_counts().to_csv(os.path.join(output_dir, 'temp', 'bad_trip_report.csv'))

    # Tour category based on tour type
    #tour['tour_type'] = tour['tour_type'].map(purpose_map).map(str)

    tour['tour_category'] = 'non_mandatory'
    tour.loc[tour['tour_type'].isin(['work','school']),'tour_category'] = 'mandatory'

    # Drop any tour with -1 as primary purpose
    _filter = tour['tour_type'] == -1
    logger.info(f"Dropped {len(tour[_filter])} tours due to -1 primary purpose")
    tour = tour[~_filter]

    tour.to_csv(os.path.join(output_dir, 'survey_tours.csv'), index=False)
    #tour = pd.read_csv(os.path.join(output_dir, 'survey_tours.csv'))

    ###########################################
    # Joint Tour Processing
    ###########################################

    expr_df = pd.read_csv(r'scripts\survey_conversion\joint_tour_expr_activitysim.csv')

    for index, row in expr_df.iterrows():
        expr = 'tour.loc[' + row['filter'] + ', "' + row['result_col'] + '"] = ' + str(row['result_value'])
        print(row['index'])

        exec(expr)

    # After we've set begin and end hours, make sure all tour ends are after beginings
    _filter = tour['end'] >= tour['start']
    logger.info(f'Dropped {len(tour[~_filter])} tours: tour end < tour start time')
    tour = tour[_filter]

    # Enforce canonical tours - there cannot be more than 2 mandatory work tours
    # Flag mandatory vs non-mandatory to trips by purpose (and include joint non-mandatory trips)
    # Borrowing procedure from infer.py
    tour['mandatory_status'] = tour['tour_category'].copy()
    tour.loc[tour['mandatory_status'] == 'joint', 'mandatory_status'] = 'non_mandatory'
    group_cols = ['person_id', 'mandatory_status', 'tour_type']
    tour['tour_type_num'] = tour.sort_values(by=group_cols).groupby(group_cols).cumcount() + 1
    tour = tour.sort_values(['person_id','day','tour_category','tour_type','tlvorig'])

    possible_tours = ci.canonical_tours()
    possible_tours_count = len(possible_tours)
    tour_num_col = 'tour_type_num'
    tour['tour_type_id'] = tour.tour_type + tour['tour_type_num'].map(str)
    tour.tour_type_id = tour.tour_type_id.replace(to_replace=possible_tours,
                                        value=list(range(possible_tours_count)))
    tour['loc_tour_id'] = tour.tour_type + tour[tour_num_col].map(str)

    # Non-numeric tour_type_id results are non-canonical and should be removed. 
    # FIXME: For now just remove the offensive tours; is it okay to continue using this person's day records otherwise?
    filter = pd.to_numeric(tour['tour_type_id'], errors='coerce').notnull()

    # Keep track of the records we removed
    tour[~filter].to_csv(os.path.join(output_dir,'temp','tours_removed_non_canoncial.csv'))
    tour = tour[filter]

    # Merge person number in household (PNUM) onto tour file
    tour = tour.merge(person[['person_id','PNUM']], on='person_id', how='left')

    # Identify joint tours from tour df
    # Each of these tours occur more than once in the data (assuming more than 1 person is on this same tour in the survey)
    joint_tour = 1
    for index, row in tour.iterrows():
        print(row.tour_id)
        filter = (tour.day==row.day)&(tour.tour_type==row.tour_type)&(tour.topcl==row.topcl)&\
                        (tour.tdpcl==row.tdpcl)&(tour.topcl==row.topcl)&(tour.tdpcl==row.tdpcl)&\
                        (tour.tour_mode==row.tour_mode)&(tour.start==row.start)&\
                        (tour.end==row.end)&(tour.household_id==row.household_id)&\
                        (tour.person_id!=row.person_id)
                        # exclude all school, work, and escort tours per activiysim tour definitions
        # Get total number of participants (total number of matching tours) and assign a participant number
        # NOTE: this may need to be given a heirarchy of primary tour maker?
        participants = len(tour[filter])
        tour.loc[filter,'joint_tour'] = joint_tour
        tour.loc[filter,'participant_num'] = tour['PNUM']
        joint_tour += 1

    tour['participant_num'] = tour['participant_num'].fillna(0).astype('int')
    # Use the joint_tour field to identify joint tour participants
    # Output should be a list of people on each tour; use the tour ID of participant_num == 1
    joint_tour_list = tour[tour['joint_tour'].duplicated()]['joint_tour'].values
    df = tour[((tour['joint_tour'].isin(joint_tour_list)) & (~tour['joint_tour'].isnull()))]

    # Drop any tours that are for work, school, or escort
    df = df[~df['tour_type'].isin(['Work','School','Escort'])]
    joint_tour_list = df[df['joint_tour'].duplicated()]['joint_tour'].values

    # Assume Tour ID of first participant, so sort by joint_tour and person ID
    df = df.sort_values(['joint_tour','person_id'])
    tour = tour.sort_values(['joint_tour','person_id'])
    for joint_tour in joint_tour_list:
        df.loc[df['joint_tour'] == joint_tour,'tour_id'] = df[df['joint_tour'] == joint_tour].iloc[0]['tour_id']
        # Remove other tours except the primary tour from tour file completely;
        # These will only be accounted for in the joint_tour_file
        tour = tour[~tour['tour_id'].isin(tour[tour['joint_tour'] == joint_tour].iloc[1:]['tour_id'])]
        # Set this tour as joint category
        tour.loc[tour['joint_tour'] == joint_tour,'tour_category'] = 'joint'

    # Define participant ID as tour ID + participant num
    df['participant_id'] = df['tour_id'].astype('str') + df['participant_num'].astype('int').astype('str')

    df = df[['person_id','tour_id','household_id','participant_num','participant_id']]
    #df[SURVEY_TOUR_ID] = df['tour_id'].copy()
    df.to_csv(os.path.join(output_dir, 'survey_joint_tour_participants.csv'), index=False)

    ## Filter to remove any joint work mandatory trips
    # FIXME: do not remove all trips, just those of the additional person and modify to be non-joint
    tour = tour[~((tour['tour_type'].isin(['school','work','escort'])) & (tour['tour_category'] == 'joint'))]
    tour.to_csv(os.path.join(output_dir, 'survey_tours.csv'), index=False)

    # These must be added to trip after tour info is available
    trip['outbound'] = False
    trip.loc[trip['half']==1,'outbound'] = True

    trip['trip_num'] = trip['tseg'].copy()
    trip.rename(columns={'tour': 'tour_id'}, inplace=True)

    ########################################
    # Day
    ########################################

    # In order to estimate, we need to enforce the mandatory tour totals
    # these can only be: ['work_and_school', 'school1', 'work1', 'school2', 'work2']
    # If someone has 2 work trips and 1 school trips, must decide a heirarchy of 
    # which of those tours to delete

    # FIXME: how do we handle people with too many mandatory tours? 
    # Do we completely ignore all of this person’s tours, select the first tours,
    # or use some other logic to identify the primary set of tours and combinations?

    person_day = tour.groupby('person_id').agg(['unique'])['loc_tour_id']

    person_day['flag'] = 0

    # Flag any trips that have 3 or more work or school tours

    # Flag 1: person days that have 2 work and 2 school tours
    filter = person_day['unique'].apply(lambda x: 'work2'  in x and 'school2' in x)
    person_day.loc[filter, 'flag'] = 1
    # Resolve by: dropping all work2 and school2 tours (?) FIXME...
    tour = tour[~((tour['person_id'].isin(person_day[person_day['flag'] == 1].index)) & 
                tour['tour_id'].isin(['work2','school2']))]

    # Flag 2: 2 work tours and 1 school tour
    filter = person_day['unique'].apply(lambda x: 'work2' in x and 'school1' in x)
    person_day.loc[filter, 'flag'] = 2
    # Resolve by: dropping all work2 tours  (?) FIXME...
    tour = tour[~((tour['person_id'].isin(person_day[person_day['flag'] == 2].index)) & 
                (tour['tour_id']=='work2'))]

    # Flag 3: 2 school tours and 1 work tour
    filter = person_day['unique'].apply(lambda x: 'work1' in x and 'school2' in x)
    person_day.loc[filter, 'flag'] = 3
    # Resolve by: dropping all school2 tours (?) FIXME...
    tour = tour[~((tour['person_id'].isin(person_day[person_day['flag'] == 3].index)) & 
                (tour['tour_id']=='school2'))]

    # Report number of tours affected
    # FIXME: write out a log file
    print(str(person_day.groupby('flag').count()))

    # stop_frequency- does not include primary stop
    tour['outbound_stops'] = tour['tripsh1'] - 1
    tour['inbound_stops'] = tour['tripsh2'] - 1
    tour['stop_frequency'] = tour['outbound_stops'].astype('int').astype('str') + 'out' + '_' + tour['inbound_stops'].astype('int').astype('str') + 'in'

    # DATA FILTER: 
    # Filter out tours with too many stops on their tours
    df = tour[(tour['tripsh1'] > 4) | (tour['tripsh2'] > 4)]
    df.to_csv(os.path.join(output_dir,'temp','too_many_stops.csv'))
    logger.info(f'Dropped {len(df)} tours for too many stops')
    tour = tour[~((tour['tripsh1'] > 4) | (tour['tripsh2'] > 4))]

    # Borrowing this from canoncial_ids set_trip_index; FIXME should be an activitysim import when available
    MAX_TRIPS_PER_LEG = 4  # max number of trips per leg (inbound or outbound) of tour

    # DATA FILTER:
    # select trips that only exist in tours - is this necessary or can we use the trip file directly?

    # canonical_trip_num: 1st trip out = 1, 2nd trip out = 2, 1st in = 5, etc.
    canonical_trip_num = (~trip.outbound * MAX_TRIPS_PER_LEG) + trip.trip_num
    trip['trip_id'] = trip['tour_id'] * (2 * MAX_TRIPS_PER_LEG) + canonical_trip_num

    # DATA FILTER:
    # Some of these IDs are duplicated and it's not clear why - seems to be an issue with the canonical_trip_num definition
    # FIXME: what do we do about this? Fix canonical_trip_num? drop duplicates?
    # NOTE: this might be due to long person IDs, which are shortened at the end of the script. Considering changing Person IDs at top
    duplicated_person = trip[trip['trip_id'].duplicated()]['person_id'].unique()
    logger.info(f'Dropped {len(duplicated_person)} persons: duplicate IDs from canonical trip num definition')
    trip = trip[~trip['person_id'].isin(duplicated_person)]
    trip.set_index('trip_id', inplace=True, drop=False, verify_integrity=True)

    # Make sure all trips in a tour have an outbound and inbound component
    trips_per_tour = trip.groupby('tour_id')['person_id'].value_counts()
    missing_trip_persons = trips_per_tour[trips_per_tour == 1].index.get_level_values('person_id').to_list()
    logger.info(f'Dropped {len(missing_trip_persons)} persons: missing an outbound or inbound trip leg')

    req_cols = ['trip_id','person_id','trip_num','household_id','outbound','purpose','destination','origin','depart','trip_mode','tour_id']
    trip['trip_mode'] = trip['mode']
    
    # Write temporary versions of these files
    #trip.to_csv(os.path.join(output_dir,'survey_trips_raw.csv'), index=False)
    trip.to_csv(os.path.join(output_dir,'survey_trips.csv'), index=False)
    tour.to_csv(os.path.join(output_dir,'survey_tours.csv'), index=False)

    ########################################
    # Additional clean up and processing
    ########################################

    # Reloading files for easy debugging
    joint_tour_participants = pd.read_csv(os.path.join(output_dir,'survey_joint_tour_participants.csv'))
    tour = pd.read_csv(os.path.join(output_dir,r'survey_tours.csv'))
    households = pd.read_csv(os.path.join(output_dir,r'survey_households.csv'))
    person = pd.read_csv(os.path.join(output_dir,r'survey_persons.csv'))
    #trip = pd.read_csv(r'survey_data\survey_trips_raw.csv')
    trip = pd.read_csv(os.path.join(output_dir,r'survey_trips.csv'))

    ##############################
    # School and workplace cleaning (must be done after tour file is created)
    ##############################

    # if person makes a school tour but doesn't have a usual school location, use the first school tour destination
    # FIXME: this may be an issue if there are no students/education employment; may need to snap these trips and locations again
    school_tours = tour[tour.tour_type == 'school']
    school_tours = school_tours.groupby('person_id').first()[['destination']]
    person['school_zone_id'] = person['school_zone_id'].fillna(-1)
    person['school_zone_id'] = person['school_zone_id'].replace(0, -1)
    person = person.merge(school_tours, how='left', left_on='person_id', right_index=True)
    person.rename(columns={'destination': 'school_dest'}, inplace=True)
    person.loc[(~person['school_dest'].isnull()) & 
               ((person['school_zone_id'] == missing_school_zone) | (person['school_zone_id'].isnull())), 
               'school_zone_id'] = person['school_dest']
    person.drop('school_dest', axis=1, inplace=True)

    # Check that anyone coded as person tpye (ptype) of college student has a school_zone (imputed or stated)
    # If not, change their person type

    # Apply same rule for usual work location
    work_tours = tour[tour.tour_type == 'work']
    work_tours = work_tours.groupby('person_id').first()[['destination']]
    person['workplace_zone_id'] = person['workplace_zone_id'].fillna(-1)
    person['workplace_zone_id'] = person['workplace_zone_id'].replace(0, -1)
    person = person.merge(work_tours, how='left', left_on='person_id', right_index=True)
    person.rename(columns={'destination': 'work_dest'}, inplace=True)
    person.loc[(~person['work_dest'].isnull()) & (person['workplace_zone_id'] == -1), 'workplace_zone_id'] = person['work_dest']
    person.drop('work_dest', axis=1, inplace=True)

    # If a person makes a school tour, make sure their pstudent and ptype is correct
    # Addressed bug in mandatory tour frequency model
    school_tours = tour[tour.tour_type == 'school']
    person.loc[person['person_id'].isin(school_tours['person_id']) & 
           (person['pstudent'] == 3) & 
           (person['age'] <= 18), 'pstudent'] = 1    # k12 student
    person.loc[person['person_id'].isin(school_tours['person_id']) & 
        (person['pstudent'] == 3) & 
        (person['age'] > 18), 'pstudent'] = 2    # college student

    # People coded as non-workers making work tours
    work_tours = tour[tour.tour_type == 'work']
    person.loc[person['person_id'].isin(work_tours['person_id']) & 
           (person['pemploy'] == 3), 'pemploy'] = 2    # assume part-time workers

    # Some young children are coded as having work tours (likely going with parent)
    # Remove any work tours for someone coded as ptype 8
    tour = tour.merge(person[['person_id','ptype']], on='person_id',how='left')
    filter = (tour['tour_type'] == 'work') & (tour['ptype'] == 8)
    logger.info(f'Dropped {len(tour[filter])} tours: ptype 8 making work tours')
    tour = tour[~filter]

    # FIXME: move this to expression files ###
    # If a person has a usual workplace zone make them a part time worker (?) or remove their usual workplace location...
    person[(person['workplace_zone_id'] > 0) & (person['pemploy'] >= 3) & (person['age'] >= 16)]['pemploy'] = 2

    ### We cannot have more than 2 joint tours per household. If so, make sure we remove those households/tours
    ### FIXME: should we remove the households or edit the tours so they are not joint, or otherwise edit them?
    joint_tours = tour[tour['tour_category'] == 'joint']
    _df = joint_tours.groupby('household_id').count()['tour_id']
    too_many_jt_hh = _df[_df > 2].index

    # FIXME: For now remove all households; there are 4
    # We should figure out how to better deal with these
    tour = tour[~tour['household_id'].isin(too_many_jt_hh)]

    person_cols = ['person_id','household_id','age','PNUM','sex','pemploy','pstudent','ptype','school_zone_id','workplace_zone_id','free_parking_at_work']
    tour_cols = ['tour_id','person_id','household_id','tour_type','tour_category','destination','origin','start','end','tour_mode','parent_tour_id']
    trip_cols = ['trip_id','person_id','household_id','tour_id','outbound','purpose','destination','origin','depart','trip_mode']
    hh_cols = ['household_id','home_zone_id','income','hhsize','HHT','auto_ownership','num_workers']

    # Make sure all records align with available and existing households/persons
    _filter = person['household_id'].isin(households['household_id'])
    logger.info(f'Dropped {len(person[~_filter])} persons: missing household records')
    person = person[_filter]

    # activitysim checks specifically for int32 types; however, converting 64-bit int to 32 can create data issues if IDs are too long
    # Create a new mapping for person ID and household ID
    person['person_id_original'] = person['person_id'].copy()
    person['person_id'] = range(1, len(person)+1)

    households['household_id_original'] = households['household_id'].copy()
    households['household_id'] = range(1, len(households)+1)

    # Merge new household ID to person records
    person = person.merge(households[['household_id','household_id_original']], left_on='household_id', right_on='household_id_original', how='left')
    person.drop(['household_id_x'], axis=1, inplace=True)
    person.rename(columns={'household_id_y': 'household_id'}, inplace=True)

    # Store mapping for later reference
    person[['person_id','person_id_original','household_id','household_id_original']].to_csv(os.path.join(output_dir,'person_and_household_id_mapping.csv'))
    person['person_id'] = person['person_id'].astype('int32')
    households['person_id'] = households['household_id'].astype('int32')

    # Set new person and household ID on all other files
    trip.drop('household_id', inplace=True, axis=1)
    trip = trip.merge(person[['person_id','person_id_original','household_id']], left_on='person_id', right_on='person_id_original', how='left')
    trip.drop(['person_id_x','person_id_original'], axis=1, inplace=True)
    trip.rename(columns={'person_id_y': 'person_id'}, inplace=True)

    tour.drop('household_id', inplace=True, axis=1)
    tour = tour.merge(person[['person_id','person_id_original','household_id']], left_on='person_id', right_on='person_id_original', how='left')
    tour.drop(['person_id_x','person_id_original'], axis=1, inplace=True)
    tour.rename(columns={'person_id_y': 'person_id'}, inplace=True)

    joint_tour_participants.drop('household_id', inplace=True, axis=1)
    joint_tour_participants = joint_tour_participants.merge(person[['person_id','person_id_original','household_id']], left_on='person_id', right_on='person_id_original', how='left')
    joint_tour_participants.drop(['person_id_x','person_id_original'], axis=1, inplace=True)
    joint_tour_participants.rename(columns={'person_id_y': 'person_id'}, inplace=True)

    # All persons and household records must have trips/tours (?)
    # Note: removing PNUM==1 causes problems in joint tour frequency model 
    # Possible fix: renumerate PNUM to start at 1. Make sure it's updated on any related trip/tour files
    
    # Make sure ptype matches employment
    person.loc[(person['pemploy'] == 3) & (person['ptype'].isin([1,2])) & (person['age'] <= 18), 'ptype'] = 6
    person.loc[(person['pemploy'] == 3) & (person['ptype'].isin([1,2])) & 
               (person['age'] > 18) & (person['age'] <= 65), 'ptype'] = 4    # non-working adult
    person.loc[(person['pemploy'] == 3) & (person['ptype'].isin([1,2])) & (person['age'] > 65), 'ptype'] = 5    # retired

    # Ensure valid trips and tours
    tour = tour[tour['origin'] > 0]
    tour = tour[tour['destination'] > 0]
    trip = trip[trip['origin'] > 0]
    trip = trip[trip['destination'] > 0]

    # Make sure all tours have a tour_type defined; removing currently
    # FIXME: add to expression files
    _filter = tour['tour_type'] == '-1'
    logger.info(f'Dropped {len(tour[_filter])} tours: missing tour type')
    tour = tour[~_filter]

    # Check that tours have a valid mode
    # FIXME: should we try to impute a mode?
    _filter = tour['tour_mode'].isnull()
    logger.info(f'Dropped {len(tour[_filter])} tours: missing tour mode')
    tour = tour[~_filter]

    # Make sure a subtour's parent tour still exists. If not, remove
    tour['drop'] = 0
    tour.loc[(tour['parent_tour_id'] != 0) & ~(tour['parent_tour_id'].isin(tour.tour_id)),'drop'] = 1
    logger.info(f'Dropped {len(tour[tour["drop"] == 1])} tours: parent tour was removed/missing')
    tour = tour[tour['drop'] == 0]

    # Drop any tours and trips if there is only 1 trips; this might have caused by cleaning above
    trips_per_tour = trip.groupby('tour_id').count()[['person_id']]
    remove_tour_list = trips_per_tour[trips_per_tour.person_id < 2].index.values
    trip = trip[~trip['tour_id'].isin(remove_tour_list)]

    # Make sure each tour has an inbound and outbound component
    outbound_tours = trip[trip['outbound'] == True].tour_id.unique()
    inbound_tours = trip[trip['outbound'] == False].tour_id.unique()
    _filter = ((tour.tour_id.isin(outbound_tours))) & ((tour.tour_id.isin(inbound_tours)))
    logger.info(f'Dropped {len(tour[~_filter])} tours: missing inbound or outbound trip component')
    tour = tour[_filter]

    # Make sure sequence of departure times is sequential to apply trip_num; remove if not
    # This comes from trips that extend past midnight;
    # FIXME: is there a way to accomodate these trips/tours or should they be dropped outright?
    drop_tours = []
    for tour_id in tour['tour_id']:
        df = trip[trip['tour_id'] == tour_id]['depart']
        if not df.is_monotonic_increasing:
            drop_tours.append(tour_id)
    _filter = tour['tour_id'].isin(drop_tours)
    logger.info(f'Dropped {len(tour[_filter])} tours: travel day passes beyond midnight')
    tour = tour[~_filter]

    # Make sure tours and trips align; if trips were dropped the tour file should be updated to reflect changes
    # expect outbound last trip destination to be same as half tour destination
    trip_dest = trip[trip['outbound'] == True].groupby('tour_id').last()[['destination']].reset_index()
    tour.drop('destination', inplace=True, axis=1)
    tour = tour.merge(trip_dest, on='tour_id', how='left')

    # Check that non-subtour tours start at home
    # FIXME: consider asserting trip origin at home location? Trace the cause of this
    tour = tour.merge(households[['household_id','home_zone_id']], on='household_id', how='left')
    _filter = (tour['parent_tour_id'] == 0) & (tour['origin'] == tour['home_zone_id'])
    logger.info(f'Dropped {len(tour[~_filter])} tours: missing inbound or outbound trip component')
    tour = tour[_filter]

    # Explicitly-defined non workers should not be making work trips
    # Recode ptype to be part-time workers
    work_tours = tour[tour.tour_type == 'work']
    person.loc[(person['ptype'].isin([4])) & person['person_id'].isin(work_tours['person_id']), 'ptype'] = 2

    # Activitysim assumes all work and school trips are to a perons's primary work/school locations
    # In the first steps here we move usual school/work locations to zones with jobs/students so the location choice model can be estimated
    # If a trip's reported location is within a reasonable distance of the usual location, change the trip destination to the usual location

    # Calculate centroid distances for MAZs
    maz_shp = gpd.read_file(r'R:\e2projects_two\activitysim\maz\maz_blk10_shp.shp')
    tour = tour.merge(person[['person_id','workplace_zone_id','school_zone_id']], on='person_id', how='left')
    df = tour[tour['tour_type'] == 'work'][['destination','tour_id','workplace_zone_id']]
    
    df1 = df.merge(maz_shp, left_on='destination', right_on='maz_id', how='left').set_index('tour_id')
    df2 = df.merge(maz_shp, left_on='workplace_zone_id', right_on='maz_id', how='left').set_index('tour_id')
    dist_df = gpd.GeoSeries(df1.geometry).distance(gpd.GeoSeries(df2.geometry))

    # For differences of a mile or less, assign trip & tour desination to match usual work location
    # Otherwise, remove these trips and tours
    change_destination = dist_df[dist_df <= 5280].index
    drop_tours = dist_df[dist_df > 5280].index
    tour.index = tour['tour_id']
    tour.loc[change_destination,'destination'] = tour['workplace_zone_id']
    tour = tour.reset_index(drop=True)

    tour = tour[~tour['tour_id'].isin(drop_tours)]

    # Edit trips
    for tour_id in change_destination:
        # change last outbound destination
        df = trip.loc[(trip['tour_id'] == tour_id) & (trip['outbound'] == True)].iloc[-1]
        trip.loc[trip['trip_id'] == df['trip_id'], 'destination'] = tour.loc[tour['tour_id'] == tour_id,'destination'].values[0]
        # change first inbound origin
        df = trip.loc[(trip['tour_id'] == tour_id) & (trip['outbound'] == False)].iloc[0]
        trip.loc[trip['trip_id'] == df['trip_id'], 'origin'] = tour.loc[tour['tour_id'] == tour_id,'destination'].values[0]

    # Adjust trip & tour destinations for school travel
    df = tour[tour['tour_type'] == 'school'][['destination','school_zone_id','tour_id']]
    df1 = df.merge(maz_shp, left_on='destination', right_on='maz_id', how='left').set_index('tour_id')
    df2 = df.merge(maz_shp, left_on='school_zone_id', right_on='maz_id', how='left').set_index('tour_id')
    dist_df = gpd.GeoSeries(df1.geometry).distance(gpd.GeoSeries(df2.geometry))
    change_destination = dist_df[dist_df <= 5280].index
    drop_tours = dist_df[dist_df > 5280].index
    tour.index = tour['tour_id']
    tour.loc[change_destination,'destination'] = tour['school_zone_id']
    tour = tour.reset_index(drop=True)
 
    tour = tour[~tour['tour_id'].isin(drop_tours)]

    # Edit trips
    for tour_id in change_destination:
        # change last outbound destination
        df = trip.loc[(trip['tour_id'] == tour_id) & (trip['outbound'] == True)].iloc[-1]
        trip.loc[trip['trip_id'] == df['trip_id'], 'destination'] = tour.loc[tour['tour_id'] == tour_id,'destination'].values[0]
        # change first inbound origin
        df = trip.loc[(trip['tour_id'] == tour_id) & (trip['outbound'] == False)].iloc[0]
        trip.loc[trip['trip_id'] == df['trip_id'], 'origin'] = tour.loc[tour['tour_id'] == tour_id,'destination'].values[0]

    # Make sure trip origins and destinations are sequential
    # Since we had to remove certain trips this can affect the tour
    # FIXME: we may want to repair these Os and Ds, but they may not all make sense if missing a trip
    # Remove all trips and tours that don't have a matching half tour destinations
    # FIXME: put this and above in the same loop...
    flag_tours = []
    for tour_id in tour['tour_id'].unique():
        df = trip[trip['tour_id'] == tour_id]
        # expect outbound last trip destination to be same as outbound first trip origin
        if (df.loc[df['outbound'] == True, 'destination'].iloc[-1]) != (df.loc[df['outbound'] == False, 'origin'].iloc[0]):
            flag_tours = np.append(flag_tours, tour_id)
        # expect inbound first trip origin to be same as half tour destination
        if (df.loc[df['outbound'] == False, 'origin'].iloc[0]) != (tour[tour.tour_id == tour_id]['destination'].values[0]):
            flag_tours = np.append(flag_tours, tour_id)
        # expect inbound last trip destination to be same as half tour origin
        if (df.loc[df['outbound'] == False, 'destination'].iloc[-1]) != (tour[tour.tour_id == tour_id]['origin'].values[0]):
            flag_tours = np.append(flag_tours, tour_id)

    # Drop all flagged tours
    logger.info(f'Dropped {len(flag_tours)} tours: first half tour destination does not match last half tour origin')
    tour = tour[~tour.tour_id.isin(flag_tours)]

    # If a university student makes a school trip, rename purpose from "school" to "univ"
    trip = trip.merge(person[['person_id','pstudent','ptype']], on='person_id', how='left')
    trip.loc[(trip['pstudent'] == 2) & (trip['purpose'] == 'school'), 'purpose'] = 'univ'

    # For people making school trips, they should be coded as some sort of student
    filter = ((trip['purpose'] == 'school') & (~trip['ptype'].isin([6,7,8])))
    person_list = trip.loc[filter,'person_id']

    person.loc[(person['person_id'].isin(person_list)) & (person['age'] <= 5), 'ptype'] = 8
    person.loc[(person['person_id'].isin(person_list)) & (person['age'] > 5) & (person['age'] < 16), 'ptype'] = 7
    person.loc[(person['person_id'].isin(person_list)) & (person['age'] > 16) & (person['age'] < 21), 'ptype'] = 6 

    # For people making work trips, they must be full- or part-time worker, or university or driving age student
    filter = ((trip['purpose'] == 'work') & (~trip['ptype'].isin([1,2,3,6])))
    person_list = trip.loc[filter,'person_id']
    person.loc[(person['person_id'].isin(person_list)) & (person['pemploy'] == 1), 'ptype'] = 1
    # No school age or preschool kid should be making a work trip
    # Remove these tours entirely
    filter = (tour['tour_type'] == 'work') & (tour['ptype'].isin([7,8]))
    logger.info(f'Dropped {len(tour[filter])} tours: under 16 making work tours')
    tour = tour[~filter]

    person.loc[(person['person_id'].isin(person_list)) & (person['pemploy'] == 2), 'ptype'] = 2
    person.loc[(person['person_id'].isin(person_list)) & (person['pemploy'] == 3) & (person['pstudent'] == 2), 'ptype'] = 3

    # For people making univ trips, they must be full-or part time worker or university student
    filter = ((trip['purpose'] == 'univ') & (~trip['ptype'].isin([1,2,3])))
    person_list = trip.loc[filter,'person_id']
    person.loc[(person['person_id'].isin(person_list)) & (person['pemploy'] == 1), 'ptype'] = 1
    person.loc[(person['person_id'].isin(person_list)) & (person['pemploy'] == 2), 'ptype'] = 2
    person.loc[(person['person_id'].isin(person_list)) & (person['pemploy'] == 3) & (person['pstudent'] == 2), 'ptype'] = 3

    # Drop trips without a valid purpose
    trip = trip[trip['purpose'] != '-1']

    # Make sure trips, tours, and joint_tour_participants align
    trip = trip[trip['tour_id'].isin(tour['tour_id'])]
    tour = tour[tour['tour_id'].isin(trip['tour_id'])]
    joint_tour_participants = joint_tour_participants[joint_tour_participants['tour_id'].isin(tour['tour_id'])]
    households = households[households['household_id'].isin(person['household_id'])]

    # Set local person order in household to work with joint tour estimation
    # joint_tour_frequency.py assumes PNUM==1 should have a joint tour
    # Re-sort PNUM in person file based on joint_tour_participants file
    # Sort by most joint tours taken in household
    person_joint_tours = joint_tour_participants.groupby('person_id').count()['household_id'].reset_index()
    person_joint_tours.rename(columns={'household_id': 'joint_tour_count'}, inplace=True)
    person = person.merge(person_joint_tours, how='left', on='person_id').fillna(0)
    person = person.sort_values(['household_id','joint_tour_count'], ascending=False)
    for household_id in person.household_id.unique():
        person.loc[person['household_id'] == household_id, 'PNUM'] = range(1, 1+len(person[person['household_id'] == household_id]))

    # Update joint_tour_participants with new PNUM
    joint_tour_participants.drop('participant_num', axis=1, inplace=True)
    joint_tour_participants = joint_tour_participants.merge(person[['person_id','PNUM']], how='left')
    
    # Tour records for joint tours only have info on primary trip makers; update these since we changed primary tour maker (PNUM==1)
    df = joint_tour_participants[joint_tour_participants['PNUM'] == 1]
    joint_tour_participants.rename(columns={'PNUM':'participant_num'}, inplace=True)    # reset to original col name

    tour = tour.merge(df[['tour_id','person_id']], how='left', on='tour_id')
    tour['person_id_y'].fillna(tour['person_id_x'], inplace=True)
    tour.drop('person_id_x', axis=1, inplace=True)
    tour.rename(columns={'person_id_y': 'person_id'}, inplace=True)

    person = person[person_cols]
    tour = tour[tour_cols]
    trip = trip[trip_cols]
    households = households[hh_cols]

    joint_tour_participants.to_csv(os.path.join(output_dir,'survey_joint_tour_participants.csv'), index=False)
    tour.to_csv(os.path.join(output_dir,'survey_tours.csv'), index=False)
    households.to_csv(os.path.join(output_dir,'survey_households.csv'), index=False)
    person.to_csv(os.path.join(output_dir,'survey_persons.csv'), index=False)
    trip.to_csv(os.path.join(output_dir,'survey_trips.csv'), index=False)

    # Not sure why infer.py requires the final tables, write them out for now so we can use default script settings
    joint_tour_participants.to_csv(os.path.join(output_dir,'final_survey_joint_tour_participants.csv'), index=False)
    tour.to_csv(os.path.join(output_dir,'final_survey_tours.csv'), index=False)
    households.to_csv(os.path.join(output_dir,'final_survey_households.csv'), index=False)
    person.to_csv(os.path.join(output_dir,'final_survey_persons.csv'), index=False)
    trip.to_csv(os.path.join(output_dir,'final_survey_trips.csv'), index=False)

    # Conclude log
    end_time = datetime.datetime.now()
    elapsed_total = end_time - start_time
    logger.info('--------------------RUN ENDING--------------------')
    logger.info('TOTAL RUN TIME %s'  % str(elapsed_total))