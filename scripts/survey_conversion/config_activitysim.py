trip_file_dir = r'R:\e2projects_two\2018_base_year\survey\geocode_parcels\2018\5_trip.csv'
hh_file_dir = r'R:\e2projects_two\2018_base_year\survey\geocode_parcels\2018\1_household.csv'
person_file_dir = r'R:\e2projects_two\2018_base_year\survey\geocode_parcels\2018\2_person.csv'

# FIXME - which columns to use?:
hh_wt_col = 'hh_wt_combined'

# Output directory
output_dir = r'C:\Users\bnichols\travel-studies\2017\daysim_conversions'
#output_dir = r'R:\e2projects_two\2018_base_year\survey\daysim_format'
#output_dir  = r'C:\Users\bnichols\Documents\estimation_2017\2014_estimation'

# Flexible column names, given that these may change in future surveys
hhno = 'hhid'
hownrent = 'rent_own'
hrestype = 'res_type'
hhincome = 'hhincome_detailed'
hhtaz = 'final_home_taz'
hhparcel = 'final_home_parcel'
hhexpfac = 'hh_wt_revised'
hhwkrs = 'numworkers'
hhvehs = 'vehicle_count'
pno = 'pernum'

# Heirarchy order for tour mode, per DaySim docs: https://www.psrc.org/sites/default/files/2015psrc-modechoiceautomodels.pdf
# Drive to Transit > Walk to Transit > School Bus > HOV3+ > HOV2 > SOV > Bike > Walk > Other
drive_transit_i = 0
walk_transit_i = 0
mode_heirarchy = {6: 'walk_to_transit',
                  8: 'drive_to_transit',
                  9: 'school_bus',
                  5: 'hov3',
                  4: 'hov2',
                  3: 'sov',
                  2: 'bike',
                  1: 'walk',
                  10: 'change_mode'}

# Field names
gender = 'pgend'
student_type = 'pstyp'
hhid = 'hhno'
person_number = 'pno'
expfac = 'psexpfac'

work_taz = 'workplace_taz'
school_taz = 'school_taz'

person_cols =['hhno', 'pno', 'pptyp', 'pagey', 'pgend', 'pwtyp', 'pwpcl', 'pwtaz', 'pwautime',
            'pwaudist', 'pstyp', 'pspcl', 'pstaz', 'psautime', 'psaudist', 'puwmode', 'puwarrp', 
            'puwdepp', 'ptpass', 'ppaidprk', 'pdiary', 'pproxy', 'psexpfac']

person_type_dict = {
    1: 'hhftw', # full time workers
    2: 'hhptw', # part time workers
    3: 'hhret', # retirees
    4: 'hhoad', # other adults,
    5: 'hhuni', # university students
    6: 'hhhsc', # high school students
    7: 'hh515', # k12 age 5-15,
    8: 'hhcu5', # age under 5
    }

# Take median age
age_map = {
    'Under 5 years old': 2,
    '5-11 years': 8,
    '12-15 years': 14,
    '16-17 years': 17,
    '18-24 years': 21,
    '25-34 years': 30,
    '35-44 years': 40,
    '45-54 years': 50,
    '55-64 years': 60,
    '65-74 years': 70,
    '75-84 years': 80,
    '85 or years older': 85
}

gender_map = {
    'Male': 1,    # male: male
    'Female': 2,    # female: female
    'Another': 9,    # another: missing
    'Prefer not to answer' : 9     # prefer not to answer: missing
}

student_map = {
    'No, not a student': 3,
    'Full-time student': 1,
    'Part-time student': 2,
    'Missing: Skip Logic': -1
}

hownrent_map = {
    'Own/paying mortgage': 1,
    'Rent': 2,
    'Other': 3,
    'Prefer not to answer': 3,
    'Provided by job or military': 3
}

mode_dict = {
    'Household vehicle 1': 'Auto', 
    'Household vehicle 2': 'Auto', 
    'Household vehicle 3': 'Auto',
    'Household vehicle 4': 'Auto',                        
    'Household vehicle 5': 'Auto', 
    'Household vehicle 6': 'Auto',
    'Household vehicle 7': 'Auto',
    'Walk (or jog/wheelchair)': 'WALK',
    'Bicycle or e-bike (rSurvey only)': 'BIKE',
    'Bus (public transit)': 'Transit',
    'Private bus or shuttle': 'Other', 
    'Other vehicle in household': 'Auto',
    'Other hired service (Uber, Lyft, or other smartphone-app car service)': 'TNC',
    'Vanpool': 'Auto', 
    "Friend/colleague's car": 'Auto', 
    'Other non-household vehicle': 'Auto',
    'Car from work': 'Auto', 
    'Carshare service (e.g., Turo, Zipcar, ReachNow)': 'Auto',
    'School bus': 'School_Bus', 
    'Urban Rail (e.g., Link light rail, monorail)': 'Transit',
    'Airplane or helicopter': 'Other', 
    'Missing: Non-response': 'Other', 
    'Rental car': 'Auto',
    'Taxi (e.g., Yellow Cab)': 'TNC',   # include as TNC or other?
    'Ferry or water taxi': 'Transit', 
    'Other motorcycle/moped/scooter': 'Auto',
    'Other rail (e.g., streetcar)': 'Transit',
    'Other mode (e.g., skateboard, kayak, motorhome, etc.)': 'Other',
    'Other bus (rMove only)': 'Transit', 
    'Paratransit': 'Other',    # Other?
    'Commuter rail (Sounder, Amtrak)': 'Transit', 
    'Bike-share bicycle (rMove only)':'BIKE', 
    'Scooter or e-scooter (e.g., Lime, Bird, Razor)':'BIKE',    # Lump this in with BIKE? 
    'Other motorcycle/moped': 'Auto',
    'Bicycle owned by my household (rMove only)':'BIKE',
    'Borrowed bicycle (e.g., from a friend) (rMove only)':'BIKE',
    'Other rented bicycle (rMove only)':'BIKE'
            }

#mode_dict = {
#    'WALK': 1,
#    'Bike': 2,
#    'SOV': 3,
#    'HOV2': 4,
#    'HOV3+': 5,
#    'Transit': 6,
#    'School_Bus': 8,
#    'TNC': 9,   # Note that 9 was other in older Daysim records
#    'Other': 10
#    }

commute_mode_dict = {
    1: 3, # SOV
    2: 4, # HOV (2 or 3)
    3: 4, # HOV (2 or 3)
    4: 3, # Motorcycle, assume drive alone 
    5: 5, # vanpool, assume HOV3+
    6: 2, # bike
    7: 1, # walk
    8: 6, # bus -> transit
    9: 10, # private bus -> other
    10: 10, # paratransit -> other
    11: 6, # commuter rail
    12: 6, # urban rail
    13: 6, # streetcar
    14: 6, # ferry
    15: 10, # taxi -> other
    16: 9, # TNC
    17: 10, # plane -> other
    97: 10 # other
    }

day_map = {
    'Monday': 1,
    'Tuesday': 2,
    'Wednesday': 3,
    'Thursday': 4
}

purpose_map = {
    'Errand/Other' :'othmaint', 
    'Shop': 'shopping', 
    'Home': 'Home', 
    'Social/Recreation': 'social',
    'Work-related': 'work', 
    'School': 'school', 
    'Escort': 'escort', 
    'Work': 'work', 
    'Meal': 'eatout', 
    'Change mode': 'change_mode',
    'Missing: Non-response': -1
    }

full_purpose_map = {
    'Conducted personal business (e.g., bank, post office)': 'othmaint',
       'Went grocery shopping': 'shopping', 
       'Went home': 'Home',
       'Went to religious/community/volunteer activity': 'social',
       'Went to work-related place (e.g., meeting, second job, delivery)' : 'work',
       'Went to other shopping (e.g., mall, pet store)': 'shopping',
       'Went to school/daycare (e.g., daycare, K-12, college)': 'school',
       "Dropped off/picked up someone (e.g., son at a friend's house, spouse at bus stop)": 'escort',
       'Went to primary workplace': 'work',
       'Went to medical appointment (e.g., doctor, dentist)': 'othmaint',
       'Went to restaurant to eat/get take-out': 'eatout',
       'Attended recreational event (e.g., movies, sporting event)': 'othdiscr',
       'Attended social event (e.g., visit with friends, family, co-workers)': 'social',
       'Went to exercise (e.g., gym, walk, jog, bike ride)': 'othdiscr',
       'Went to other work-related activity': 'work', 
       'Other purpose': 'othdiscr',
       'Other appointment/errands (rMove only)': 'othmaint',
       'Other social/leisure (rMove only)': 'social',
       'Transferred to another mode of transportation (e.g., change from ferry to bus)': 'change_mode',
       'Missing: Non-response': -1,
       "Went to a family activity (e.g., child's softball game)": 'othdiscr',
       'Vacation/Traveling (rMove only)': 'othdiscr'
       }

dorp_map = {
    1: 1,
    2: 2,
    3: 9
}

# Household


hhrestype_map = {'Single-family house (detached house)': 1,
                 'Townhouse (attached house)': 2,
                 'Building with 3 or fewer apartments/condos': 2,
                 'Building with 4 or more apartments/condos': 3,
                 'Other (including boat, RV, van, etc.)': 6,
                 'Mobile home/trailer': 4,
                 'Dorm or institutional housing': 5
                 }

# Use the midpoint of the ranges provided 

income_map = {
    'Under $10,000': 5000,
    '$10,000-$24,999': 17500,
    '$25,000-$34,999': 30000,
    '$35,000-$49,999': 42500,
    '$50,000-$74,999': 62500,
    '$75,000-$99,999': 87500,
    '$100,000-$149,999': 125000,
    '$150,000-$199,999': 175000,
    '$200,000-$249,999': 225000,
    '$250,000 or more':250000,
    'Prefer not to answer': -1,
        }

vehicle_count_map = {
    '0 (no vehicles)': 0,
    '1 vehicle': 1, 
    '2 vehicles': 2,
    '3 vehicles': 3,
    '4 vehicles': 4,
    '5 vehicles': 5,
    '6 vehicles': 6,
    '7 vehicles': 7,
    '8+ vehicles': 8
         }

hhsize_map = {
    '1 person': 1,
    '2 people': 2, 
    '3 people': 3,
    '4 people': 4, 
    '5 people': 5,
    '6 people': 6,
    '7 people': 7,
    '8 people': 8,
    '9 people': 9
         }

is_mf_map = {
    'Other (including boat, RV, van, etc.)': 0,
    'Single-family house (detached house)': 0,
    'Building with 4 or more apartments/condos': 1,
    'Townhouse (attached house)': 1,
    'Building with 3 or fewer apartments/condos': 1,
    'Mobile home/trailer': 0,
    'Dorm or institutional housing': 1}

race_dict = {
    'African American': 2,
    'Asian': 3,
    'Child': 8,    
    'Hispanic': 7,
    'Missing': 8,
    'Other': 4,
    'White Only': 1}

# Daysim concept of path type, 1-4 for transit hierarchy 
# with 1 being the top of the list, to 5 for auto/non-motorized trips
path_type_dict = {'DRIVEALONEFREE': 5,
                  'SHARED2FREE': 5,
                  'SHARED3FREE': 5,
                  'BIKE': 5,
                  'WALK': 5,
                  'WALK_LOC': 4,
                  'WALK_LR': 3,
                  'WALK_COM': 2,
                  'WALK_FRY': 1}

mode_heirarchy = ['WALK_FRY','WALK_COM','WALK_LR','WALK_LOC','SHARED3FREE','SHARED2FREE','DRIVEALONEFREE','BIKE','WALK','Other']