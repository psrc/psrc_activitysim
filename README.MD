
## PSRC Example

### Survey Conversion Script

Survey conversion to Activitysim format for estimation is available in [scripts/survey_conversion/activitysim_converstion.py](https://github.com/psrc/psrc_activitysim/blob/dev/scripts/survey_conversion/activitysim_survey_conversion.py). The latest output of this script (in Activitysim format) is available in [data/survey_data](https://github.com/psrc/psrc_activitysim/tree/main/data/survey_data). 

Most inputs to run this script from raw PSRC survey format are available in [data/survey_conversion](https://github.com/psrc/psrc_activitysim/tree/main/data/survey_conversion). However, some input files were too large. These are [available here for download via FTP](https://file.ac/iA6D4tPhtBQ/), and include the following files, which should be added to the local data/survey_conversion directory:
- elmer_trip.csv (raw survey trips)
- elmer_trip_geocoded.csv (raw survey trips with properly geocoded MAZs, only including trips within the region)
- parcels_urbansim.txt (parcels file used for geocoding - only needed if performing optional geocoding steps)
