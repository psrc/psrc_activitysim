import array as _array
import time
import inro.emme.desktop.app as app
import inro.modeller as _m
import inro.emme.matrix as ematrix
import inro.emme.database.matrix
import inro.emme.database.emmebank as _eb
import pandas as pd
import numpy as np
import h5py
import openmatrix as omx
import json
import os
from EmmeProject import *

def get_trn_sub_component_skim(condition, my_project, sub_component_name):
    bus = emmeMatrix_to_numpyMatrix(sub_component_name + 'a', my_project.bank, 'float32')

    rail = emmeMatrix_to_numpyMatrix(sub_component_name + 'r', my_project.bank, 'float32')

    ferry = emmeMatrix_to_numpyMatrix(sub_component_name + 'f', my_project.bank, 'float32')

    p_ferry = emmeMatrix_to_numpyMatrix(sub_component_name + 'f', my_project.bank, 'float32')

    commuter_rail = emmeMatrix_to_numpyMatrix(sub_component_name + 'c', my_project.bank, 'float32')

    condition[1] = np.where(condition[0] == True, False, condition[1])
    condition[2] = np.where((condition[0] + condition[1]), False, condition[2])
    condition[3] = np.where(
        (condition[0] + condition[1] + condition[2]), False, condition[3]
    )
    condition[4] = np.where(
        (condition[0] + condition[1] + condition[2] + condition[3]), False, condition[4]
    )

    skim = (
        np.where([condition[0]], bus, 0)
        + np.where([condition[1]], rail, 0)
        + np.where([condition[2]], ferry, 0)
        + np.where([condition[2]], p_ferry, 0)
        + np.where([condition[2]], commuter_rail, 0)
    )
    skim = skim.reshape([matrix_size, matrix_size])
    return skim

def emmeMatrix_to_numpyMatrix(matrix_name, emmebank, np_data_type, multiplier=1, max_value = None):
    matrix_id = emmebank.matrix(matrix_name).id
    emme_matrix = emmebank.matrix(matrix_id)
    matrix_data = emme_matrix.get_data()
    np_matrix = np.matrix(matrix_data.raw_data) 
    np_matrix = np_matrix * multiplier
    
    if np_data_type == 'uint16':
        max_value = np.iinfo(np_data_type).max
        np_matrix = np.where(np_matrix > max_value, max_value, np_matrix)
    
    if np_data_type != 'float32':
        np_matrix = np.where(np_matrix > np.iinfo(np_data_type).max, np.iinfo(np_data_type).max, np_matrix)

    return np_matrix

results_dict = {}
#matrix_size = len(myh5["Skims"]["sov_inc2t"])
my_project = EmmeProject('C:/Workspace/sc_park_and_ride2/soundcast/projects/LoadTripTables/LoadTripTables.emp')

# Bike and walk skims
my_project.change_active_database('5to6')

# Assuming 10 mph bike speed
results_dict["DISTBIKE"] = (emmeMatrix_to_numpyMatrix('walkt', my_project.bank, 'float32') * (10.0 / 60.0))
  
# Assuming 3 mph bike speed
results_dict["DISTWALK"] = (emmeMatrix_to_numpyMatrix('walkt', my_project.bank, 'float32') * (3.0 / 60.0))  


# Transit fares
my_project.change_active_database('6to7')

for tod in ["AM", "MD", "PM", "EV", "EA"]:
    results_dict["WLK_TRN_DRV_FAR__" + tod] = emmeMatrix_to_numpyMatrix('mfafarbx', my_project.bank, 'float32')
    results_dict["DRV_TRN_WLK_FAR__" + tod] = emmeMatrix_to_numpyMatrix('mfafarbx', my_project.bank, 'float32')
    for mtc_mode in ["COM", "FRY", "HVY", "LOC", "LR"]:
        # Fare
        # Using AM fares for all time periods for now
        results_dict["WLK_" + mtc_mode + "_WLK_FAR__" + tod] = emmeMatrix_to_numpyMatrix('mfafarbx', my_project.bank, 'float32')
        
        

# Distance Skims:
my_project.change_active_database('7to8')
# Looks like DIST is some auto distance averaged over different time periods, use AM for now.
results_dict["DIST"] = emmeMatrix_to_numpyMatrix('sov_inc2d', my_project.bank, 'float32')  

#fix later
matrix_size = len(results_dict['DISTBIKE'])
zero_array = np.zeros((matrix_size, matrix_size))

# Vehicle matrices
#### DISTANCE ####
# Distance is only skimmed for once (7to8); apply to all TOD periods
for tod in ["AM", "MD", "PM", "EV", "EA"]:  # EA=early AM
    results_dict["SOV_DIST__" + tod] = emmeMatrix_to_numpyMatrix('sov_inc2d', my_project.bank, 'float32')
    
    results_dict["HOV2_DIST__" + tod] = emmeMatrix_to_numpyMatrix('hov2_inc2d', my_project.bank, 'float32')

    results_dict["HOV3_DIST__" + tod] = emmeMatrix_to_numpyMatrix('hov3_inc2d', my_project.bank, 'float32')

    results_dict["SOVTOLL_DIST__" + tod] = emmeMatrix_to_numpyMatrix('sov_inc2d', my_project.bank, 'float32')

    results_dict["HOV2TOLL_DIST__" + tod] = emmeMatrix_to_numpyMatrix('hov2_inc2d', my_project.bank, 'float32')

    results_dict["HOV3TOLL_DIST__" + tod] = emmeMatrix_to_numpyMatrix('hov3_inc2d', my_project.bank, 'float32')

    # Cost
    # BTOLL is bridge toll
    # Set this to 0 and only use the value toll (?)
    results_dict["SOV_BTOLL__" + tod] = zero_array
    results_dict["HOV2_BTOLL__" + tod] = zero_array
    results_dict["HOV3_BTOLL__" + tod] = zero_array
    results_dict["SOVTOLL_BTOLL__" + tod] = zero_array
    results_dict["HOV2TOLL_BTOLL__" + tod] = zero_array
    results_dict["HOV3TOLL_BTOLL__" + tod] = zero_array

time_dict = {"AM": "7to8", "MD": "10to14", "PM": "17to18", "EV": "18to20", "EA": "5to6"}


for tod, hour in time_dict.items():
    print(tod)
    print(hour)
    my_project.change_active_database(hour)

    # Auto Time
    results_dict["SOV_TIME__" + tod] = emmeMatrix_to_numpyMatrix('sov_inc2t', my_project.bank, 'float32')

    results_dict["HOV2_TIME__" + tod] = emmeMatrix_to_numpyMatrix('hov2_inc2t', my_project.bank, 'float32')

    results_dict["HOV3_TIME__" + tod] = emmeMatrix_to_numpyMatrix('hov3_inc2t', my_project.bank, 'float32')

    results_dict["SOVTOLL_TIME__" + tod] = emmeMatrix_to_numpyMatrix('sov_inc2t', my_project.bank, 'float32')

    results_dict["HOV2TOLL_TIME__" + tod] = emmeMatrix_to_numpyMatrix('hov2_inc2t', my_project.bank, 'float32')

    results_dict["HOV3TOLL_TIME__" + tod] = emmeMatrix_to_numpyMatrix('hov3_inc2t', my_project.bank, 'float32')

    results_dict["SOVTOLL_VTOLL__" + tod] = emmeMatrix_to_numpyMatrix('sov_inc2c', my_project.bank, 'float32')

    results_dict["HOV2TOLL_VTOLL__" + tod] = emmeMatrix_to_numpyMatrix('hov2_inc2c', my_project.bank, 'float32')
    
    results_dict["HOV3TOLL_VTOLL__" + tod] = emmeMatrix_to_numpyMatrix('hov3_inc2c', my_project.bank, 'float32')

    submode_dict = {"COM": "c", "LR": "r", "LOC": "a", "FRY": "f"}

    ## TRN mode, which we think is all transit:
    #bus_time = (
    #    emmeMatrix_to_numpyMatrix('auxwa', my_project.bank, 'float32') +
    #    emmeMatrix_to_numpyMatrix('twtwa', my_project.bank, 'float32') +
    #    emmeMatrix_to_numpyMatrix('ivtwa', my_project.bank, 'float32')
    #    )

    #rail_time = (
    #    emmeMatrix_to_numpyMatrix('auxwr', my_project.bank, 'float32') +
    #    emmeMatrix_to_numpyMatrix('twtwr', my_project.bank, 'float32') +
    #    emmeMatrix_to_numpyMatrix('ivtwr', my_project.bank, 'float32')
    #    )
    
    #ferry_time = (
    #    emmeMatrix_to_numpyMatrix('auxwf', my_project.bank, 'float32') +
    #    emmeMatrix_to_numpyMatrix('twtwf', my_project.bank, 'float32') +
    #    emmeMatrix_to_numpyMatrix('ivtwf', my_project.bank, 'float32')
    #    )

    #p_ferry_time = (
    #    emmeMatrix_to_numpyMatrix('auxwp', my_project.bank, 'float32') +
    #    emmeMatrix_to_numpyMatrix('twtwp', my_project.bank, 'float32') +
    #    emmeMatrix_to_numpyMatrix('ivtwp', my_project.bank, 'float32')
    #    )

    #commuter_rail_time = (
    #    emmeMatrix_to_numpyMatrix('auxwc', my_project.bank, 'float32') +
    #    emmeMatrix_to_numpyMatrix('twtwc', my_project.bank, 'float32') +
    #    emmeMatrix_to_numpyMatrix('ivtwc', my_project.bank, 'float32')
    #    )

    #arr = np.array((bus_time, rail_time, ferry_time, p_ferry_time, commuter_rail_time))
    #skim_index = arr == arr.min(axis=0)

    ## we dont break walks skims out for access, egress and transfer.
    ## used in accessbility and combined so hopefully not a big
    ## deal if we leave the transfer 0 for now.
    #transit_walk = get_trn_sub_component_skim(skim_index, my_project, "auxw")
    #results_dict["WLK_TRN_WLK_WAUX__" + tod] = transit_walk * 0
    #results_dict["WLK_TRN_WLK_WACC__" + tod] = transit_walk / 2
    #results_dict["WLK_TRN_WLK_WEGR__" + tod] = transit_walk / 2
    ## Initial Wait Time
    #results_dict["WLK_TRN_WLK_IWAIT__" + tod] = get_trn_sub_component_skim(
    #    skim_index, my_project, "iwtw"
    #)
    ## Transfer Wait Time
    #results_dict["WLK_TRN_WLK_XWAIT__" + tod] = get_trn_sub_component_skim(
    #    skim_index, my_project, "xfrw"
    #)
    ## Total Wait Time
    #results_dict["WLK_TRN_WLK_WAIT__" + tod] = get_trn_sub_component_skim(
    #    skim_index, my_project, "twtw"
    #)
    ## In vehicle time for Walk access/egress
    #results_dict["WLK_TRN_WLK_IVT__" + tod] = get_trn_sub_component_skim(
    #    skim_index, my_project, "ivtw"
    #)

    for mtc_mode, psrc_mode in submode_dict.items():
        print(mtc_mode)
        # if mtc_mode == 'LOC':
        # Walk Access Time
        results_dict["WLK_" + mtc_mode + "_WLK_WAUX__" + tod] = emmeMatrix_to_numpyMatrix('auxw' + psrc_mode, my_project.bank, 'float32')

        # Initial Wait Time
        results_dict["WLK_" + mtc_mode + "_WLK_IWAIT__" + tod] = emmeMatrix_to_numpyMatrix('iwtw' + psrc_mode, my_project.bank, 'float32')

        # Total Wait Time
        results_dict["WLK_" + mtc_mode + "_WLK_WAIT__" + tod] = emmeMatrix_to_numpyMatrix('twtw' + psrc_mode, my_project.bank, 'float32')

        # In vehicle time for Walk access/egress
        results_dict["WLK_" + mtc_mode + "_WLK_TOTIVT__" + tod] = emmeMatrix_to_numpyMatrix('ivtw' + psrc_mode, my_project.bank, 'float32')

        if not mtc_mode == "LOC":
            results_dict["WLK_" + mtc_mode + "_WLK_KEYIVT__" + tod] = emmeMatrix_to_numpyMatrix('ivtw' + psrc_mode + psrc_mode, my_project.bank, 'float32')

            if mtc_mode == "LRF":
                results_dict["WLK_" + mtc_mode + "_WLK_FERRYIVT__" + tod] = emmeMatrix_to_numpyMatrix('ivtw' + psrc_mode + psrc_mode, my_project.bank, 'float32') 

                

            # Transfer Time
        results_dict["WLK_" + mtc_mode + "_WLK_XWAIT__" + tod] = emmeMatrix_to_numpyMatrix('xfrw' + psrc_mode, my_project.bank, 'float32') 

        # Boardings
        results_dict["WLK_" + mtc_mode + "_WLK_BOARDS__" + tod] = emmeMatrix_to_numpyMatrix('ndbw' + psrc_mode, my_project.bank, 'float32')
        
    

        
####################
# Drive to Transit #
####################
pnr_skims_path = r'C:\Workspace\sc_park_and_ride\soundcast\pnr_skims.h5'
myh5 = h5py.File(pnr_skims_path)
for skim_name in myh5['Skims'].keys():
    print (skim_name)
    results_dict[skim_name] = myh5["Skims"][skim_name][:matrix_size, :matrix_size]


f = omx.open_file(r"C:\Workspace\new2.omx", "w")

for skim_matrix in results_dict.keys():
    print(skim_matrix)
    #statsDict= {attr:getattr(skim_matrix,attr)() for attr in ['min', 'max','mean','std']}
    f[skim_matrix] = results_dict[skim_matrix]
f.close()
myh5.close()