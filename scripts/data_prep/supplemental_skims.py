# This script performs a mixed mode assignment to produce park and ride skims
# These skims are primarly used for development of Activitysim and supplementary to
# Soundcast's park and ride modeling.
# This script was generated to provide individual skim components from an origin to a destination
# For example, auto time to access the nearest park and ride station along with all other standard
# transit components like wait time, in-vehicle time, etc.
# Emme's mixed mode assignment will select the optimal park and ride location and transit route.
# The result is a measure of skim components directly between an origin and destination, which
# is not directly available from existing Soundcast park and ride procedures.

import array as _array
import time
import inro.emme.desktop.app as app
import inro.modeller as _m
import inro.emme.matrix as ematrix
import inro.emme.database.matrix
import inro.emme.database.emmebank as _eb
import json
import numpy as np
import time
import os, sys
import h5py
import shutil
import multiprocessing as mp
import subprocess
from multiprocessing import Pool
import logging
import datetime
import argparse
import traceback

sys.path.append(os.path.join(os.getcwd(), "scripts"))
sys.path.append(os.path.join(os.getcwd(), "inputs"))
sys.path.append(os.getcwd())
# from emme_configuration import *
from EmmeProject import *

# from data_wrangling import text_to_dictionary, json_to_dictionary
from pathlib import Path
import openmatrix as omx

# Script should be run from project root
# os.chdir(r'C:\Workspace\sc_park_and_ride\soundcast')

# Run this script for 5 time periods

# tod_dict = {
#     'AM': '7to8',
#     'MD': '10to14',
#     'PM': '16to17',
#     'EV': '18to20',
#     'NI': '20to5',
#     "EA": "5to6"
#     }

# Lookup between matrix names used here and activitysim names
matrix_dict = {
    "DRV_TRN_WLK_DTIM": "auto_pnr_time_access",
    "WLK_TRN_DRV_DTIM": "auto_pnr_time_egress",
    "DRV_TRN_WLK_DDIST": "auto_pnr_distance_access",
    "WLK_TRN_DRV_DDIST": "auto_pnr_distance_egress",
    "DRV_TRN_WLK_WAUX": "pnr_aux_time_access",
    "WLK_TRN_DRV_WAUX": "pnr_aux_time_egress",
    "DRV_TRN_WLK_IWAIT": "pnr_initial_wait_access",
    "WLK_TRN_DRV_IWAIT": "pnr_initial_wait_egress",
    "DRV_TRN_WLK_WAIT": "pnr_total_wait_access",
    "WLK_TRN_DRV_WAIT": "pnr_total_wait_egress",
    "DRV_TRN_WLK_TOTIVT": "pnr_aivt_access",
    "WLK_TRN_DRV_TOTIVT": "pnr_aivt_egress",
    "DRV_TRN_WLK_BOARDS": "pnr_boardings_access",
    "WLK_TRN_DRV_BOARDS": "pnr_boardings_egress",
}

pnr_access_class_name = "pnr_access"
pnr_egress_class_name = "pnr_egress"
ferry_class_name = "asim_ferry"
all_transit_class_name = "all_transit"


def emmeMatrix_to_numpyMatrix(
    matrix_name, emmebank, np_data_type, multiplier, max_value=None
):
    matrix_id = emmebank.matrix(matrix_name).id
    emme_matrix = emmebank.matrix(matrix_id)
    matrix_data = emme_matrix.get_data()
    np_matrix = np.matrix(matrix_data.raw_data)
    np_matrix = np_matrix * multiplier

    if np_data_type == "uint16":
        max_value = np.iinfo(np_data_type).max
        np_matrix = np.where(np_matrix > max_value, max_value, np_matrix)

    if np_data_type != "float32":
        np_matrix = np.where(
            np_matrix > np.iinfo(np_data_type).max,
            np.iinfo(np_data_type).max,
            np_matrix,
        )

    return np_matrix


def assignment(spec, my_project, class_name):
    # Extended transit assignment
    assign_transit = my_project.m.tool(
        "inro.emme.transit_assignment.extended_transit_assignment"
    )
    assign_transit(spec, class_name=class_name)

    return None


def skim(spec1, spec2, my_project, class_name):

    skim_pnr = my_project.m.tool("inro.emme.transit_assignment.extended.matrix_results")

    # Skim for time in transit from park and ride lot to destination station
    # submodes bcfpr
    # loop through each submode and write out a skim file for each (?)
    # my_project.create_matrimx ('auto_access_time_all_test', 'new test', 'FULL')
    skim_pnr(spec1, class_name=class_name)

    # Skim for walk access to destination
    skim_pnr(spec2, class_name=class_name)

    return None


def park_and_ride_skims(my_project, sc_tod, spec_dir, results_dict, asim_tod):

    # my_project.change_active_database(sc_tod)

    matrix_list = [
        "pnr_demand",
        "auto_pnr_distance",
        "auto_pnr_time",
        "pnr_aivt",
        "pnr_boardings",
        "pnr_aux_time",
        "pnr_initial_wait",
        "pnr_total_wait",
    ]

    for type in ["access", "egress"]:
        for matrix_name in matrix_list:
            # Delete matrix if it already exists
            try:
                my_project.delete_matrix(
                    my_project.bank.matrix(matrix_name + "_" + type).id
                )
                print("-----")
                print(matrix_name)
            except:
                pass
            my_project.create_matrix(matrix_name + "_" + type, "", "FULL")
            print(matrix_name)

    ## Create extra attribute for auxiliary walk demand modes x and w with an all designation
    my_project.create_extra_attribute(
        "LINK", "@volax_w_all", description="aux w pnr all", overwrite=True
    )
    my_project.create_extra_attribute(
        "LINK", "@volax_x_all", description="aux x pnr all", overwrite=True
    )
    my_project.create_extra_attribute(
        "LINK", "@volax_w_pnr", description="aux x pnr", overwrite=True
    )
    my_project.create_extra_attribute(
        "LINK", "@volax_x_pnr", description="aux x pnr", overwrite=True
    )
    my_project.create_extra_attribute(
        "LINK", "@volax_y_pnr", description="aux x pnr", overwrite=True
    )
    my_project.create_extra_attribute(
        "LINK", "@volax_z_pnr", description="aux (x pnr", overwrite=True
    )

    ##############################
    # Process auto access portion (trips TO Park and Rides)

    spec = json.load(open(spec_dir / "pnr_assignment_spec.json"))
    assignment(spec, my_project, pnr_access_class_name)

    spec1 = json.load(open(spec_dir / "pnr_skim_1.json"))
    spec2 = json.load(open(spec_dir / "pnr_skim_2.json"))
    skim(spec1, spec2, my_project, pnr_access_class_name)

    ##############################
    # Process auto egress portion (trips FROM Park and Rides)
    spec = json.load(open(spec_dir / "pnr_assignment_spec_egress.json"))
    assignment(spec, my_project, pnr_egress_class_name)

    spec1 = json.load(open(spec_dir / "pnr_skim_1_egress.json"))
    spec2 = json.load(open(spec_dir / "pnr_skim_2_egress.json"))
    skim(spec1, spec2, my_project, pnr_egress_class_name)

    for asim_name, matrix_name in matrix_dict.items():
        print(asim_name)
        try:
            matrix_value = emmeMatrix_to_numpyMatrix(
                matrix_name, my_project.bank, "uint16", 1
            )
            results_dict[asim_name + "__" + asim_tod] = matrix_value
            # h5file.create_dataset(asim_name+'__'+asim_tod, data=matrix_value.astype('uint16'),compression='gzip')
        except:
            print(matrix_name)

    # Transfer Time
    # Total wait time minus initial wait time gives transfer time
    # DRV_TRN_WLK_XWAIT: pnr_total_wait_access-pnr_initial_wait_access
    # WLK_TRN_DRV_XWAIT:pnr_total_wait_egress-pnr_initial_wait_egress

    pnr_total_wait_access = emmeMatrix_to_numpyMatrix(
        "pnr_total_wait_access", my_project.bank, "uint16", 1
    )
    pnr_initial_wait_access = emmeMatrix_to_numpyMatrix(
        "pnr_initial_wait_access", my_project.bank, "uint16", 1
    )
    pnr_transfer_wait_access = pnr_total_wait_access - pnr_initial_wait_access
    results_dict["DRV_TRN_WLK_XWAIT__" + asim_tod] = pnr_transfer_wait_access
    # h5file.create_dataset('DRV_TRN_WLK_XWAIT__'+asim_tod,
    #                               data=pnr_transfer_wait_access.astype('uint16'),compression='gzip')

    pnr_total_wait_egress = emmeMatrix_to_numpyMatrix(
        "pnr_total_wait_egress", my_project.bank, "uint16", 1
    )
    pnr_initial_wait_egress = emmeMatrix_to_numpyMatrix(
        "pnr_initial_wait_egress", my_project.bank, "uint16", 1
    )
    pnr_transfer_wait_egress = pnr_total_wait_egress - pnr_initial_wait_egress
    results_dict["WLK_TRN_DRV_XWAIT__" + asim_tod] = pnr_transfer_wait_egress
    # h5file["Skims"].create_dataset('WLK_TRN_DRV_XWAIT__'+asim_tod,
    #                               data=pnr_transfer_wait_egress.astype('uint16'),compression='gzip')

    # del my_project
    return results_dict


def ferry_assignment(my_project, ferry_spec_dir, class_name):
    spec = json.load(open(ferry_spec_dir / "asim_ferry_assignment.json"))
    assignment(spec, my_project, class_name)
    # Skims:
    skim_ferry = my_project.m.tool(
        "inro.emme.transit_assignment.extended.matrix_results"
    )

    skim_list = ["b", "c", "f", "r", "components"]
    for skim in skim_list:
        skim_ferry(
            json.load(open(ferry_spec_dir / f"asim_ferry_skim_{skim}.json")),
            class_name=class_name,
        )


def all_transit_assignment(
    my_project, transit_spec_dir, class_name, matrix_list, results_dict, asim_tod
):
    spec = json.load(open(transit_spec_dir / "all_transit_assignment.json"))
    spec["demand"] = "WLK_TRN_WLK_DEMAND"
    assignment(spec, my_project, class_name)
    # Skims:
    skim = my_project.m.tool("inro.emme.transit_assignment.extended.matrix_results")
    skim(
        json.load(open(transit_spec_dir / "all_transit_skim_spec.json")),
        class_name=class_name,
    )
    # Export
    for name in matrix_list:
        print(name)
        try:
            matrix_value = emmeMatrix_to_numpyMatrix(name, my_project.bank, "uint16", 1)
            results_dict[name + "__" + asim_tod] = matrix_value
            # h5file.create_dataset(name+'__'+asim_tod, data=matrix_value.astype('uint16'),compression='gzip')
            # f[name+'__'+asim_tod] = matrix_value
        except:
            print(name)

    return results_dict


def create_transit_matrices(my_project, asim_tod, matrix_list):

    for matrix_name in matrix_list:
        # Delete matrix if it already exists
        try:
            my_project.delete_matrix(my_project.bank.matrix(matrix_name).id)
            print("-----")
            print(matrix_name)
        except:
            pass
        my_project.create_matrix(matrix_name, "", "FULL")
        print(matrix_name)


def create_supplemental_skims(
    my_project: EmmeProject,
    skims_file_path: Path,
    spec_dir: Path,
    time_period_lookup: dict,
):
    # my_project = EmmeProject(project_path)
    results_dict = {}
    pnr_spec_dir = spec_dir / "park_and_ride"
    ferry_spec_dir = spec_dir / "ferry"
    all_transit_dir = spec_dir / "all_transit"

    matrix_list = [
        "WLK_TRN_WLK_DEMAND",
        "WLK_TRN_WLK_WAUX",
        "WLK_TRN_WLK_TWAIT",
        "WLK_TRN_WLK_IVT",
    ]

    # h5file = h5py.File(skims_file_path/'supplemental_skims.h5', "w")
    # h5file.create_group('Skims')

    for asim_tod, sc_tod in time_period_lookup.items():
        print(asim_tod)
        print(sc_tod)
        my_project.change_active_database(sc_tod)
        park_and_ride_skims(my_project, sc_tod, pnr_spec_dir, results_dict, asim_tod)
        create_transit_matrices(my_project, asim_tod, matrix_list)
        if asim_tod != "NI":
            ferry_assignment(my_project, ferry_spec_dir, ferry_class_name)
            all_transit_assignment(
                my_project,
                all_transit_dir,
                all_transit_class_name,
                matrix_list,
                results_dict,
                asim_tod,
            )
    f = omx.open_file(skims_file_path, "w")
    for skim_matrix in results_dict.keys():
        print(skim_matrix)
        # statsDict= {attr:getattr(skim_matrix,attr)() for attr in ['min', 'max','mean','std']}
        f[skim_matrix] = results_dict[skim_matrix]

    f.close()


################################################
# Write skims to file in activitysim format
# h5file = h5py.File('pnr_skims.h5', "w")
# h5file.create_group('Skims')

# my_project = EmmeProject('C:\Workspace\sc_park_and_ride2\soundcast\projects\LoadTripTables/LoadTripTables.emp')
# pnr_spec_dir = Path('R:/e2projects_two/activitysim/assignment_skims_inputs/park_and_ride')
# ferry_spec_dir = Path('R:/e2projects_two/activitysim/assignment_skims_inputs/ferry')
# all_transit_dir = Path('R:/e2projects_two/activitysim/assignment_skims_inputs/all_transit')

# pnr_access_class_name = 'pnr_access'
# pnr_egress_class_name = 'pnr_egress'
# ferry_class_name = 'asim_ferry'
# all_transit_class_name = 'all_transit'

# # all transit:
# matrix_list = ["WLK_TRN_WLK_DEMAND", "WLK_TRN_WLK_WAUX", "WLK_TRN_WLK_TWAIT", "WLK_TRN_WLK_IVT"]

# for asim_tod, sc_tod in tod_dict.items():

#     print(asim_tod)
#     print(sc_tod)

#     my_project.change_active_database(sc_tod)

#     park_and_ride_skims(my_project, sc_tod)
#     create_transit_matrices(my_project, asim_tod, matrix_list)
#     if asim_tod != 'NI':
#         ferry_assignment(my_project, ferry_spec_dir, ferry_class_name)
#         all_transit_assignment(my_project, all_transit_dir, all_transit_class_name, matrix_list)


# h5file.close()


# for mat in range(130,150):
#    try:
#        my_project.delete_matrix('mf'+str(max))
#    except:
#        pass
