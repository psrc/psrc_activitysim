import glob
import logging
import os
import time

import numpy as np
import openmatrix
import sharrow as sh
from activitysim.core import workflow
from activitysim.core.los import LOS_SETTINGS_FILE_NAME
from zarr.errors import GroupNotFoundError

logger = logging.getLogger("activitysim.custom.skims_preprocess")


@workflow.step(overloading=True)
def skims_preprocess(state: workflow.State):
    """
    Simulate multiple VOT when missing.
    """
    los_settings = state.filesystem.read_settings_file(
        LOS_SETTINGS_FILE_NAME, mandatory=True
    )
    skim_settings = los_settings["taz_skims"]
    cache_dir = state.filesystem.get_cache_dir()
    skims_zarr = skim_settings.get("zarr", None)
    if skims_zarr is None:
        raise ValueError(
            f"skims_preprocess requires taz_skims.zarr in {LOS_SETTINGS_FILE_NAME}"
        )
    try:
        ds_check = sh.dataset.from_zarr_with_attr(os.path.join(cache_dir, skims_zarr))
    except (GroupNotFoundError, FileNotFoundError):
        regenerate = True
        zarr_write_time = 0
    else:
        zarr_write_time = ds_check.attrs.get("ZARR_WRITE_TIME", 0)
        regenerate = False
        check_names = [
            "SOV_DIST_L",
            "SOV_DIST_M",
            "SOV_DIST_H",
            'HOV2_DIST_L',
            "HOV3_DIST_L",
            "SOV_TIME_L",
            "HOV2_TIME_L",
            "HOV3_TIME_L",
            "SOVTOLL_VTOLL_L",
            "HOV2TOLL_VTOLL_L",
            "HOV3TOLL_VTOLL_L",
        ]
        for n in check_names:
            if n not in ds_check:
                regenerate = True
                break

    if isinstance(skim_settings, str):
        skims_omx_fileglob = skim_settings
    else:
        skims_omx_fileglob = skim_settings.get("omx", None)
        skims_omx_fileglob = skim_settings.get("files", skims_omx_fileglob)
    skims_filenames = []
    for d in state.filesystem.get_data_dir():
        skims_filenames.extend(glob.glob(os.path.join(d, skims_omx_fileglob)))

    if not regenerate:
        # check file modification times
        # if any OMX file was modified more recently than the cached zarr, rewrite the zarr
        for skims_filename in skims_filenames:
            if os.path.getmtime(skims_filename) > zarr_write_time:
                logger.warning(
                    f"OMX file {skims_filename} has been modified more recently than cached ZARR"
                )
                regenerate = True
                break

    if not regenerate:
        logger.warning(f"OMX files are all older than cached ZARR, using cache")
        return

    index_names = ("otaz", "dtaz", "time_period")
    indexes = None
    time_period_breaks = los_settings.get("skim_time_periods", {}).get("periods")
    time_periods_raw = los_settings.get("skim_time_periods", {}).get("labels")
    time_periods = np.unique(time_periods_raw)
    time_period_sep = "__"

    time_window = los_settings.get("skim_time_periods", {}).get("time_window")
    period_minutes = los_settings.get("skim_time_periods", {}).get("period_minutes")
    n_periods = int(time_window / period_minutes)

    tp_map = {}
    tp_imap = {}
    label = time_periods_raw[0]
    i = 0
    for t in range(n_periods):
        if t in time_period_breaks:
            i = time_period_breaks.index(t)
            label = time_periods_raw[i]
        tp_map[t + 1] = label
        tp_imap[t + 1] = i
    tp_map[-1] = tp_map[1]
    tp_imap[-1] = tp_imap[1]

    omxs = [
        openmatrix.open_file(skims_filename, mode="r")
        for skims_filename in skims_filenames
    ]
    try:
        if isinstance(time_periods, (list, tuple)):
            time_periods = np.asarray(time_periods)
        ds = sh.dataset.from_omx_3d(
            omxs,
            index_names=index_names,
            indexes=indexes,
            time_periods=time_periods,
            time_period_sep=time_period_sep,
        )

        ds['SOV_DIST_L'] = ds['SOV_DIST']
        ds['SOV_DIST_M'] = ds['SOV_DIST']
        ds['SOV_DIST_H'] = ds['SOV_DIST']
        ds['SOVTOLL_VTOLL_L'] = ds['SOVTOLL_VTOLL']
        ds['SOVTOLL_VTOLL_M'] = ds['SOVTOLL_VTOLL']
        ds['SOVTOLL_VTOLL_H'] = ds['SOVTOLL_VTOLL']
        ds['SOV_TIME_L'] = ds['SOV_TIME']
        ds['SOV_TIME_M'] = ds['SOV_TIME']
        ds['SOV_TIME_H'] = ds['SOV_TIME']
        ds['HOV2_DIST_L'] = ds['HOV2_DIST']
        ds['HOV2_DIST_M'] = ds['HOV2_DIST']
        ds['HOV2_DIST_H'] = ds['HOV2_DIST']
        ds['HOV2TOLL_VTOLL_L'] = ds['HOV2TOLL_VTOLL']
        ds['HOV2TOLL_VTOLL_M'] = ds['HOV2TOLL_VTOLL']
        ds['HOV2TOLL_VTOLL_H'] = ds['HOV2TOLL_VTOLL']
        ds['HOV3TOLL_VTOLL_L'] = ds['HOV3TOLL_VTOLL']
        ds['HOV3TOLL_VTOLL_M'] = ds['HOV3TOLL_VTOLL']
        ds['HOV3TOLL_VTOLL_H'] = ds['HOV3TOLL_VTOLL']
        ds['HOV2_TIME_L'] = ds['HOV2_TIME']
        ds['HOV2_TIME_M'] = ds['HOV2_TIME']
        ds['HOV2_TIME_H'] = ds['HOV2_TIME']
        ds['HOV3_TIME_L'] = ds['HOV3_TIME']
        ds['HOV3_TIME_M'] = ds['HOV3_TIME']
        ds['HOV3_TIME_H'] = ds['HOV3_TIME']
        ds['HOV3_DIST_L'] = ds['HOV3_DIST']
        ds['HOV3_DIST_M'] = ds['HOV3_DIST']
        ds['HOV3_DIST_H'] = ds['HOV3_DIST']

        ds.attrs["time_period_map"] = tp_map
        ds.attrs["time_period_imap"] = tp_imap
        ds.attrs["ZARR_WRITE_TIME"] = time.time()

        cache_dir = state.filesystem.get_cache_dir()
        logger.info(f"cache_dir = {os.path.join(cache_dir, skims_zarr)}")
        ds.to_zarr_with_attr(os.path.join(cache_dir, skims_zarr), mode="w")

    finally:
        for f in omxs:
            f.close()

    logger.info("skims preprocess complete")
