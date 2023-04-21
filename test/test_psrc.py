# ActivitySim
# See full license in LICENSE.txt.
import sys
import time
from pathlib import Path
import subprocess

import pandas as pd

from activitysim.core import testing, workflow

test_dir = Path(__file__).parent
top_dir = test_dir.parent


def _test_psrc(tmp_path, dataframe_regression, mp=False, use_sharrow=True):
    import activitysim.abm  # noqa: F401 # register steps

    if mp:
        configs_dir = (
            test_dir.joinpath("configs_mp"),
            test_dir.joinpath("configs"),
            top_dir.joinpath("configs_dev"),
        )
    else:
        configs_dir = (
            test_dir.joinpath("configs"),
            top_dir.joinpath("configs_dev"),
        )

    settings = {}
    if use_sharrow:
        settings["sharrow"] = "test"

    # Writing output for each test to a clean temporary directory, not
    # a pre-existing output directory that might have residual content
    # from a prior test run.
    state = workflow.State.make_default(
        working_dir=top_dir,
        configs_dir=configs_dir,
        data_dir=top_dir.joinpath("data"),
        output_dir=tmp_path,  # test_dir.joinpath("output"),
        settings=settings,
    )

    # IF extensions are ever included, this is how to tell ActivitySim
    # state.import_extensions("extensions")

    # persisting sharrow cache locally speeds testing runtimes
    # on local machines.  CI testing won't have a pre-built cache, so
    # it will be slower but more reliable (and will not be blocking a
    # developer's individual computer).
    state.filesystem.persist_sharrow_cache()

    state.run.all()

    # Check that result tables all match as expected.
    for t in ("trips", "tours", "persons", "households"):
        df = state.get_dataframe(t)
        # check that in-memory result table is as expected
        try:
            dataframe_regression.check(df, basename=t)
        except AssertionError:
            print(f"AssertionError for {t!r} table", file=sys.stderr)
            raise

# If it is necessary to update/regenerate the target regression files, run
# pytest with the `--regen-all` flag.

def test_psrc(tmp_path, dataframe_regression):
    return _test_psrc(tmp_path, dataframe_regression, mp=False, use_sharrow=False)


def test_psrc_sh(tmp_path, dataframe_regression):
    return _test_psrc(tmp_path, dataframe_regression, mp=False, use_sharrow=True)


def test_psrc_mp(tmp_path, dataframe_regression):
    return _test_psrc(tmp_path, dataframe_regression, mp=True, use_sharrow=False)


def test_psrc_sh_mp(tmp_path, dataframe_regression):
    return _test_psrc(tmp_path, dataframe_regression, mp=True, use_sharrow=True)


def test_psrc_cli(tmp_path, dataframe_regression, original_datadir: Path):
    """
    Test running the PSRC mini-model using the command line interface.
    """
    tag = time.strftime("%Y-%m-%d-%H%M%S")
    tmp_path = Path(f"/tmp/psrc-cli/{tag}")
    cli_args = [
        "-m",
        "activitysim",
        "run",
        "-c",
        test_dir.joinpath("configs"),
        "-c",
        top_dir.joinpath("configs_dev"),
        "-d",
        top_dir.joinpath("data"),
        "-o",
        tmp_path,
    ]
    subprocess.run([sys.executable] + cli_args, check=True)

    # Check that result tables all match as expected.
    for t in ("trips", "tours", "persons", "households"):
        df = pd.read_csv(tmp_path / f"final_{t}.csv", index_col=f"{t[:-1]}_id")
        expected = pd.read_csv(original_datadir / f"{t}.csv", index_col=f"{t[:-1]}_id")
        # check that in-memory result table is as expected
        try:
            pd.testing.assert_frame_equal(df.sort_index(), expected.sort_index())
        except AssertionError:
            print(f"AssertionError for {t!r} table", file=sys.stderr)
            raise

EXPECTED_MODELS = [
    "initialize_landuse",
    "compute_accessibility",
    "initialize_households",
    "school_location",
    "work_from_home",
    "workplace_location",
    "auto_ownership_simulate",
    "free_parking",
    "telecommute_frequency",
    "cdap_simulate",
    "mandatory_tour_frequency",
    "mandatory_tour_scheduling",
    "joint_tour_frequency",
    "joint_tour_composition",
    "joint_tour_participation",
    "joint_tour_destination",
    "joint_tour_scheduling",
    "non_mandatory_tour_frequency",
    "non_mandatory_tour_destination",
    "non_mandatory_tour_scheduling",
    "tour_mode_choice_simulate",
    "atwork_subtour_frequency",
    "atwork_subtour_destination",
    "atwork_subtour_scheduling",
    "atwork_subtour_mode_choice",
    "stop_frequency",
    "trip_purpose",
    "trip_destination",
    "trip_purpose_and_destination",
    "trip_scheduling",
    "trip_mode_choice",
    "write_data_dictionary",
    "track_skim_usage",
    "write_trip_matrices",
    "write_tables",
]

@testing.run_if_exists("prototype_psrc_reference_pipeline.zip")
def test_psrc_progressive(tmp_path):

    import activitysim.abm  # register components # noqa: F401

    configs_dir = (
        test_dir.joinpath("configs"),
        top_dir.joinpath("configs_dev"),
    )
    state = workflow.State.make_default(
        working_dir=top_dir,
        configs_dir=configs_dir,
        data_dir=top_dir.joinpath("data"),
        output_dir=tmp_path,  # test_dir.joinpath("output"),
    )
    state.filesystem.persist_sharrow_cache()

    assert state.settings.models == EXPECTED_MODELS
    assert state.settings.chunk_size == 0
    assert state.settings.sharrow is False

    for step_name in EXPECTED_MODELS:
        state.run.by_name(step_name)
        try:
            state.checkpoint.check_against(
                Path(__file__).parent.joinpath("prototype_psrc_reference_pipeline.zip"),
                checkpoint_name=step_name,
            )
        except Exception:
            print(f"> prototype_psrc {step_name}: ERROR")
            raise
        else:
            print(f"> prototype_psrc {step_name}: ok")
