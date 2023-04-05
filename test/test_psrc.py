# ActivitySim
# See full license in LICENSE.txt.
import sys
import time
from pathlib import Path
import subprocess

import pandas as pd

from activitysim.core import workflow

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

    # persisting sharrow cache in local output speeds testing runtimes
    # on local machines.  CI testing won't have a pre-built cache, so
    # it will be slower but more reliable (and will not be blocking a
    # developer's individual computer).
    sharrow_cache_dir = test_dir.joinpath("output", "cache")
    sharrow_cache_dir.mkdir(parents=True, exist_ok=True)
    state.filesystem.sharrow_cache_dir = sharrow_cache_dir

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
