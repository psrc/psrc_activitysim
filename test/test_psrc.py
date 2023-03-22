# ActivitySim
# See full license in LICENSE.txt.
from pathlib import Path

from activitysim.core import workflow

test_dir = Path(__file__).parent
top_dir = test_dir.parent


def _test_psrc(tmp_path, dataframe_regression, mp=False):
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

    state = workflow.State.make_default(
        working_dir=top_dir,
        configs_dir=configs_dir,
        data_dir=top_dir.joinpath("data"),
        output_dir=tmp_path,  # test_dir.joinpath("output"),
    )
    state.import_extensions("extensions")

    # persist sharrow cache in local output
    sharrow_cache_dir = test_dir.joinpath("output", "cache")
    sharrow_cache_dir.mkdir(parents=True, exist_ok=True)
    state.filesystem.sharrow_cache_dir = sharrow_cache_dir

    state.run.all()

    for t in ("trips", "tours", "persons", "households"):
        df = state.get_dataframe(t)
        # check that in-memory result table is as expected
        dataframe_regression.check(df, basename=t)


def test_psrc(tmp_path, dataframe_regression):
    return _test_psrc(tmp_path, dataframe_regression, False)


def test_psrc_mp(tmp_path, dataframe_regression):
    return _test_psrc(tmp_path, dataframe_regression, True)
