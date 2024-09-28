"""
Module to benchmark zonalstats.
"""

from datetime import datetime
import logging
from pathlib import Path
from typing import List

import geopandas as gpd
import geowombat as gw

from benchmarker import RunResult
from benchmarks_zonalstats import _common as common
import testdata

logger = logging.getLogger(__name__)


def _get_package() -> str:
    return "geowombat"


def _get_version() -> str:
    return f"{gw.__version__}".replace("v", "")


def zonalstats_1band(tmp_dir: Path) -> List[RunResult]:
    # Init
    results = []
    vector_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    raster_path, _ = testdata.TestFile.S2_NDVI_2020.get_file(tmp_dir)

    # Prepare a sample of the parcels, otherwise to slow
    nb_poly = common.nb_polygons_for_test
    vector_gdf = gpd.read_file(vector_path, rows=slice(0, nb_poly))
    vector_tmp_path = tmp_dir / "vector_input.gpkg"
    vector_gdf.to_file(vector_tmp_path)

    # Go!
    start_time = datetime.now()
    #  1.000: 10s
    # 10.000: 97s
    stats_df = None
    # with gw.config.update(sensor="bgr"):

    # Remark: all bands are read, specifying only one band gives error?
    with gw.open(raster_path) as src:
        assert src is not None
        stats_df = src.gw.extract(str(vector_tmp_path), bands=[1])
        # use pandas groupby to calc pixel mean
        stats_df = stats_df[["id", 1]].groupby("id").mean()
    # print(stats_df)

    secs_taken = (datetime.now() - start_time).total_seconds()
    results.append(
        RunResult(
            package=_get_package(),
            package_version=_get_version(),
            operation="zonalstats_1band",
            secs_taken=secs_taken,
            operation_descr=(
                f"zonalstats of agri parcels ({nb_poly} polygons) + S2 NDVI BEFL"
            ),
            run_details={},
        )
    )

    logger.info(
        f"took {secs_taken:.2f}s for {nb_poly} polygons, {len(stats_df)} results"
    )

    # Return
    return results
