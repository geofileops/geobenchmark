"""
Module to benchmark zonalstats.
"""
import os
from datetime import datetime
import logging
from pathlib import Path
from typing import List

import pandas as pd
import geopandas as gpd
import pyjeo as pj

from benchmarker import RunResult
from benchmarks_zonalstats import _common as common
import testdata

logger = logging.getLogger(__name__)


def _get_package() -> str:
    return "pyjeo"


def _get_version() -> str:
    nproc = os.environ.get("OMP_NUM_THREADS")
    if nproc is not None:
        return f"{pj.__version__}-{nproc} threads".replace("v", "")
    else:
        return f"{pj.__version__}".replace("v", "")


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
    vector_data = pj.JimVect(vector_tmp_path)
    jim = pj.Jim(raster_path, band=0)
    stats = pj.geometry.extract(
        vector_data,
        jim,
        # rule=["mean", "stdev", "count"],
        rule=["mean", "stdev", "sum"],
        output="/vsimem/pj.json",
        oformat="GeoJSON",
    )

    print(pd.DataFrame(stats.dict()))

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
        f"took {secs_taken:.2f}s for {nb_poly} polygons, "
        f"{stats.properties.getFeatureCount()} results"
    )

    # Return
    return results
