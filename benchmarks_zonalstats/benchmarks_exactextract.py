"""
Module to benchmark zonalstats.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List

import exactextract
import geopandas as gpd
import rasterio

from benchmarker import RunResult
from benchmarks_zonalstats import _common as common
import testdata

logger = logging.getLogger(__name__)


def _get_package() -> str:
    return "exactextract"


def _get_version() -> str:
    return f"{exactextract.__version__}".replace("v", "")


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

    # Extract single band of raster, as exactextract doesn't support specifying one band
    raster_tmp_path = tmp_dir / "raster_1band.tif"
    with rasterio.open(raster_path) as src:
        single_band = src.read(1)
        out_meta = src.meta.copy()
        out_meta.update({"count": 1})

        # Write single band
        with rasterio.open(raster_tmp_path, "w", **out_meta) as dest:
            dest.write(single_band, 1)

        raster_path = raster_tmp_path

    # Go!
    start_time = datetime.now()
    stats = exactextract.exact_extract(
        raster_path,
        vector_tmp_path,
        [
            "count",
            "mean",
            "min",
            "max",
            "count(coverage_weight=none)",
            # "mean(coverage_weight=none)",
            # "min(coverage_weight=none)",
            # "max(coverage_weight=none)",
        ],
    )
    # print(stats)
    assert len(stats) == nb_poly

    secs_taken = (datetime.now() - start_time).total_seconds()
    results.append(
        RunResult(
            package=_get_package(),
            package_version=_get_version(),
            operation="zonalstats_1band",
            secs_taken=secs_taken,
            operation_descr=(
                f"zonalstats of agri parcels ({nb_poly} polygons) + S2 NDVI RGB BEFL"
            ),
            run_details={},
        )
    )

    logger.info(f"took {secs_taken:.2f}s for {nb_poly} polygons, {len(stats)} results")

    # Return
    return results


def zonalstats_3bands(tmp_dir: Path) -> List[RunResult]:
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
    stats = exactextract.exact_extract(
        raster_path,
        vector_tmp_path,
        [
            "count",
            "mean",
            "min",
            "max",
            # "count(coverage_weight=none)",
            # "mean(coverage_weight=none)",
            # "min(coverage_weight=none)",
            # "max(coverage_weight=none)",
        ],
    )
    # print(stats)
    assert len(stats) == nb_poly

    secs_taken = (datetime.now() - start_time).total_seconds()
    results.append(
        RunResult(
            package=_get_package(),
            package_version=_get_version(),
            operation="zonalstats_3bands",
            secs_taken=secs_taken,
            operation_descr=(
                f"zonalstats of agri parcels ({nb_poly} polygons) + S2 NDVI RGB BEFL"
            ),
            run_details={},
        )
    )

    logger.info(f"took {secs_taken:.2f}s for {nb_poly} polygons, {len(stats)} results")

    # Return
    return results
