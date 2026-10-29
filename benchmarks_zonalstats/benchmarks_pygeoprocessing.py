"""Benchmark zonal statistics using PyGeoprocessing."""

import logging
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pygeoprocessing.geoprocessing

import testdata
from benchmarker import RunResult
from benchmarks_zonalstats import _common

logger = logging.getLogger(__name__)


def _get_package() -> str:
    return "pygeoprocessing"


def _get_version() -> str:
    return f"{pygeoprocessing.__version__}".replace("v", "")  # type: ignore


def zonalstats_1band(tmp_dir: Path) -> list[RunResult]:
    """Benchmark PyGeoprocessing statistics for one raster band."""
    # Init
    results = []
    vector_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    raster_path, _ = testdata.TestFile.S2_NDVI_2020.get_file(tmp_dir)

    # Prepare a sample of the parcels, otherwise to slow
    nb_poly = _common.nb_polygons_for_test
    vector_gdf = gpd.read_file(vector_path, rows=slice(0, nb_poly))
    vector_tmp_path = tmp_dir / "vector_input.gpkg"
    vector_gdf.to_file(vector_tmp_path)

    # Go!
    start_time = datetime.now()
    #  1.000: 10s
    # 10.000: 97s
    stats = pygeoprocessing.geoprocessing.zonal_statistics(
        (str(raster_path), 1), str(vector_tmp_path), polygons_might_overlap=True
    )
    print(stats)

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

    logger.info(f"took {secs_taken:.2f}s for {nb_poly} polygons, {len(stats)} results")

    # Return
    return results
