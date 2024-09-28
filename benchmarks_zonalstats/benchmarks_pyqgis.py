"""
Module to benchmark zonalstats.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd
import qgis.core  # type: ignore
import qgis.analysis  # type: ignore

from benchmarker import RunResult
from benchmarks_zonalstats import _common as common
import testdata

logger = logging.getLogger(__name__)


def _get_package() -> str:
    return "pyqgis"


def _get_version() -> str:
    return f"{qgis.core.Qgis.QGIS_VERSION}".replace("v", "")


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

    # Inits app
    # QgsApplication.setPrefixPath(path_to_qgis, True)
    qgs = qgis.core.QgsApplication([], False)
    qgs.initQgis()

    # Reads the input file
    vlayer = qgis.core.QgsVectorLayer(str(vector_tmp_path), vector_tmp_path.stem, "ogr")

    if True:
        # Materializing the vector layer in memory gives a performance boost of 35%
        vlayer_mem = vlayer.materialize(
            qgis.core.QgsFeatureRequest().setFilterFids(vlayer.allFeatureIds())
        )
        del vlayer
        vlayer = vlayer_mem

    # Calculates zonal stats with raster
    raster = qgis.core.QgsRasterLayer(str(raster_path))
    stats = (
        qgis.analysis.QgsZonalStatistics.Count
        | qgis.analysis.QgsZonalStatistics.Sum
        | qgis.analysis.QgsZonalStatistics.Mean
    )
    zoneStats = qgis.analysis.QgsZonalStatistics(
        vlayer,
        raster,
        stats=stats,
        rasterBand=1,
    )
    zoneStats.calculateStatistics(None)

    columns = [f.name() for f in vlayer.fields()] + ["geometry"]
    # columns_types = [f.typeName() for f in vlayer.fields()]
    data = [
        dict(zip(columns, f.attributes() + [f.geometry().asWkt()]))
        for f in vlayer.getFeatures()
    ]

    df = pd.DataFrame(data, columns=columns)
    df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
    stats = gpd.GeoDataFrame(
        df,
        geometry="geometry",
        crs=vlayer.crs().toWkt(),  # type: ignore
    )
    del vlayer

    # print(stats)

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
