"""
Module to benchmark geopandas operations.
"""

from datetime import datetime
import inspect
import logging
from pathlib import Path

from geofileops.util import _geoseries_util as geoseries_util
import geofileops as gfo
import geopandas as gpd
import shapely

from benchmarker import RunResult
import testdata

logger = logging.getLogger(__name__)

_package = "geopandas"
_package_version = gpd.__version__


def buffer(tmp_dir: Path) -> RunResult:
    # Init
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    # Read input file
    start_time = datetime.now()
    gdf = gpd.read_file(input_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # Buffer
    start_time_buffer = datetime.now()
    gdf.geometry = gdf.geometry.buffer(distance=1, resolution=5)
    logger.info(
        f"time for buffer: {(datetime.now() - start_time_buffer).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_geopandas_buf.gpkg"
    gdf.to_file(output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now() - start_time_write).total_seconds()}")
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="buffer",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="buffer agri parcels BEFL (~500k polygons)",
    )

    # Cleanup and return
    output_path.unlink()
    return result


def _clip(tmp_dir: Path) -> RunResult:
    """
    On the current test datasets, clip with geopandas runs for days without result,
    so no use to activate benchmark
    """
    # Init
    input1_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path, _ = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    # Go!
    # Read input files
    start_time = datetime.now()
    input1_gdf = gpd.read_file(input1_path)
    input2_gdf = gpd.read_file(input2_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # clip
    start_time_op = datetime.now()
    result_gdf = gpd.clip(input1_gdf, input2_gdf, keep_geom_type=True)
    logger.info(f"time for clip: {(datetime.now() - start_time_op).total_seconds()}")

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    assert isinstance(result_gdf.geometry, gpd.GeoSeries)
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    output_path = tmp_dir / f"{input1_path.stem}_clip_{input2_path.stem}.gpkg"
    result_gdf.to_file(output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now() - start_time_write).total_seconds()}")
    secs_taken = (datetime.now() - start_time).total_seconds()
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="clip",
        secs_taken=secs_taken,
        operation_descr="clip of 2 agri parcel layers BEFL (2*~500k polygons)",
    )

    # Cleanup and return
    output_path.unlink()
    return result


def dissolve(tmp_dir: Path) -> RunResult:
    # Init
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    # Read input file
    start_time = datetime.now()
    gdf = gpd.read_file(input_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # dissolve
    start_time_dissolve = datetime.now()
    result_gdf = gdf.dissolve()
    assert isinstance(result_gdf, gpd.GeoDataFrame)
    result_gdf = result_gdf.explode(ignore_index=True)
    logger.info(
        f"time for dissolve: {(datetime.now() - start_time_dissolve).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_geopandas_diss.gpkg"
    result_gdf.to_file(output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now() - start_time_write).total_seconds()}")
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="dissolve",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="dissolve agri parcels BEFL (~500k polygons)",
    )

    # Cleanup and return
    output_path.unlink()
    return result


def dissolve_groupby(tmp_dir: Path) -> RunResult:
    # Init
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    # Read input file
    start_time = datetime.now()
    gdf = gpd.read_file(input_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # dissolve
    start_time_dissolve = datetime.now()
    result_gdf = gdf.dissolve(by="GWSGRPH_LB")
    assert isinstance(result_gdf, gpd.GeoDataFrame)
    result_gdf = result_gdf.explode(ignore_index=True)
    logger.info(
        f"time for dissolve: {(datetime.now() - start_time_dissolve).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_geopandas_diss_groupby.gpkg"
    result_gdf.to_file(output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now() - start_time_write).total_seconds()}")
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="dissolve_groupby",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr=(
            "dissolve on agri parcels BEFL (~500k polygons), groupby=GWSGRPH_LB"
        ),
    )

    # Cleanup and return
    output_path.unlink()
    return result


def intersection(tmp_dir: Path) -> RunResult:
    # Init
    input1_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path, _ = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    # Go!
    # Read input files
    start_time = datetime.now()
    input1_gdf = gpd.read_file(input1_path)
    input2_gdf = gpd.read_file(input2_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # intersection
    start_time_op = datetime.now()
    result_gdf = input1_gdf.overlay(input2_gdf, how="intersection")
    logger.info(
        f"time for intersection: {(datetime.now() - start_time_op).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}.gpkg"
    result_gdf.to_file(output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now() - start_time_write).total_seconds()}")
    secs_taken = (datetime.now() - start_time).total_seconds()
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="intersection",
        secs_taken=secs_taken,
        operation_descr="intersection of 2 agri parcel layers BEFL (2*~500k polygons)",
    )

    # Cleanup and return
    output_path.unlink()
    return result


def _symdif_complexpolys_agri(tmp_dir: Path) -> RunResult:
    """Symmetric difference between very complex polygons and standard polygons.

    The complex polygons are created as such:

        - each geometry is a multipolygon with several polygons
        - each polygon has a large number of points
        - the area for each (multipolygon) is a lot larger than the standard polygons,
          so each complex polygon interacts with many standard polygons

    Changelog:

        - 2025-01-23: make complex polygons a lot more complex to be able to see the
          difference between geofileops 0.9.1 and 0.10.0
    """
    # Init
    function_name = inspect.currentframe().f_code.co_name  # type: ignore[union-attr]

    input1_path, input1_descr = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    info1 = gfo.get_layerinfo(input1_path)
    bbox = shapely.box(*info1.total_bounds).buffer(-10_000, join_style="mitre").bounds
    crs = info1.crs
    input2_path, input2_descr = testdata.create_testfile(
        bbox=bbox,
        geoms=3,
        polys_per_geom=4,
        points_per_poly=30_000,
        poly_width=30_000,
        poly_height=30_000,
        crs=crs,
        dst_dir=tmp_dir,
    )

    start_time = datetime.now()
    input1_gdf = gpd.read_file(input1_path)
    input2_gdf = gpd.read_file(input2_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # symmetric_difference
    start_time_op = datetime.now()
    result_gdf = input1_gdf.overlay(input2_gdf, how="symmetric_difference")
    logger.info(
        "time for symmetric_difference: "
        f"{(datetime.now() - start_time_op).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    output_path = tmp_dir / f"{input1_path.stem}_symdif_{input2_path.stem}.gpkg"
    result_gdf.to_file(output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now() - start_time_write).total_seconds()}")
    secs_taken = (datetime.now() - start_time).total_seconds()
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation=function_name,
        secs_taken=secs_taken,
        operation_descr=f"{function_name} between {input1_descr} and {input2_descr}",
    )

    # Cleanup and return
    output_path.unlink()
    return result


def union(tmp_dir: Path) -> RunResult:
    # Init
    input1_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path, _ = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    # Go!
    # Read input files
    start_time = datetime.now()
    input1_gdf = gpd.read_file(input1_path)
    input2_gdf = gpd.read_file(input2_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # union
    start_time_union = datetime.now()
    result_gdf = input1_gdf.overlay(input2_gdf, how="union")
    logger.info(
        f"time for union: {(datetime.now() - start_time_union).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    output_path = tmp_dir / f"{input1_path.stem}_union_{input2_path.stem}.gpkg"
    result_gdf.to_file(output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now() - start_time_write).total_seconds()}")
    secs_taken = (datetime.now() - start_time).total_seconds()
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="union",
        secs_taken=secs_taken,
        operation_descr="union of 2 agri parcel layers BEFL (2*~500k polygons)",
    )

    # Cleanup and return
    output_path.unlink()
    return result
