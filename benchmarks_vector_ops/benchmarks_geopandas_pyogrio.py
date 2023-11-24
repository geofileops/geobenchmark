"""
Module to benchmark geopandas operations using pyogrio for IO.
"""

from datetime import datetime
import inspect
import logging
from pathlib import Path

from geofileops.util import _geoseries_util
import geopandas as gpd
import pyogrio

from benchmarker import RunResult
import testdata

logger = logging.getLogger(__name__)


def _get_package() -> str:
    return "geopandas_pyogrio"


def _get_version() -> str:
    return f"{gpd.__version__}_{pyogrio.__version__}".replace("v", "")


def buffer(tmp_dir: Path) -> RunResult:
    # Init
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    # Read input file
    start_time = datetime.now()
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")

    # Buffer
    start_time_buffer = datetime.now()
    gdf.geometry = gdf.geometry.buffer(distance=1, resolution=5)
    logger.info(
        f"time for buffer: {(datetime.now()-start_time_buffer).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    gdf.geometry = _geoseries_util.harmonize_geometrytypes(gdf.geometry)
    output_path = tmp_dir / f"{input_path.stem}_geopandas_buf.gpkg"
    pyogrio.write_dataframe(gdf, output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
        operation="buffer",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="buffer agri parcels BEFL (~500k polygons)",
    )

    # Cleanup
    output_path.unlink()

    return result


def dissolve(tmp_dir: Path) -> RunResult:
    # Init
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    # Read input file
    start_time = datetime.now()
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")

    # dissolve
    start_time_dissolve = datetime.now()
    result_gdf = gdf.dissolve()
    assert isinstance(result_gdf, gpd.GeoDataFrame)
    result_gdf = result_gdf.explode(ignore_index=True)
    logger.info(
        f"time for dissolve: {(datetime.now()-start_time_dissolve).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_geopandas_diss.gpkg"
    pyogrio.write_dataframe(
        result_gdf, output_path, layer=output_path.stem, driver="GPKG"
    )
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
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
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")

    # dissolve
    start_time_dissolve = datetime.now()
    result_gdf = gdf.dissolve(by="GEWASGROEP")
    assert isinstance(result_gdf, gpd.GeoDataFrame)
    result_gdf = result_gdf.explode(ignore_index=True)
    logger.info(
        f"time for dissolve: {(datetime.now()-start_time_dissolve).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_geopandas_diss_groupby.gpkg"
    pyogrio.write_dataframe(
        result_gdf, output_path, layer=output_path.stem, driver="GPKG"
    )
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
        operation="dissolve_groupby",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr=(
            "dissolve on agri parcels BEFL (~500k polygons), groupby=GEWASGROEP"
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
    input1_gdf = pyogrio.read_dataframe(input1_path)
    input2_gdf = pyogrio.read_dataframe(input2_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")

    # intersection
    start_time_op = datetime.now()
    result_gdf = input1_gdf.overlay(input2_gdf, how="intersection")
    logger.info(
        f"time for intersection: {(datetime.now()-start_time_op).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    result_gdf.geometry = _geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}.gpkg"
    pyogrio.write_dataframe(
        result_gdf, output_path, layer=output_path.stem, driver="GPKG"
    )
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    secs_taken = (datetime.now() - start_time).total_seconds()
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
        operation="intersection",
        secs_taken=secs_taken,
        operation_descr="intersection of 2 agri parcel layers BEFL (2*~500k polygons)",
    )

    # Cleanup and return
    output_path.unlink()
    return result


def symdif_complexpolys_agri(tmp_dir: Path) -> RunResult:
    # Init
    function_name = inspect.currentframe().f_code.co_name  # type: ignore[union-attr]

    input1_path, input1_descr = testdata.TestFile.COMPLEX_POLYS.get_file(tmp_dir)
    input2_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    start_time = datetime.now()
    input1_gdf = pyogrio.read_dataframe(input1_path)
    input2_gdf = pyogrio.read_dataframe(input2_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")

    # symmetric_difference
    start_time_op = datetime.now()
    result_gdf = input1_gdf.overlay(input2_gdf, how="symmetric_difference")
    logger.info(
        "time for symmetric_difference: "
        f"{(datetime.now()-start_time_op).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    result_gdf.geometry = _geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    output_path = tmp_dir / f"{input1_path.stem}_symdif_{input2_path.stem}.gpkg"
    pyogrio.write_dataframe(
        result_gdf, output_path, layer=output_path.stem, driver="GPKG"
    )
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    secs_taken = (datetime.now() - start_time).total_seconds()
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
        operation=function_name,
        secs_taken=secs_taken,
        operation_descr=(
            f"{function_name} between {input1_descr} and agriparcels BEFL (~500k poly)"
        ),
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
    input1_gdf = pyogrio.read_dataframe(input1_path)
    input2_gdf = pyogrio.read_dataframe(input2_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")

    # union
    start_time_op = datetime.now()
    result_gdf = input1_gdf.overlay(input2_gdf, how="union")
    logger.info(f"time for union: {(datetime.now()-start_time_op).total_seconds()}")

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    result_gdf.geometry = _geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    output_path = tmp_dir / f"{input1_path.stem}_union_{input2_path.stem}.gpkg"
    pyogrio.write_dataframe(
        result_gdf, output_path, layer=output_path.stem, driver="GPKG"
    )
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    secs_taken = (datetime.now() - start_time).total_seconds()
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
        operation="union",
        secs_taken=secs_taken,
        operation_descr="union of 2 agri parcel layers BEFL (2*~500k polygons)",
    )

    # Cleanup and return
    output_path.unlink()
    return result
