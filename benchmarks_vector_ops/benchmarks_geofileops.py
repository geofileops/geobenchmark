# -*- coding: utf-8 -*-
"""
Module to benchmark geofileops operations.
"""

from datetime import datetime
import inspect
import logging
import multiprocessing
from pathlib import Path

import geofileops as gfo
import geopandas as gpd
import shapely

from benchmarker import RunResult
import testdata

################################################################################
# Some init
################################################################################

logger = logging.getLogger(__name__)

################################################################################
# The real work
################################################################################


def _get_package() -> str:
    return "geofileops"


def _get_version() -> str:
    return gfo.__version__


def _get_nb_parallel() -> int:
    nb_parallel = 12
    nb_cores = multiprocessing.cpu_count()
    if nb_cores < nb_parallel:
        logger.warning(
            f"nb_parallel specified ({nb_parallel}) > nb logical cores available "
            f"({nb_cores})"
        )
    return nb_parallel


def buffer(tmp_dir: Path) -> RunResult:
    # Init
    input_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_buf.gpkg"
    gfo.buffer(
        input_path, output_path, distance=1, force=True, nb_parallel=_get_nb_parallel()
    )
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
        operation="buffer",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="buffer on agri parcel layer BEFL (~500k polygons)",
        run_details={"nb_cpu": _get_nb_parallel()},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def _clip(tmp_dir: Path) -> RunResult:
    """
    Clip doesn't work for the other libraries, so no use to activate it here.
    """
    # Init
    input1_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input1_path.stem}_clip_{input2_path.stem}.gpkg"
    gfo.clip(
        input_path=input1_path,
        clip_path=input2_path,
        output_path=output_path,
        force=True,
        nb_parallel=_get_nb_parallel(),
    )
    result = RunResult(
        package="geofileops",
        package_version=gfo.__version__,
        operation="clip",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="clip of 2 agri parcel layers BEFL (2*~500k polygons)",
        run_details={"nb_cpu": _get_nb_parallel()},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def dissolve_nogroupby(tmp_dir: Path) -> RunResult:
    # Init
    input_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_diss_nogroupby.gpkg"
    gfo.dissolve(
        input_path=input_path,
        output_path=output_path,
        explodecollections=True,
        force=True,
        nb_parallel=_get_nb_parallel(),
    )
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
        operation="dissolve",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="dissolve on agri parcels BEFL (~500k polygons)",
        run_details={"nb_cpu": _get_nb_parallel()},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def dissolve_groupby(tmp_dir: Path) -> RunResult:
    # Init
    input_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_diss_groupby.gpkg"
    gfo.dissolve(
        input_path,
        output_path,
        groupby_columns=["GEWASGROEP"],
        explodecollections=True,
        force=True,
        nb_parallel=_get_nb_parallel(),
    )
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
        operation="dissolve_groupby",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr=(
            "dissolve on agri parcels BEFL (~500k polygons), groupby=[GEWASGROEP]"
        ),
        run_details={"nb_cpu": _get_nb_parallel()},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def intersection(tmp_dir: Path) -> RunResult:
    # Init
    input1_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}.gpkg"
    gfo.intersection(
        input1_path=input1_path,
        input2_path=input2_path,
        output_path=output_path,
        force=True,
        nb_parallel=_get_nb_parallel(),
    )
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
        operation="intersection",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="intersection of 2 agri parcel layers BEFL (2*~500k polygons)",
        run_details={"nb_cpu": _get_nb_parallel()},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def symmetric_difference_complexpoly_agri(tmp_dir: Path) -> RunResult:
    # Init
    function_name = inspect.currentframe().f_code.co_name  # type: ignore[union-attr]

    # Prepare some complex polygons to test with
    poly_complex = testdata.create_complex_poly(
        xmin=30000.123,
        ymin=170000.123,
        width=20000,
        height=20000,
        line_distance=500,
        max_segment_length=100,
    )
    print(f"num_coordinates: {shapely.get_num_coordinates(poly_complex)}")
    input1_path = tmp_dir / "complex.gpkg"
    complex_gdf = gpd.GeoDataFrame(geometry=[poly_complex], crs="epsg:31370")
    complex_gdf.to_file(input1_path, engine="pyogrio")
    input2_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}.gpkg"
    gfo.symmetric_difference(
        input1_path=input1_path,
        input2_path=input2_path,
        output_path=output_path,
        nb_parallel=_get_nb_parallel(),
        force=True,
    )
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
        operation=function_name,
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr=(
            f"{function_name} between 1 complex poly and the agriparcels BEFL "
            "(~500k poly)"
        ),
        run_details={"nb_cpu": _get_nb_parallel()},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def union(tmp_dir: Path) -> RunResult:
    # Init
    input1_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}.gpkg"
    gfo.union(
        input1_path=input1_path,
        input2_path=input2_path,
        output_path=output_path,
        force=True,
        nb_parallel=_get_nb_parallel(),
    )
    result = RunResult(
        package=_get_package(),
        package_version=_get_version(),
        operation="union",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="union of 2 agri parcel layers BEFL (2*~500k polygons)",
        run_details={"nb_cpu": _get_nb_parallel()},
    )

    # Cleanup and return
    output_path.unlink()
    return result
