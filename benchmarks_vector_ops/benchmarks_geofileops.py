"""
Module to benchmark geofileops operations.
"""

from datetime import datetime
import inspect
import logging
import multiprocessing
from pathlib import Path

import geofileops as gfo
import shapely

from benchmarker import RunResult
import testdata

logger = logging.getLogger(__name__)

_package = "geofileops"
_package_version = gfo.__version__
_nb_parallel = 12

nb_cores = multiprocessing.cpu_count()
if nb_cores < _nb_parallel:
    logger.warning(
        f"nb_parallel specified ({_nb_parallel}) > nb logical cores available "
        f"({nb_cores})"
    )


def buffer(tmp_dir: Path) -> RunResult:
    # Init
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_buf.gpkg"
    gfo.buffer(
        input_path, output_path, distance=1, force=True, nb_parallel=_nb_parallel
    )
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="buffer",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="buffer on agri parcel layer BEFL (~500k polygons)",
        run_details={"nb_cpu": _nb_parallel},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def _clip(tmp_dir: Path) -> RunResult:
    """
    Clip doesn't work for the other libraries, so no use to activate it here.
    """
    # Init
    input1_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path, _ = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input1_path.stem}_clip_{input2_path.stem}.gpkg"
    gfo.clip(
        input_path=input1_path,
        clip_path=input2_path,
        output_path=output_path,
        force=True,
        nb_parallel=_nb_parallel,
    )
    result = RunResult(
        package="geofileops",
        package_version=gfo.__version__,
        operation="clip",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="clip of 2 agri parcel layers BEFL (2*~500k polygons)",
        run_details={"nb_cpu": _nb_parallel},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def dissolve_nogroupby(tmp_dir: Path) -> RunResult:
    # Init
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_diss_nogroupby.gpkg"
    gfo.dissolve(
        input_path=input_path,
        output_path=output_path,
        explodecollections=True,
        force=True,
        nb_parallel=_nb_parallel,
    )
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="dissolve",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="dissolve on agri parcels BEFL (~500k polygons)",
        run_details={"nb_cpu": _nb_parallel},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def dissolve_groupby(tmp_dir: Path) -> RunResult:
    # Init
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_diss_groupby.gpkg"
    gfo.dissolve(
        input_path,
        output_path,
        groupby_columns=["GWSGRPH_LB"],
        explodecollections=True,
        force=True,
        nb_parallel=_nb_parallel,
    )
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="dissolve_groupby",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr=(
            "dissolve on agri parcels BEFL (~500k polygons), groupby=[GWSGRPH_LB]"
        ),
        run_details={"nb_cpu": _nb_parallel},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def intersection(tmp_dir: Path) -> RunResult:
    # Init
    input1_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path, _ = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}.gpkg"
    gfo.intersection(
        input1_path=input1_path,
        input2_path=input2_path,
        output_path=output_path,
        force=True,
        nb_parallel=_nb_parallel,
    )
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="intersection",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="intersection of 2 agri parcel layers BEFL (2*~500k polygons)",
        run_details={"nb_cpu": _nb_parallel},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def _join_by_location_intersects(tmp_dir: Path) -> RunResult:
    # Init
    function_name = inspect.currentframe().f_code.co_name  # type: ignore[union-attr]

    input1_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path, _ = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = (
        tmp_dir / f"{input1_path.stem}_join_inters_{input2_path.stem}_{_package}.gpkg"
    )
    gfo.join_by_location(
        input1_path=input1_path,
        input2_path=input2_path,
        output_path=output_path,
        spatial_relations_query="intersects is True",
        force=True,
        nb_parallel=_nb_parallel,
    )

    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation=function_name,
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr=(
            "join_by_location_intersects between 2 agri parcel layers BEFL "
            "(2*~500.000 polygons)"
        ),
        run_details={"nb_cpu": _nb_parallel},
    )

    # Cleanup and return
    logger.info(f"nb features in result: {gfo.get_layerinfo(output_path).featurecount}")
    output_path.unlink()
    return result


def symdif_complexpolys_agri(tmp_dir: Path) -> RunResult:
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

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input1_path.stem}_symdif_{input2_path.stem}.gpkg"
    gfo.symmetric_difference(
        input1_path=input1_path,
        input2_path=input2_path,
        output_path=output_path,
        nb_parallel=_nb_parallel,
        force=True,
    )
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation=function_name,
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr=f"{function_name} between {input1_descr} and {input2_descr}",
        run_details={"nb_cpu": _nb_parallel},
    )

    # Cleanup and return
    output_path.unlink()
    return result


def union(tmp_dir: Path) -> RunResult:
    # Init
    input1_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path, _ = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    # Go!
    start_time = datetime.now()
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}.gpkg"
    gfo.union(
        input1_path=input1_path,
        input2_path=input2_path,
        output_path=output_path,
        force=True,
        nb_parallel=_nb_parallel,
    )
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="union",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="union of 2 agri parcel layers BEFL (2*~500k polygons)",
        run_details={"nb_cpu": _nb_parallel},
    )

    # Cleanup and return
    output_path.unlink()
    return result
