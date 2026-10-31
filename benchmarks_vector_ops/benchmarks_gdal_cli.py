"""Module to benchmark GDAL CLI vector algorithms."""

import logging
from datetime import datetime
from pathlib import Path

import geofileops as gfo
import shapely
from osgeo import gdal

import testdata
from benchmarker import RunResult

logger = logging.getLogger(__name__)

gdal.UseExceptions()

_package = "gdal_cli"
_package_version = gdal.VersionInfo("RELEASE_NAME")


def buffer(tmp_dir: Path) -> RunResult:
    """Benchmark GDAL CLI buffering of the agricultural parcels."""
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    output_path = tmp_dir / f"{input_path.stem}_{_package}_buffer.gpkg"

    start_time = datetime.now()
    try:
        with gdal.alg.vector.buffer(
            input=str(input_path),
            output=str(output_path),
            distance=1,
            quadrant_segments=5,
            output_format="GPKG",
            overwrite=True,
        ):
            pass

        secs_taken = (datetime.now() - start_time).total_seconds()
        logger.info(f"buffer took {secs_taken:.2f} s")
        return RunResult(
            package=_package,
            package_version=_package_version,
            operation="buffer",
            secs_taken=secs_taken,
            operation_descr="buffer agri parcels BEFL (~500k polygons)",
        )
    finally:
        output_path.unlink(missing_ok=True)


def buffer_sealed(tmp_dir: Path) -> RunResult:
    """Benchmark GDAL CLI buffering of the sealed surfaces."""
    input_path, _ = testdata.TestFile.SEALED_2019.get_file(tmp_dir)
    output_path = tmp_dir / f"{input_path.stem}_{_package}_buffer.gpkg"

    start_time = datetime.now()
    try:
        with gdal.alg.vector.buffer(
            input=str(input_path),
            output=str(output_path),
            distance=1,
            quadrant_segments=5,
            output_format="GPKG",
            overwrite=True,
        ):
            pass

        secs_taken = (datetime.now() - start_time).total_seconds()
        logger.info(f"buffer_sealed took {secs_taken:.2f} s")
        return RunResult(
            package=_package,
            package_version=_package_version,
            operation="buffer_sealed",
            secs_taken=secs_taken,
            operation_descr="buffer sealed surfaces BEFL (~2.6M polygons)",
        )
    finally:
        output_path.unlink(missing_ok=True)


def _layer_algebra(  # noqa: PLR0917
    tmp_dir: Path,
    input_path: Path,
    method_path: Path,
    operation: str,
    operation_name: str,
    operation_descr: str,
) -> RunResult:
    output_path = (
        tmp_dir
        / f"{input_path.stem}_{operation_name}_{method_path.stem}_{_package}.gpkg"
    )

    start_time = datetime.now()
    try:
        with gdal.alg.vector.layer_algebra(
            input=str(input_path),
            method=str(method_path),
            output=str(output_path),
            operation=operation,
            output_format="GPKG",
            overwrite=True,
        ):
            pass

        secs_taken = (datetime.now() - start_time).total_seconds()
        logger.info(f"{operation_name} took {secs_taken:.2f} s")
        return RunResult(
            package=_package,
            package_version=_package_version,
            operation=operation_name,
            secs_taken=secs_taken,
            operation_descr=operation_descr,
        )
    finally:
        output_path.unlink(missing_ok=True)


def intersection(tmp_dir: Path) -> RunResult:
    """Benchmark GDAL CLI intersection of the two agricultural parcel datasets."""
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    method_path, _ = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    return _layer_algebra(
        tmp_dir=tmp_dir,
        input_path=input_path,
        method_path=method_path,
        operation="intersection",
        operation_name="intersection",
        operation_descr="intersection of 2 agri parcel layers BEFL (2*~500k polygons)",
    )


def union(tmp_dir: Path) -> RunResult:
    """Benchmark GDAL CLI union of the two agricultural parcel datasets."""
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    method_path, _ = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    return _layer_algebra(
        tmp_dir=tmp_dir,
        input_path=input_path,
        method_path=method_path,
        operation="union",
        operation_name="union",
        operation_descr="union of 2 agri parcel layers BEFL (2*~500k polygons)",
    )


def _symdif_complexpolys_agri(tmp_dir: Path) -> RunResult:
    """Symmetric difference of agricultural parcels with complex polygons.

    Test is disabled by default as I stopped it after 53 hours without being completed.
    """
    input_path, input_descr = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    info = gfo.get_layerinfo(input_path)
    bbox = shapely.box(*info.total_bounds).buffer(-10_000, join_style="mitre").bounds
    method_path, method_descr = testdata.create_testfile(
        bbox=bbox,
        geoms=3,
        polys_per_geom=4,
        points_per_poly=30_000,
        poly_width=30_000,
        poly_height=30_000,
        crs=info.crs,
        dst_dir=tmp_dir,
    )

    return _layer_algebra(
        tmp_dir=tmp_dir,
        input_path=input_path,
        method_path=method_path,
        operation="sym-difference",
        operation_name="symdif_complexpolys_agri",
        operation_descr=(
            f"symdif_complexpolys_agri between {input_descr} and {method_descr}"
        ),
    )
