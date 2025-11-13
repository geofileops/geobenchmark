"""
Module to benchmark geopandas operations.
"""

from datetime import datetime
import logging
import multiprocessing
from pathlib import Path

import dask_geopandas as dgpd

import geofileops as gfo
from geofileops.util import geoseries_util
import pyogrio

from benchmarker import RunResult
import testdata

logger = logging.getLogger(__name__)

_package = "dask-geopandas"
_package_version = dgpd.__version__.replace("v", "")
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
    # Read input file
    start_time = datetime.now()
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # Buffer
    start_time_buffer = datetime.now()
    dgdf = dgpd.from_geopandas(gdf, npartitions=_nb_parallel)
    assert isinstance(dgdf.geometry, dgpd.GeoSeries)
    dgdf.geometry = dgdf.geometry.buffer(distance=1, resolution=5).compute()
    assert isinstance(dgdf, dgpd.GeoDataFrame)
    logger.info(
        f"time for buffer: {(datetime.now() - start_time_buffer).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()

    # Remark: write to parquet is significantly faster (~4x), but isn't really
    # a typical geo file + actually is 1 file per partition in a directory...?
    # output_path = tmp_dir / f"{input_path.stem}_geopandas_buf.parquet"
    # dask_gdf.to_parquet(output_path)

    # Convert to normal GeoDataFrame
    result_gdf = dgdf.compute()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    # Remark: the harmonization would ideally also be done in parallel
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)

    output_path = tmp_dir / f"{input_path.stem}_{_package}_buf.gpkg"
    pyogrio.write_dataframe(
        result_gdf, output_path, layer=output_path.stem, driver="GPKG"
    )
    logger.info(f"write took {(datetime.now() - start_time_write).total_seconds()}")
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="buffer",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr="buffer agri parcels BEFL (~500k polygons)",
        run_details={"nb_cpu": _nb_parallel},
    )

    # Cleanup
    # shutil.rmtree(output_path)
    output_path.unlink()

    return result


def _clip(tmp_dir: Path) -> RunResult:
    """
    On the current test datasets, clip with dask-geopandas crashes with memory issues,
    so no use to activate benchmark.
    """

    # Clip operation always crashes with memory issues in dask-geopandas
    # TODO: try using a less complex clip layer

    # Init
    input1_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path, _ = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)
    """
    client = Client(
        LocalCluster(
            n_workers=_nb_parallel,
            threads_per_worker=1,
            memory_limit="1GB",
        )
    )
    """

    # Go!
    # Read input files
    start_time = datetime.now()
    input1_gdf = pyogrio.read_dataframe(input1_path)
    input2_gdf = pyogrio.read_dataframe(input2_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # Apply operation
    start_time_op = datetime.now()
    input1_dgdf = dgpd.from_geopandas(input1_gdf, npartitions=_nb_parallel)
    result_dgdf = dgpd.clip(input1_dgdf, input2_gdf, keep_geom_type=True)
    result_gdf = result_dgdf.compute()
    logger.info(
        f"time for operation: {(datetime.now() - start_time_op).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    # result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    output_path = (
        tmp_dir / f"{input1_path.stem}_clip_{input2_path.stem}_{_package}.gpkg"
    )
    pyogrio.write_dataframe(
        result_gdf, output_path, layer=output_path.stem, driver="GPKG"
    )
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
    # output_path.unlink()
    return result


def dissolve(tmp_dir: Path) -> RunResult:
    # Init
    input_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    # Read input file
    start_time = datetime.now()
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # dissolve
    start_time_dissolve = datetime.now()
    dgdf = dgpd.from_geopandas(gdf, npartitions=_nb_parallel)
    dgdf = dgdf.dissolve(split_out=_nb_parallel)  # type: ignore
    dgdf = dgdf.explode()
    result_gdf = dgdf.compute()
    logger.info(
        f"time for dissolve: {(datetime.now() - start_time_dissolve).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()

    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    # Remark: the harmonization would ideally also be done in parallel
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)

    output_path = tmp_dir / f"{input_path.stem}_{_package}_diss.gpkg"
    pyogrio.write_dataframe(
        result_gdf, output_path, layer=output_path.stem, driver="GPKG"
    )
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
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # dissolve
    start_time_dissolve = datetime.now()
    dgdf = dgpd.from_geopandas(gdf, npartitions=_nb_parallel)
    assert isinstance(dgdf, dgpd.GeoDataFrame)
    dgdf = dgdf.dissolve(by="GWSGRPH_LB", split_out=_nb_parallel)
    dgdf = dgdf.explode()
    result_gdf = dgdf.compute()
    logger.info(
        f"time for dissolve: {(datetime.now() - start_time_dissolve).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()

    # Remark: write to parquet is significantly faster (~4x), but isn't really
    # a typical geo file + actually is 1 file per partition in a directory...?
    # output_path = tmp_dir / f"{input_path.stem}_geopandas_buf.parquet"
    # dask_gdf.to_parquet(output_path)

    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    # Remark: the harmonization would ideally also be done in parallel
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)

    output_path = tmp_dir / f"{input_path.stem}_{_package}_diss_groupby.gpkg"
    pyogrio.write_dataframe(
        result_gdf, output_path, layer=output_path.stem, driver="GPKG"
    )
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


def join_by_location_intersects(tmp_dir: Path) -> RunResult:
    # Init-
    input1_path, _ = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path, _ = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    ### Go! ###
    # Read input files
    start_time = datetime.now()
    input1_gdf = pyogrio.read_dataframe(input1_path)
    input2_gdf = pyogrio.read_dataframe(input2_path)
    logger.info(f"time for read: {(datetime.now() - start_time).total_seconds()}")

    # intersect
    start_time_operation = datetime.now()
    input1_dgdf = dgpd.from_geopandas(input1_gdf, npartitions=_nb_parallel)
    joined_dgdf = dgpd.sjoin(input1_dgdf, input2_gdf, predicate="intersects")
    result_gdf = joined_dgdf.compute()
    logger.info(
        f"time for intersect: {(datetime.now() - start_time_operation).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    output_path = (
        tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}_{_package}.gpkg"
    )
    pyogrio.write_dataframe(
        result_gdf, output_path, layer=output_path.stem, driver="GPKG"
    )

    logger.info(f"write took {(datetime.now() - start_time_write).total_seconds()}")
    result = RunResult(
        package=_package,
        package_version=_package_version,
        operation="join_by_location_intersects",
        secs_taken=(datetime.now() - start_time).total_seconds(),
        operation_descr=(
            "join_by_location_intersects between 2 agri parcel layers BEFL "
            "(2*~500.000 polygons)"
        ),
        run_details={"nb_cpu": multiprocessing.cpu_count()},
    )

    # Cleanup and return
    logger.info(f"nb features in result: {gfo.get_layerinfo(output_path).featurecount}")
    output_path.unlink()
    return result


"""
def intersection(tmp_dir: Path) -> RunResult:
    # Intersection operation is not yet supported in dask-geopandas
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
    input1_dgdf = dgpd.from_geopandas(
        input1_gdf, npartitions=nb_parallel
    )
    result_gdf = input1_dgdf.overlay(input2_gdf, how="intersection")
    logger.info(
        f"time for intersection: {(datetime.now()-start_time_op).total_seconds()}"
    )

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    output_path = (
        tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}_{_package}.gpkg"
    )
    pyogrio.write_dataframe(
        result_gdf, output_path, layer=output_path.stem, driver="GPKG"
    )
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
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
"""
