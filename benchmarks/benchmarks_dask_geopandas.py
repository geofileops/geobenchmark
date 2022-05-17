# -*- coding: utf-8 -*-
"""
Module to benchmark geopandas operations.
"""

from datetime import datetime
import logging
import multiprocessing
import os
from pathlib import Path

import dask_geopandas as dgpd
from dask.distributed import Client, LocalCluster
import geofileops as gfo
from geofileops.util import geoseries_util
import pyogrio

from benchmarker import RunResult
from benchmarks import testdata

################################################################################
# Some init
################################################################################

logger = logging.getLogger(__name__)

################################################################################
# The real work
################################################################################

def _get_package() -> str:
    return "dask-geopandas"

def _get_version() -> str:
    return dgpd.__version__.replace("v", "")

def buffer(tmp_dir: Path) -> RunResult:
    ### Init ###
    input_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    
    ### Go! ###
    # Read input file
    start_time = datetime.now()
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")
    
    # Buffer
    start_time_operation = datetime.now()
    dgdf = dgpd.from_geopandas(gdf, npartitions=multiprocessing.cpu_count())
    dgdf["geometry"] = dgdf["geometry"].buffer(distance=1, resolution=5).compute()
    assert isinstance(dgdf, dgpd.GeoDataFrame)
    logger.info(f"time for buffer: {(datetime.now()-start_time_operation).total_seconds()}")
    
    # Write to output file
    start_time_write = datetime.now()
    
    # Remark: write to parquet is significantly faster (~4x), but isn't really 
    # a typical geo file + actually is 1 file per partition in a directory...?
    #output_path = tmp_dir / f"{input_path.stem}_geopandas_buf.parquet"
    #dask_gdf.to_parquet(output_path) 

    # Convert to normal GeoDataFrame
    result_gdf = dgdf.compute()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    # Remark: the harmonization would ideally also be done in parallel
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    
    output_path = tmp_dir / f"{input_path.stem}_{_get_package()}_buf.gpkg"
    pyogrio.write_dataframe(result_gdf, output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")    
    result = RunResult(
            package=_get_package(), 
            package_version=_get_version(),
            operation="buffer", 
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="buffer agri parcels BEFL (~500.000 polygons)",
            run_details={"nb_cpu": multiprocessing.cpu_count()})
    
    # Cleanup
    #shutil.rmtree(output_path)
    output_path.unlink()

    return result

def _clip(tmp_dir: Path) -> RunResult:
    """
    On the current test datasets, clip with dask-geopandas crashes with memory issues,
    so no use to activate benchmark.
    """

    # Clip operation always crashes with memory issues in dask-geopandas
    # TODO: try using a less complex clip layer

    ### Init ###
    input1_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)
    client = Client(LocalCluster(
            n_workers=multiprocessing.cpu_count(), 
            threads_per_worker=1,
            memory_limit="1GB"))

    ### Go! ###
    # Read input files
    start_time = datetime.now()
    input1_gdf = pyogrio.read_dataframe(input1_path)
    input2_gdf = pyogrio.read_dataframe(input2_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")
    
    # Apply operation
    start_time_operation = datetime.now()
    input1_dgdf = dgpd.from_geopandas(input1_gdf, npartitions=multiprocessing.cpu_count())
    result_dgdf = dgpd.clip(input1_dgdf, input2_gdf, keep_geom_type=True)
    result_gdf = result_dgdf.compute()
    logger.info(f"time for operation: {(datetime.now()-start_time_operation).total_seconds()}")

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    #result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    output_path = tmp_dir / f"{input1_path.stem}_clip_{input2_path.stem}_{_get_package()}.gpkg"
    pyogrio.write_dataframe(result_gdf, output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    secs_taken = (datetime.now()-start_time).total_seconds()
    result = RunResult(
            package=_get_package(), 
            package_version=_get_version(),
            operation='clip', 
            secs_taken=secs_taken,
            operation_descr="clip between 2 agri parcel layers BEFL (2*~500.000 polygons)")
    
    # Cleanup and return
    #output_path.unlink()
    return result

def dissolve(tmp_dir: Path) -> RunResult: 
    ### Init ###
    input_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    
    ### Go! ###
    # Read input file
    start_time = datetime.now()
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")
    
    # dissolve
    start_time_operation = datetime.now()
    dgdf = dgpd.from_geopandas(gdf, npartitions=multiprocessing.cpu_count())
    dgdf = dgdf.dissolve(split_out=multiprocessing.cpu_count())   # type: ignore
    dgdf = dgdf.explode()
    result_gdf = dgdf.compute()
    logger.info(f"time for dissolve: {(datetime.now()-start_time_operation).total_seconds()}")
    
    # Write to output file
    start_time_write = datetime.now()
    
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    # Remark: the harmonization would ideally also be done in parallel
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    
    output_path = tmp_dir / f"{input_path.stem}_{_get_package()}_diss.gpkg"
    pyogrio.write_dataframe(result_gdf, output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}") 
    result = RunResult(
            package=_get_package(), 
            package_version=_get_version(),
            operation="dissolve", 
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="dissolve agri parcels BEFL (~500.000 polygons)")
    
    # Cleanup and return
    output_path.unlink()
    return result

def dissolve_groupby(tmp_dir: Path) -> RunResult:
    ### Init ###
    input_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    
    ### Go! ###
    # Read input file
    start_time = datetime.now()
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")
    
    # dissolve
    start_time_operation = datetime.now()
    dgdf = dgpd.from_geopandas(gdf, npartitions=multiprocessing.cpu_count())
    dgdf = dgdf.dissolve(by="GEWASGROEP", split_out=multiprocessing.cpu_count())  # type: ignore
    dgdf = dgdf.explode()
    result_gdf = dgdf.compute()
    logger.info(f"time for dissolve: {(datetime.now()-start_time_operation).total_seconds()}")
    
    # Write to output file
    start_time_write = datetime.now()
    
    # Remark: write to parquet is significantly faster (~4x), but isn't really 
    # a typical geo file + actually is 1 file per partition in a directory...?
    #output_path = tmp_dir / f"{input_path.stem}_geopandas_buf.parquet"
    #dask_gdf.to_parquet(output_path) 

    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    # Remark: the harmonization would ideally also be done in parallel
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    
    output_path = tmp_dir / f"{input_path.stem}_{_get_package()}_diss_groupby.gpkg"
    pyogrio.write_dataframe(result_gdf, output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}") 
    
    result = RunResult(
            package=_get_package(), 
            package_version=_get_version(),
            operation="dissolve_groupby", 
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="dissolve on agri parcels BEFL (~500.000 polygons), groupby=GEWASGROEPs")
    
    # Cleanup and return
    output_path.unlink()
    return result

def join_by_location_intersects(tmp_dir: Path) -> RunResult:
    # Init-
    input1_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)

    ### Go! ###
    # Read input files
    start_time = datetime.now()
    input1_gdf = pyogrio.read_dataframe(input1_path)
    input2_gdf = pyogrio.read_dataframe(input2_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")
    
    # intersect
    start_time_operation = datetime.now()
    input1_dgdf = dgpd.from_geopandas(input1_gdf, npartitions=multiprocessing.cpu_count())
    joined_dgdf = dgpd.sjoin(input1_dgdf, input2_gdf, predicate="intersects")
    result_gdf = joined_dgdf.compute()
    logger.info(f"time for intersect: {(datetime.now()-start_time_operation).total_seconds()}")

    # Write to output file
    start_time_write = datetime.now()
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}_{_get_package()}.gpkg"
    pyogrio.write_dataframe(result_gdf, output_path, layer=output_path.stem, driver="GPKG")
    
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    result = RunResult(
            package=_get_package(), 
            package_version=_get_version(),
            operation="join_by_location_intersects",
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="join_by_location_intersects between 2 agri parcel layers BEFL (2*~500.000 polygons)",
            run_details={"nb_cpu": multiprocessing.cpu_count()})

    # Cleanup and return
    logger.info(f"nb features in result: {gfo.get_layerinfo(output_path).featurecount}")
    output_path.unlink()
    return result

"""
def intersection(tmp_dir: Path) -> RunResult:
    # Intersection operation is not yet supported in dask-geopandas
    ### Init ###
    input1_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    input2_path = testdata.TestFile.AGRIPRC_2019.get_file(tmp_dir)
        
    ### Go! ###
    # Read input files
    start_time = datetime.now()
    input1_gdf = pyogrio.read_dataframe(input1_path)
    input2_gdf = pyogrio.read_dataframe(input2_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")
    
    # intersection
    start_time_intersection = datetime.now()
    input1_dgdf = dgpd.from_geopandas(input1_gdf, npartitions=multiprocessing.cpu_count())
    result_gdf = input1_dgdf.overlay(input2_gdf, how="intersection")
    logger.info(f"time for intersection: {(datetime.now()-start_time_intersection).total_seconds()}")

    # Write to output file
    start_time_write = datetime.now()
    # Harmonize, otherwise invalid gpkg because mixed poly and multipoly
    result_gdf.geometry = geoseries_util.harmonize_geometrytypes(result_gdf.geometry)
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}_{_get_package()}.gpkg"
    pyogrio.write_dataframe(result_gdf, output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    secs_taken = (datetime.now()-start_time).total_seconds()
    result = RunResult(
            package=_get_package(), 
            package_version=_get_version(),
            operation='intersection', 
            secs_taken=secs_taken,
            operation_descr="intersection between 2 agri parcel layers BEFL (2*~500.000 polygons)")
    
    # Cleanup and return
    output_path.unlink()
    return result
"""