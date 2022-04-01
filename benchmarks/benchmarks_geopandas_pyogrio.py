# -*- coding: utf-8 -*-
"""
Module to benchmark geopandas operations using pyogrio for IO.
"""

from datetime import datetime
import logging
from pathlib import Path

import geopandas as gpd
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

def get_package() -> str:
    return "geopandas_pyogrio"

def get_version() -> str:
    return f"{gpd.__version__}_{pyogrio.__version__}".replace("v", "")

def buffer(tmp_dir: Path) -> RunResult:
    
    ### Init ###
    input_path, _ = testdata.get_testdata(tmp_dir)
    
    ### Go! ###
    # Read input file
    start_time = datetime.now()
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")
    
    # Buffer
    start_time_buffer = datetime.now()
    gdf.geometry = gdf.geometry.buffer(distance=1, resolution=5)
    logger.info(f"time for buffer: {(datetime.now()-start_time_buffer).total_seconds()}")
    
    # Write to output file
    start_time_write = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_geopandas_buf.gpkg"
    pyogrio.write_dataframe(gdf, output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")    
    result = RunResult(
            package=get_package(), 
            package_version=get_version(),
            operation="buffer", 
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="buffer agri parcels BEFL (~500.000 polygons)")
    
    # Cleanup
    output_path.unlink()

    return result

def dissolve(tmp_dir: Path) -> RunResult:
    
    ### Init ###
    input_path, _ = testdata.get_testdata(tmp_dir)
    
    ### Go! ###
    # Read input file
    start_time = datetime.now()
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")
    
    # dissolve
    start_time_dissolve = datetime.now()
    gdf = gdf.dissolve()
    logger.info(f"time for dissolve: {(datetime.now()-start_time_dissolve).total_seconds()}")
    
    # Write to output file
    start_time_write = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_geopandas_diss.gpkg"
    pyogrio.write_dataframe(gdf, output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    result = RunResult(
            package=get_package(), 
            package_version=get_version(),
            operation="dissolve", 
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="dissolve agri parcels BEFL (~500.000 polygons)")
    
    # Cleanup and return
    output_path.unlink()
    return result

def dissolve_groupby(tmp_dir: Path) -> RunResult:
    
    ### Init ###
    input_path, _ = testdata.get_testdata(tmp_dir)
    
    ### Go! ###
    # Read input file
    start_time = datetime.now()
    gdf = pyogrio.read_dataframe(input_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")
    
    # dissolve
    start_time_dissolve = datetime.now()
    gdf = gdf.dissolve(by="GEWASGROEP")
    logger.info(f"time for dissolve: {(datetime.now()-start_time_dissolve).total_seconds()}")
    
    # Write to output file
    start_time_write = datetime.now()
    output_path = tmp_dir / f"{input_path.stem}_geopandas_diss_groupby.gpkg"
    pyogrio.write_dataframe(gdf, output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    result = RunResult(
            package=get_package(), 
            package_version=get_version(),
            operation="dissolve_groupby", 
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="dissolve on agri parcels BEFL (~500.000 polygons), groupby=GEWASGROEPs")
    
    # Cleanup and return
    output_path.unlink()
    return result

def intersect(tmp_dir: Path) -> RunResult:
    
    ### Init ###
    input1_path, input2_path = testdata.get_testdata(tmp_dir)
        
    ### Go! ###
    # Read input files
    start_time = datetime.now()
    input1_gdf = pyogrio.read_dataframe(input1_path)
    input2_gdf = pyogrio.read_dataframe(input2_path)
    logger.info(f"time for read: {(datetime.now()-start_time).total_seconds()}")
    
    # intersect
    start_time_intersect = datetime.now()
    output_gdf = input1_gdf.overlay(input2_gdf, how="intersection")
    logger.info(f"time for intersect: {(datetime.now()-start_time_intersect).total_seconds()}")

    # Write to output file
    start_time_write = datetime.now()
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}.gpkg"
    pyogrio.write_dataframe(output_gdf, output_path, layer=output_path.stem, driver="GPKG")
    logger.info(f"write took {(datetime.now()-start_time_write).total_seconds()}")
    secs_taken = (datetime.now()-start_time).total_seconds()
    result = RunResult(
            package=get_package(), 
            package_version=get_version(),
            operation='intersect', 
            secs_taken=secs_taken,
            operation_descr="intersect between 2 agri parcel layers BEFL (2*~500.000 polygons)")
    
    # Cleanup and return
    output_path.unlink()
    return result
