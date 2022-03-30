# -*- coding: utf-8 -*-
"""
Module to benchmark geofileops operations.
"""

from datetime import datetime
import logging
import multiprocessing
from pathlib import Path

import geofileops as gfo

from benchmarker import RunResult
from benchmarks import testdata

################################################################################
# Some init
################################################################################

logger = logging.getLogger(__name__)

################################################################################
# The real work
################################################################################

def buffer(tmp_dir: Path) -> RunResult:
    
    # Init
    input_path, _ = testdata.get_testdata(tmp_dir)
    output_path = tmp_dir / f"{input_path.stem}_buf.gpkg"
    
    # Go!
    start_time = datetime.now()
    gfo.buffer(input_path, output_path, distance=1, force=True)
    result = RunResult(
            package="geofileops", 
            package_version=gfo.__version__,
            operation="buffer", 
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="buffer on agri parcel layer BEFL (~500.000 polygons)",
            run_details={"nb_cpu": multiprocessing.cpu_count()})
    
    # Cleanup and return
    output_path.unlink()
    return result

def dissolve_nogroupby(tmp_dir: Path) -> RunResult:
    
    # Init
    input_path, _ = testdata.get_testdata(tmp_dir)
    output_path = tmp_dir / f"{input_path.stem}_diss_nogroupby.gpkg"
    
    # Go!
    start_time = datetime.now()
    gfo.dissolve(
            input_path=input_path, 
            output_path=output_path, 
            explodecollections=True,
            force=True)
    result = RunResult(
            package="geofileops", 
            package_version=gfo.__version__,
            operation="dissolve",
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="dissolve on agri parcels BEFL (~500.000 polygons)",
            run_details={"nb_cpu": multiprocessing.cpu_count()})

    # Cleanup and return
    output_path.unlink()
    return result

def dissolve_groupby(tmp_dir: Path) -> RunResult:    

    # Init
    input_path, _ = testdata.get_testdata(tmp_dir)
    output_path = tmp_dir / f"{input_path.stem}_diss_groupby.gpkg"
    
    # Go!
    start_time = datetime.now()
    gfo.dissolve(
            input_path, 
            output_path, 
            groupby_columns=["GEWASGROEP"],
            explodecollections=True,
            force=True)
    result = RunResult(
            package="geofileops", 
            package_version=gfo.__version__,
            operation="dissolve_groupby", 
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="dissolve on agri parcels BEFL (~500.000 polygons), groupby=[GEWASGROEP]",
            run_details={"nb_cpu": multiprocessing.cpu_count()})
    
    # Cleanup and return
    output_path.unlink()
    return result

def intersect(tmp_dir: Path) -> RunResult:
    # Init
    input1_path, input2_path = testdata.get_testdata(tmp_dir)
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}.gpkg"
    
    # Go!
    start_time = datetime.now()
    gfo.intersect(
            input1_path=input1_path, 
            input2_path=input2_path, 
            output_path=output_path,
            force=True)
    result = RunResult(
            package="geofileops", 
            package_version=gfo.__version__,
            operation='intersect', 
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="intersect between 2 agri parcel layers BEFL (2*~500.000 polygons)",
            run_details={"nb_cpu": multiprocessing.cpu_count()})

    # Cleanup and return
    output_path.unlink()
    return result

def union(tmp_dir: Path) -> RunResult:
    # Init
    input1_path, input2_path = testdata.get_testdata(tmp_dir)
    output_path = tmp_dir / f"{input1_path.stem}_inters_{input2_path.stem}.gpkg"
    
    # Go!
    start_time = datetime.now()
    gfo.union(
            input1_path=input1_path, 
            input2_path=input2_path, 
            output_path=output_path,
            force=True)
    result = RunResult(
            package="geofileops", 
            package_version=gfo.__version__,
            operation="union",
            secs_taken=(datetime.now()-start_time).total_seconds(),
            operation_descr="union between 2 agri parcel layers BEFL (2*~500.000 polygons)",
            run_details={"nb_cpu": multiprocessing.cpu_count()})

    # Cleanup and return
    output_path.unlink()
    return result
    