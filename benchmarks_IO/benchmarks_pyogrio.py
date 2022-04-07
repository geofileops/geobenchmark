# -*- coding: utf-8 -*-
"""
Module to benchmark geopandas operations using pyogrio for IO.
"""

from datetime import datetime
import itertools
import logging
import os
from pathlib import Path
import pyogrio
from typing import List

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
    return "pyogrio"

def _get_version() -> str:
    return f"{pyogrio.__version__}".replace("v", "")

class set_env_tmp(object):
    def __init__(self, name: str, value: str):
        self.value_backup = None
        self.name = name
        self.value = value
    def __enter__(self):
        # Only if a name and value is specified...
        if self.name is not None and self.value is not None:
            # If the environment variable is already defined, make backup
            if self.name in os.environ:
                self.value_backup = os.environ[self.name]

            # Set env variable to value
            os.environ[self.name] = self.value
    def __exit__(self, type, value, traceback):
        if self.value is not None and self.value is not None:
            if self.value_backup is not None:
                # Recover backed up value
                os.environ[self.name] = self.value_backup
            else:
                # Delete env variable
                if self.name in os.environ:
                    del os.environ[self.name]                

def write_dataframe(tmp_dir: Path) -> List[RunResult]:
    # Init
    results = []
    input_path = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    
    ### Go! ###
    # Read input files
    input_gdf = pyogrio.read_dataframe(input_path)
    
    # Operation = write
    output_path = tmp_dir / f"{input_path.stem}_write_dataframe_{_get_package()}.gpkg"

    # Make combinations of all possible pragma's that could be used    
    sqlite_possible_pragmas = {
            "temp_store=MEMORY": "Store temp results in memory",
            "journal_mode=OFF": "Don't use any transaction log",
            "locking_mode=EXCLUSIVE": "Locking mode exclusive -> no concurrent reading allowed",
            "synchronous=OFF": "Don't flush to disk",
            "mmap_size=30000000000": "Use memory mapped IO (max 30GB)"}

    sqlite_pragma_combinations_tmp = []
    for lengths in range(0, len(sqlite_possible_pragmas)+1):
        for subset in itertools.combinations(sqlite_possible_pragmas, lengths):
            sqlite_pragma_combinations_tmp.append(subset)
    
    # Now additionally add some different values for the cache_size pragma
    sqlite_caches_sizes = {
        None: "no Cache size",
        "cache_size=-100000": "Set cache size, in KB",
        "cache_size=-50000": "Set cache size, in KB",
        "cache_size=-30000": "Set cache size, in KB",
        "cache_size=-10000": "Set cache size, in KB", }
    sqlite_pragma_combinations = []
    for sqlite_pragma_combination in sqlite_pragma_combinations_tmp:
        for sqlite_caches_size in sqlite_caches_sizes:
            if sqlite_caches_size is None:
                sqlite_pragma_combinations.append(sqlite_pragma_combination)
            else:
                sqlite_pragma_combinations.append([sqlite_caches_size] + list(sqlite_pragma_combination))

    print(f"Nb pragma combinations found: {len(sqlite_pragma_combinations)}")
    
    for combination_id, sqlite_pragmas in enumerate(sqlite_pragma_combinations):
        sqlite_pragma_str = ",".join(sqlite_pragmas)
        start_time = datetime.now()
        with set_env_tmp("OGR_SQLITE_PRAGMA", sqlite_pragma_str):
            pyogrio.write_dataframe(input_gdf, output_path, layer=output_path.stem, driver="GPKG")

        secs_taken = (datetime.now()-start_time).total_seconds()
        results.append(RunResult(
                package=_get_package(), 
                package_version=_get_version(),
                operation="write_dataframe",
                secs_taken=secs_taken,
                operation_descr="write_dataframe of agri parcel layers BEFL (~500.000 polygons)",
                run_details={"pragmas": sqlite_pragma_str}))

        logger.info(f"write took: {secs_taken:.2f}s for {combination_id}/{len(sqlite_pragma_combinations)}, with pragma's <{sqlite_pragma_str}>")
        output_path.unlink()
        
    # Return
    return results
