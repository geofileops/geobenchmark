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
import testdata

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


class set_env_variables(object):
    def __init__(self, env_variables_to_set: dict):
        self.env_variables_backup: dict[str, str] = {}
        self.env_variables_to_set = env_variables_to_set

    def __enter__(self):
        # Only if a name and value is specified...
        for name, value in self.env_variables_to_set.items():
            # If the environment variable is already defined, make backup
            if name in os.environ:
                self.env_variables_backup[name] = os.environ[name]

            # Set env variable to value
            os.environ[name] = value

    def __exit__(self, type, value, traceback):
        # Set variables that were backed up back to original value
        for name, value in self.env_variables_backup.items():
            # Recover backed up value
            os.environ[name] = value
        # For variables without backup, remove them
        for name, value in self.env_variables_to_set.items():
            if name not in self.env_variables_backup:
                if name in os.environ:
                    del os.environ[name]


def write_dataframe(tmp_dir: Path) -> List[RunResult]:
    # Init
    results = []
    (
        input_path,
        _,
    ) = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)

    # Go!
    # Read input files
    input_gdf = pyogrio.read_dataframe(input_path)

    # Operation = write
    output_path = tmp_dir / f"{input_path.stem}_write_dataframe_{_get_package()}.gpkg"

    # Make combinations of all possible pragma's that could be used
    sqlite_possible_pragmas = {
        "temp_store=MEMORY": "Store temp results in memory",
        "journal_mode=OFF": "Don't use any transaction log",
        "locking_mode=EXCLUSIVE": "Locking mode exclusive -> no concurrent reading",
        "synchronous=OFF": "Don't flush to disk",
        "mmap_size=30000000000": "Use memory mapped IO (max 30GB)",
    }

    sqlite_pragma_combinations_tmp = []
    for lengths in range(0, len(sqlite_possible_pragmas) + 1):
        for subset in itertools.combinations(sqlite_possible_pragmas, lengths):
            sqlite_pragma_combinations_tmp.append(list(subset))

    # Now additionally add some different values for the cache_size pragma
    sqlite_caches_sizes = {
        None: "no Cache size",
        "cache_size=-100000": "Set cache size, in KB",
        "cache_size=-50000": "Set cache size, in KB",
        "cache_size=-30000": "Set cache size, in KB",
        "cache_size=-10000": "Set cache size, in KB",
    }
    sqlite_pragma_combinations = []
    for sqlite_pragma_combination in sqlite_pragma_combinations_tmp:
        for sqlite_caches_size in sqlite_caches_sizes:
            if sqlite_caches_size is None:
                sqlite_pragma_combinations.append(sqlite_pragma_combination)
            else:
                sqlite_pragma_combinations.append(
                    [sqlite_caches_size] + list(sqlite_pragma_combination)
                )

    print(f"Nb pragma combinations found: {len(sqlite_pragma_combinations)}")

    for combination_id, sqlite_pragmas in enumerate(sqlite_pragma_combinations):
        sqlite_pragma_str = ",".join(sqlite_pragmas)
        start_time = datetime.now()
        with set_env_variables({"OGR_SQLITE_PRAGMA": sqlite_pragma_str}):
            pyogrio.write_dataframe(
                input_gdf, output_path, layer=output_path.stem, driver="GPKG"
            )

        secs_taken = (datetime.now() - start_time).total_seconds()
        results.append(
            RunResult(
                package=_get_package(),
                package_version=_get_version(),
                operation="write_dataframe",
                secs_taken=secs_taken,
                operation_descr=(
                    "write_dataframe of agri parcel layers BEFL (~500k polygons)"
                ),
                run_details={"pragmas": sqlite_pragma_str},
            )
        )

        logger.info(
            f"write took: {secs_taken:.2f}s for "
            f"{combination_id}/{len(sqlite_pragma_combinations)}, "
            f"with pragma's <{sqlite_pragma_str}>"
        )
        output_path.unlink()

    # Return
    return results
