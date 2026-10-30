"""Entry point for running vector operation benchmarks."""

import benchmarker


def main() -> None:
    """Run vector operation benchmarks."""
    all_benchmarks = False
    if not all_benchmarks:
        # debug: only run specific benchmark modules/function(s)
        benchmarker.run_benchmarks(
            benchmarks_subdir="benchmarks_vector_ops",
            results_subdir="results_vector_ops",
            modules=[
                "benchmarks_geofileops",
                # "benchmarks_gdal_cli",
                # ("benchmarks_duckdb", {"file_format": "geoparquet"}),
                # ("benchmarks_duckdb", {"file_format": "gpkg"}),
                # "benchmarks_geopandas",
                # "benchmarks_dask_geopandas",
            ],
            # functions=["buffer"],
            # functions=["_symdif_complexpolys_agri"],
        )
        return

    benchmarker.run_benchmarks(
        benchmarks_subdir="benchmarks_vector_ops",
        results_subdir="results_vector_ops",
    )


if __name__ == "__main__":
    main()
