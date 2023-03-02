import benchmarker


def main():
    benchmarker.run_benchmarks(modules=["benchmarks_dask_geopandas"])

    return

    # Only run specific benchmark function(s)
    benchmarker.run_benchmarks(
        modules=["benchmarks_dask_geopandas"], functions=["join_by_location_intersects"]
    )


if __name__ == "__main__":
    main()
