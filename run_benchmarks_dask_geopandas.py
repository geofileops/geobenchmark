
import benchmarker

if __name__ == "__main__":
    benchmarker.run_benchmarks(modules=["benchmarks_dask_geopandas"])

    # Only run specific benchmark function(s)
    #benchmarker.run_benchmarks(modules=["benchmarks_dask_geopandas"], functions=["join_by_location_intersects"])
