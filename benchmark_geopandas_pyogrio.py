
import benchmarker

if __name__ == "__main__":
    benchmarker.run_benchmarks(["benchmarks_geopandas_pyogrio"])

    # Only run specific benchmark function(s)
    #benchmarker.run_benchmarks(["benchmarks_geopandas_pyogrio"], ["dissolve_groupby"])
    #benchmarker.run_benchmarks(["benchmarks_geopandas_pyogrio"], ["buffer"])
