import benchmarker

if __name__ == "__main__":
    benchmarker.run_benchmarks(modules=["benchmarks_geofileops"])

    # Only run specific benchmark function(s)
    # benchmarker.run_benchmarks(modules=["benchmarks_geofileops"], functions=["join_by_location_intersects"])
