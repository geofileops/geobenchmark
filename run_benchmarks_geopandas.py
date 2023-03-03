import benchmarker


def main():
    benchmarker.run_benchmarks(modules=["benchmarks_geopandas"])

    # Only run specific benchmark function(s)
    # benchmarker.run_benchmarks(modules=["benchmarks_geopandas"], functions=["buffer"])


if __name__ == "__main__":
    main()
