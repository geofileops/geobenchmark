import benchmarker


def main():
    debug = False
    if debug:
        # Only run specific benchmark function(s)
        benchmarker.run_benchmarks(
            modules=["benchmarks_geopandas"], functions=["buffer"]
        )
        return

    benchmarker.run_benchmarks(modules=["benchmarks_geopandas"])


if __name__ == "__main__":
    main()
