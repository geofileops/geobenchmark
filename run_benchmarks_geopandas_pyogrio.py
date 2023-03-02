import benchmarker


def main():
    benchmarker.run_benchmarks(modules=["benchmarks_geopandas_pyogrio"])

    return

    # Only run specific benchmark function(s)
    benchmarker.run_benchmarks(
        modules=["benchmarks_geopandas_pyogrio"], functions=["buffer"]
    )


if __name__ == "__main__":
    main()
