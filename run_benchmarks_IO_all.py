import benchmarker


def main():
    all_benchmarks = True
    if not all_benchmarks:
        # Only run specific benchmark function(s)
        benchmarker.run_benchmarks(
            benchmarks_subdir="benchmarks_IO", results_subdir="results_IO",
            modules=["benchmarks_pyogrio"], functions=["write_dataframe"]
        )
        return

    benchmarker.run_benchmarks(
        benchmarks_subdir="benchmarks_IO", results_subdir="results_IO"
    )


if __name__ == "__main__":
    main()