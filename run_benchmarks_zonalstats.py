import benchmarker


def main():
    debug = False
    if debug:
        # debug: only run specific benchmark modules/function(s)
        benchmarker.run_benchmarks(
            benchmarks_subdir="benchmarks_zonalstats",
            results_subdir="results_zonalstats",
            # modules=["benchmarks_pygeoprocessing"],
            modules=["benchmarks_geowombat"],
        )
        return

    benchmarker.run_benchmarks(
        benchmarks_subdir="benchmarks_zonalstats",
        results_subdir="results_zonalstats",
    )


if __name__ == "__main__":
    main()
