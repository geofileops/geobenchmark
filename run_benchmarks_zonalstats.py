import benchmarker


def main():
    all_benchmarks = True
    if not all_benchmarks:
        # debug: only run specific benchmark modules/function(s)
        benchmarker.run_benchmarks(
            benchmarks_subdir="benchmarks_zonalstats",
            results_subdir="results_zonalstats",
            # modules=["benchmarks_rasterstats"],
            # modules=["benchmarks_pygeoops"],
            modules=["benchmarks_geowombat"],
        )
        return

    benchmarker.run_benchmarks(
        benchmarks_subdir="benchmarks_zonalstats",
        results_subdir="results_zonalstats",
    )


if __name__ == "__main__":
    main()
