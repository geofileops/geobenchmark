import benchmarker


def main():
    all_benchmarks = False
    if not all_benchmarks:
        # debug: only run specific benchmark modules/function(s)
        benchmarker.run_benchmarks(
            benchmarks_subdir="benchmarks_vector_ops",
            results_subdir="results_vector_ops",
            # modules=["benchmarks_geofileops"],
            modules=["benchmarks_geopandas"],
            # modules=["benchmarks_dask_geopandas"],
            # functions=["buffer"],
            # functions=["symdif_complexpolys_agri"],
        )
        return

    benchmarker.run_benchmarks(
        benchmarks_subdir="benchmarks_vector_ops",
        results_subdir="results_vector_ops",
    )


if __name__ == "__main__":
    main()
