import benchmarker


def main():
    all_benchmarks = True
    if not all_benchmarks:
        # debug: only run specific benchmark modules/function(s)
        benchmarker.run_benchmarks(
            benchmarks_subdir="benchmarks_vector_ops",
            results_subdir="results_vector_ops",
            modules=["benchmarks_geofileops"],
            # modules=["benchmarks_geopandas_pyogrio"]
            # modules=["benchmarks_dask_geopandas"],
            functions=["buffer"],
        )
        return

    benchmarker.run_benchmarks(
        benchmarks_subdir="benchmarks_vector_ops",
        results_subdir="results_vector_ops",
    )


if __name__ == "__main__":
    main()
