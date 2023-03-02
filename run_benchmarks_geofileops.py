import benchmarker


def main():
    benchmarker.run_benchmarks(modules=["benchmarks_geofileops"])

    return

    # Only run specific benchmark function(s)
    benchmarker.run_benchmarks(
        modules=["benchmarks_geofileops"], functions=["join_by_location_intersects"]
    )


if __name__ == "__main__":
    main()
