import benchmarker


def main():
    debug = False
    if debug:
        # Only run specific benchmark function(s)
        benchmarker.run_benchmarks(
            modules=["benchmarks_geofileops"], functions=["join_by_location_intersects"]
        )
        return

    benchmarker.run_benchmarks(modules=["benchmarks_geofileops"])


if __name__ == "__main__":
    main()
