from pathlib import Path

import pandas as pd

import reporter


def test_detailed_chart_omits_verbose_io_from_run_labels(
    monkeypatch, tmp_path: Path
) -> None:
    results = pd.DataFrame(
        [
            {
                "run_datetime": "2026-10-24 10:00:00",
                "package": "duckdb-gpkg",
                "package_version": "1.5.0",
                "operation": "buffer",
                "operation_descr": "Buffer geometries",
                "secs_taken": 10.0,
                "run_details": (
                    "{'nb_cpu': 12, 'file_format': 'gpkg', "
                    "'io': 'ST_Read (single-threaded), GDAL COPY to GPKG'}"
                ),
            },
            {
                "run_datetime": "2026-10-24 11:00:00",
                "package": "duckdb-geoparquet",
                "package_version": "1.5.0",
                "operation": "buffer",
                "operation_descr": "Buffer geometries",
                "secs_taken": 3.0,
                "run_details": (
                    "{'nb_cpu': 12, 'file_format': 'geoparquet', "
                    "'io': 'read_parquet, COPY to GeoParquet'}"
                ),
            },
            {
                "run_datetime": "2026-10-24 12:00:00",
                "package": "geopandas",
                "package_version": "1.0.1",
                "operation": "buffer",
                "operation_descr": "Buffer geometries",
                "secs_taken": 20.0,
                "run_details": "{'nb_cpu': 12, 'io': 'GeoPandas file I/O'}",
            },
        ]
    )
    results_path = tmp_path / "benchmark_results.csv"
    results.to_csv(results_path, index=False)
    captured_charts: dict[str, pd.DataFrame] = {}

    def capture_chart(
        df: pd.DataFrame,
        title: str,
        output_path: Path,
        **kwargs: object,
    ) -> None:
        captured_charts[output_path.name] = df.copy()

    monkeypatch.setattr(reporter, "save_chart", capture_chart)
    reporter.generate_reports(results_path, tmp_path / "reports")

    gpkg_labels = captured_charts["duckdb-gpkg_buffer.png"].index.get_level_values(
        "run_details"
    )
    geoparquet_labels = captured_charts[
        "duckdb-geoparquet_buffer.png"
    ].index.get_level_values("run_details")
    geopandas_labels = captured_charts["geopandas_buffer.png"].index.get_level_values(
        "run_details"
    )
    overview = captured_charts["GeoBenchmark.png"]
    package_series = {column[0] for column in overview.columns}

    assert list(gpkg_labels) == ["nb_cpu:12;file_format:gpkg"]
    assert list(geoparquet_labels) == ["nb_cpu:12;file_format:geoparquet"]
    assert list(geopandas_labels) == ["nb_cpu:12"]
    assert "duckdb-gpkg" in package_series
    assert "duckdb-geoparquet" in package_series
