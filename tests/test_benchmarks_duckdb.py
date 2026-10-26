from pathlib import Path

import duckdb
import geopandas as gpd
import pytest
import shapely

from benchmarks_vector_ops import benchmarks_duckdb as benchmarks


@pytest.fixture
def spatial_connection():
    connection = benchmarks._connect()
    yield connection
    connection.close()


@pytest.fixture
def input_layers(tmp_path: Path) -> tuple[Path, Path]:
    left_path = tmp_path / "left.gpkg"
    right_path = tmp_path / "right.gpkg"
    gpd.GeoDataFrame(
        {"parcel_id": [1, 2], "GWSGRPH_LB": ["north", "south"]},
        geometry=[shapely.box(0, 0, 2, 2), shapely.box(4, 0, 6, 2)],
        crs="EPSG:31370",
    ).to_file(left_path, driver="GPKG")
    gpd.GeoDataFrame(
        {"parcel_id": [10, 20], "group_name": ["east", "west"]},
        geometry=[shapely.box(1, 1, 3, 3), shapely.box(7, 1, 8, 2)],
        crs="EPSG:31370",
    ).to_file(right_path, driver="GPKG")
    return left_path, right_path


def _write_result(
    connection: duckdb.DuckDBPyConnection,
    operation: str,
    input1_path: Path,
    output_path: Path,
    input2_path: Path | None = None,
) -> None:
    query = benchmarks._build_operation_query(
        connection, operation, input1_path, input2_path
    )
    benchmarks._copy_query_to_gpkg(connection, query, output_path)


def _read_scalar(
    connection: duckdb.DuckDBPyConnection, path: Path, expression: str
) -> tuple:
    return connection.execute(
        f"SELECT {expression} FROM {benchmarks._read_table(path)}"
    ).fetchone()


def test_buffer_and_dissolve_operations(
    spatial_connection: duckdb.DuckDBPyConnection,
    input_layers: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    left_path, _ = input_layers

    buffer_path = tmp_path / "buffer.gpkg"
    _write_result(spatial_connection, "buffer", left_path, buffer_path)
    buffer_count, buffer_area = _read_scalar(
        spatial_connection, buffer_path, "COUNT(*), SUM(ST_Area(geom))"
    )
    assert buffer_count == 2
    assert buffer_area > 8

    dissolve_path = tmp_path / "dissolve.gpkg"
    _write_result(spatial_connection, "dissolve", left_path, dissolve_path)
    assert _read_scalar(
        spatial_connection, dissolve_path, "COUNT(*), SUM(ST_Area(geom))"
    ) == (1, 8.0)

    grouped_path = tmp_path / "grouped.gpkg"
    _write_result(spatial_connection, "dissolve_groupby", left_path, grouped_path)
    groups = spatial_connection.execute(
        "SELECT GWSGRPH_LB, ST_Area(geom) "
        f"FROM {benchmarks._read_table(grouped_path)} ORDER BY GWSGRPH_LB"
    ).fetchall()
    assert groups == [("north", 4.0), ("south", 4.0)]


def test_overlay_and_spatial_join_semantics(
    spatial_connection: duckdb.DuckDBPyConnection,
    input_layers: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    left_path, right_path = input_layers

    intersection_path = tmp_path / "intersection.gpkg"
    _write_result(
        spatial_connection,
        "intersection",
        left_path,
        intersection_path,
        right_path,
    )
    assert _read_scalar(
        spatial_connection, intersection_path, "COUNT(*), SUM(ST_Area(geom))"
    ) == (1, 1.0)
    assert _read_scalar(
        spatial_connection,
        intersection_path,
        "a_parcel_id, b_parcel_id",
    ) == (1, 10)

    union_path = tmp_path / "union.gpkg"
    _write_result(spatial_connection, "union", left_path, union_path, right_path)
    union_count, union_area = _read_scalar(
        spatial_connection, union_path, "COUNT(*), SUM(ST_Area(geom))"
    )
    assert union_count == 5
    assert union_area == 12.0
    assert _read_scalar(
        spatial_connection,
        union_path,
        "SUM(a_parcel_id IS NULL), SUM(b_parcel_id IS NULL)",
    ) == (2, 2)

    join_path = tmp_path / "join.gpkg"
    _write_result(
        spatial_connection,
        "join_by_location_intersects",
        left_path,
        join_path,
        right_path,
    )
    assert _read_scalar(
        spatial_connection, join_path, "COUNT(*), SUM(ST_Area(geom))"
    ) == (1, 4.0)
    assert _read_scalar(spatial_connection, join_path, "a_parcel_id, b_parcel_id") == (
        1,
        10,
    )


def test_symmetric_difference_is_composed_from_differences(
    spatial_connection: duckdb.DuckDBPyConnection,
    input_layers: tuple[Path, Path],
    tmp_path: Path,
) -> None:
    left_path, right_path = input_layers
    output_path = tmp_path / "symdifference.gpkg"
    _write_result(
        spatial_connection,
        "symdifference",
        left_path,
        output_path,
        right_path,
    )
    assert _read_scalar(
        spatial_connection, output_path, "COUNT(*), SUM(ST_Area(geom))"
    ) == (4, 11.0)


@pytest.mark.parametrize("file_format", ["gpkg", "geoparquet", "duckdb"])
def test_buffer_supports_configured_file_format(
    input_layers: tuple[Path, Path],
    tmp_path: Path,
    file_format: str,
) -> None:
    input_path, _ = input_layers
    original_file_format = benchmarks._file_format

    with benchmarks._benchmark_configuration(file_format):
        result = benchmarks._run_benchmark(
            operation="buffer",
            operation_descr="buffer synthetic fixture",
            input1_path=input_path,
            tmp_dir=tmp_path,
        )

    assert result.run_details is not None
    assert result.run_details["file_format"] == file_format
    package_format = "native" if file_format == "duckdb" else file_format
    assert result.package == f"duckdb-{package_format}"
    assert benchmarks._file_format == original_file_format


def test_benchmark_configuration_restores_format_after_failure() -> None:
    original_file_format = benchmarks._file_format

    with pytest.raises(RuntimeError, match="benchmark failed"):
        with benchmarks._benchmark_configuration("geoparquet"):
            assert benchmarks._file_format == "geoparquet"
            raise RuntimeError("benchmark failed")

    assert benchmarks._file_format == original_file_format


def test_benchmark_configuration_rejects_unsupported_format() -> None:
    with pytest.raises(ValueError, match="file_format must be one of"):
        with benchmarks._benchmark_configuration("unknown"):
            pass
