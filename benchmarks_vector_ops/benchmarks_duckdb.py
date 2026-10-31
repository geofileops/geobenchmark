"""Benchmark DuckDB Spatial vector operations.

Single-run format comparison measured on Windows on 2026-10-24 with DuckDB 1.5.6
and 12 threads. Each operation uses the ~500k polygon test datasets. GeoParquet and
native DuckDB input materialization was completed before timing; operation reads
and output writes are included.

Elapsed seconds:

| Operation         | GPKG     | GeoParquet | Native DuckDB |
|-------------------|----------|------------|---------------|
| buffer            | 21.114   | 8.877      | 11.697        |
| dissolve          | 1661.057 | 1856.478   | 1865.123      |
| dissolve_groupby  | 683.730  | 701.683    | 708.236       |
| intersection      | 427.763  | 111.476    | 118.033       |
| union             | 811.883  | 677.351    | 693.315       |

These are single-run measurements, not statistical averages.
"""

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import duckdb

from benchmarker import RunResult

logger = logging.getLogger(__name__)

_package_version = duckdb.__version__
_nb_parallel = 12
_geometry_column = "geom"
_file_format_extensions = {
    "gpkg": "gpkg",
    "geoparquet": "parquet",
    "duckdb": "duckdb",
}


def _validate_file_format(file_format: str) -> None:
    if file_format not in _file_format_extensions:
        raise ValueError(
            "file_format must be one of "
            f"{', '.join(_file_format_extensions)}; got {file_format!r}"
        )


def _package_for_format(file_format: str) -> str:
    package_format = "native" if file_format == "duckdb" else file_format
    return f"duckdb-{package_format}"


_file_format = os.environ.get("GEOBENCHMARK_DUCKDB_FORMAT", "gpkg").lower()
_validate_file_format(_file_format)
DEFAULT_BENCHMARK_CONFIGS = (
    [{"file_format": _file_format}]
    if "GEOBENCHMARK_DUCKDB_FORMAT" in os.environ
    else [{"file_format": "gpkg"}, {"file_format": "geoparquet"}]
)


@contextmanager
def _benchmark_configuration(file_format: str) -> Iterator[None]:
    global _file_format  # noqa: PLW0603

    file_format = file_format.lower()
    _validate_file_format(file_format)
    previous_file_format = _file_format
    _file_format = file_format
    try:
        yield
    finally:
        _file_format = previous_file_format


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _read_table(path: Path) -> str:
    if _file_format == "gpkg":
        return f"ST_Read({_sql_literal(path.as_posix())})"
    if _file_format == "geoparquet":
        return f"read_parquet({_sql_literal(path.as_posix())})"
    return _sql_identifier(path.stem)


def _test_file(tmp_dir: Path, name: str) -> tuple[Path, str]:
    import testdata  # noqa: PLC0415

    return testdata.TestFile[name].get_file(tmp_dir)


def _connect(database_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    connection = (
        duckdb.connect(str(database_path)) if database_path else duckdb.connect()
    )
    connection.execute("INSTALL spatial")
    connection.execute("LOAD spatial")
    connection.execute(f"SET threads = {_nb_parallel}")

    drivers_query = "SELECT * FROM ST_Drivers()"
    drivers = connection.execute(drivers_query).fetchall()
    has_gpkg = any(str(value).upper() == "GPKG" for row in drivers for value in row)
    if not has_gpkg:
        connection.close()
        raise RuntimeError("DuckDB Spatial's GDAL drivers do not include GPKG")
    return connection


def _attribute_columns(connection: duckdb.DuckDBPyConnection, path: Path) -> list[str]:
    query = f"SELECT * FROM {_read_table(path)} LIMIT 0"
    cursor = connection.execute(query)
    columns = [column[0] for column in cursor.description]
    if _geometry_column not in columns:
        raise ValueError(f"Expected geometry column '{_geometry_column}' in {path}")
    return [
        column for column in columns if column.lower() not in (_geometry_column, "fid")
    ]


def _column_projections(table_alias: str, columns: list[str], prefix: str) -> list[str]:
    return [
        f"{table_alias}.{_sql_identifier(column)} AS {_sql_identifier(prefix + column)}"
        for column in columns
    ]


def _build_operation_query(
    connection: duckdb.DuckDBPyConnection,
    operation: str,
    input1_path: Path,
    input2_path: Path | None = None,
) -> str:
    input1 = f"a AS (SELECT * FROM {_read_table(input1_path)})"
    if operation == "buffer":
        return f"""
            WITH {input1}
            SELECT
                * EXCLUDE ({_geometry_column}, fid),
                ST_Buffer({_geometry_column}, 1.0, 5) AS {_geometry_column}
            FROM a
        """
    if operation == "dissolve":
        return f"""
            WITH {input1}
            SELECT ST_Union_Agg({_geometry_column}) AS {_geometry_column}
            FROM a
        """
    if operation == "dissolve_groupby":
        group_column = _sql_identifier("GWSGRPH_LB")
        return f"""
            WITH {input1}
            SELECT
                {group_column},
                ST_Union_Agg({_geometry_column}) AS {_geometry_column}
            FROM a
            GROUP BY {group_column}
        """

    if input2_path is None:
        raise ValueError(f"Operation '{operation}' requires a second input layer")

    input2 = f"b AS (SELECT * FROM {_read_table(input2_path)})"
    input_ctes = f"{input1}, {input2}"
    columns_a = _attribute_columns(connection, input1_path)
    columns_b = _attribute_columns(connection, input2_path)
    projections_a = _column_projections("a", columns_a, "a_")
    projections_b = _column_projections("b", columns_b, "b_")

    if operation == "intersection":
        projections = ",\n        ".join(projections_a + projections_b)
        return f"""
            WITH {input_ctes},
            pieces AS (
                SELECT
                    {projections},
                    ST_Intersection(a.{_geometry_column}, b.{_geometry_column})
                        AS {_geometry_column}
                FROM a
                JOIN b ON ST_Intersects(a.{_geometry_column}, b.{_geometry_column})
            )
            SELECT *
            FROM pieces
            WHERE NOT ST_IsEmpty({_geometry_column})
              AND ST_Dimension({_geometry_column}) = 2
        """

    if operation == "join_by_location_intersects":
        projections = ",\n        ".join(
            projections_a + projections_b + [f"a.{_geometry_column} AS geom"]
        )
        return f"""
            WITH {input_ctes}
            SELECT
                {projections}
            FROM a
            JOIN b ON ST_Intersects(a.{_geometry_column}, b.{_geometry_column})
        """

    if operation not in ("union", "symdifference"):
        raise ValueError(f"Unknown DuckDB Spatial operation: {operation}")

    null_a = [f"NULL AS {_sql_identifier('a_' + column)}" for column in columns_a]
    null_b = [f"NULL AS {_sql_identifier('b_' + column)}" for column in columns_b]
    a_only_projections = ",\n            ".join(projections_a + null_b)
    b_only_projections = ",\n            ".join(null_a + projections_b)
    ctes = [input_ctes]
    ctes.append(
        f"""
            a_only_raw AS (
                SELECT
                    {a_only_projections},
                    CASE
                        WHEN overlap.geom IS NULL THEN a.{_geometry_column}
                        ELSE ST_Difference(a.{_geometry_column}, overlap.geom)
                    END AS {_geometry_column}
                FROM a
                LEFT JOIN LATERAL (
                    SELECT ST_Union_Agg(b.{_geometry_column}) AS geom
                    FROM b
                    WHERE ST_Intersects(a.{_geometry_column}, b.{_geometry_column})
                ) AS overlap ON TRUE
            )
        """
    )
    ctes.append(
        f"""
            a_only AS (
                SELECT *
                FROM a_only_raw
                WHERE NOT ST_IsEmpty({_geometry_column})
                  AND ST_Dimension({_geometry_column}) = 2
            )
        """
    )

    if operation == "union":
        intersection_projections = ",\n                ".join(
            projections_a + projections_b
        )
        ctes.append(
            f"""
                intersections_raw AS (
                    SELECT
                        {intersection_projections},
                        ST_Intersection(a.{_geometry_column}, b.{_geometry_column})
                            AS {_geometry_column}
                    FROM a
                    JOIN b ON ST_Intersects(a.{_geometry_column}, b.{_geometry_column})
                )
            """
        )
        ctes.append(
            f"""
                intersections AS (
                    SELECT *
                    FROM intersections_raw
                    WHERE NOT ST_IsEmpty({_geometry_column})
                      AND ST_Dimension({_geometry_column}) = 2
                )
            """
        )
        ctes.append(
            f"""
                b_only_raw AS (
                    SELECT
                        {b_only_projections},
                        CASE
                            WHEN overlap.geom IS NULL THEN b.{_geometry_column}
                            ELSE ST_Difference(b.{_geometry_column}, overlap.geom)
                        END AS {_geometry_column}
                    FROM b
                    LEFT JOIN LATERAL (
                        SELECT ST_Union_Agg(a.{_geometry_column}) AS geom
                        FROM a
                        WHERE ST_Intersects(b.{_geometry_column}, a.{_geometry_column})
                    ) AS overlap ON TRUE
                )
            """
        )
        ctes.append(
            f"""
                b_only AS (
                    SELECT *
                    FROM b_only_raw
                    WHERE NOT ST_IsEmpty({_geometry_column})
                      AND ST_Dimension({_geometry_column}) = 2
                )
            """
        )
        final_query = """
            SELECT *
            FROM a_only
            UNION ALL
            SELECT *
            FROM intersections
            UNION ALL
            SELECT *
            FROM b_only
        """
    else:
        ctes.append(
            f"""
                b_only_raw AS (
                    SELECT
                        {b_only_projections},
                        CASE
                            WHEN overlap.geom IS NULL THEN b.{_geometry_column}
                            ELSE ST_Difference(b.{_geometry_column}, overlap.geom)
                        END AS {_geometry_column}
                    FROM b
                    LEFT JOIN LATERAL (
                        SELECT ST_Union_Agg(a.{_geometry_column}) AS geom
                        FROM a
                        WHERE ST_Intersects(b.{_geometry_column}, a.{_geometry_column})
                    ) AS overlap ON TRUE
                )
            """
        )
        ctes.append(
            f"""
                b_only AS (
                    SELECT *
                    FROM b_only_raw
                    WHERE NOT ST_IsEmpty({_geometry_column})
                      AND ST_Dimension({_geometry_column}) = 2
                )
            """
        )
        final_query = """
            SELECT *
            FROM a_only
            UNION ALL
            SELECT *
            FROM b_only
        """

    cte_query = ",\n".join(ctes)
    return f"""
        WITH {cte_query}
        {final_query}
    """


def _copy_query_to_gpkg(
    connection: duckdb.DuckDBPyConnection, query: str, output_path: Path
) -> None:
    sql = f"""
        COPY ({query})
        TO {_sql_literal(output_path.as_posix())}
        WITH (FORMAT GDAL, DRIVER 'GPKG')
    """
    connection.execute(sql)


def _copy_query_to_geoparquet(
    connection: duckdb.DuckDBPyConnection, query: str, output_path: Path
) -> None:
    sql = f"""
        COPY ({query})
        TO {_sql_literal(output_path.as_posix())}
        (FORMAT PARQUET)
    """
    connection.execute(sql)


def _prepare_inputs(
    tmp_dir: Path, input_paths: list[Path]
) -> tuple[Path | None, list[Path]]:
    if _file_format == "gpkg":
        return None, input_paths

    if _file_format == "geoparquet":
        connection = _connect()
        try:
            parquet_paths = [tmp_dir / f"{path.stem}.parquet" for path in input_paths]
            for input_path, parquet_path in zip(
                input_paths, parquet_paths, strict=True
            ):
                if not parquet_path.exists():
                    query = (
                        f"SELECT * FROM ST_Read({_sql_literal(input_path.as_posix())})"
                    )
                    _copy_query_to_geoparquet(connection, query, parquet_path)
            return None, parquet_paths
        finally:
            connection.close()

    database_path = tmp_dir / "duckdb_inputs.duckdb"
    connection = _connect(database_path)
    try:
        existing_tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
        """
        existing_tables = {
            row[0] for row in connection.execute(existing_tables_query).fetchall()
        }
        table_paths = []
        for input_path in input_paths:
            table_name = input_path.stem
            if table_name not in existing_tables:
                query = f"""
                    CREATE TABLE {_sql_identifier(table_name)} AS
                    SELECT *
                    FROM ST_Read({_sql_literal(input_path.as_posix())})
                """
                connection.execute(query)
                existing_tables.add(table_name)
            table_paths.append(Path(table_name))
        return database_path, table_paths
    finally:
        connection.close()


def _write_query_result(
    connection: duckdb.DuckDBPyConnection, query: str, output_path: Path
) -> None:
    if _file_format == "gpkg":
        _copy_query_to_gpkg(connection, query, output_path)
    elif _file_format == "geoparquet":
        _copy_query_to_geoparquet(connection, query, output_path)
    else:
        output_database = "benchmark_output"
        attach_query = f"""
            ATTACH {_sql_literal(output_path.as_posix())}
            AS {_sql_identifier(output_database)}
        """
        connection.execute(attach_query)
        create_table_query = f"""
            CREATE TABLE {_sql_identifier(output_database)}.
                {_sql_identifier(output_path.stem)} AS
                {query}
        """
        connection.execute(create_table_query)


def _run_benchmark(  # noqa: PLR0917
    operation: str,
    operation_descr: str,
    input1_path: Path,
    tmp_dir: Path,
    input2_path: Path | None = None,
    result_operation: str | None = None,
) -> RunResult:
    result_operation = result_operation or operation
    input_paths = [input1_path]
    if input2_path is not None:
        input_paths.append(input2_path)
    package_name = _package_for_format(_file_format)
    database_path, query_paths = _prepare_inputs(tmp_dir, input_paths)
    query_input1 = query_paths[0]
    query_input2 = query_paths[1] if input2_path is not None else None
    input2_stem = f"_{input2_path.stem}" if input2_path else ""
    output_path = (
        tmp_dir / f"{input1_path.stem}_{result_operation}{input2_stem}_{package_name}."
        f"{_file_format_extensions[_file_format]}"
    )
    output_path.unlink(missing_ok=True)
    connection = _connect(database_path)
    try:
        start_time = datetime.now()
        query = _build_operation_query(
            connection, operation, query_input1, query_input2
        )
        _write_query_result(connection, query, output_path)
        secs_taken = (datetime.now() - start_time).total_seconds()
        run_details = {
            "nb_cpu": _nb_parallel,
            "file_format": _file_format,
            "io": {
                "gpkg": "ST_Read (single-threaded), GDAL COPY to GPKG",
                "geoparquet": "read_parquet, COPY to GeoParquet",
                "duckdb": "native DuckDB table read/write",
            }[_file_format],
        }
        if operation == "join_by_location_intersects":
            run_details["spatial_index"] = "R-tree does not accelerate this join"
        logger.info(f"{result_operation} took {secs_taken:.2f} s")
        return RunResult(
            package=package_name,
            package_version=_package_version,
            operation=result_operation,
            secs_taken=secs_taken,
            operation_descr=operation_descr,
            run_details=run_details,
        )
    finally:
        connection.close()
        output_path.unlink(missing_ok=True)


def buffer_agriprc(tmp_dir: Path) -> RunResult:
    """Benchmark DuckDB's buffer operation for the configured file format."""
    input_path, _ = _test_file(tmp_dir, "AGRIPRC_2018")
    return _run_benchmark(
        operation="buffer",
        operation_descr="buffer agri parcels BEFL (~500k polygons), distance=1",
        input1_path=input_path,
        tmp_dir=tmp_dir,
        result_operation="buffer_agriprc",
    )


def buffer_sealed(tmp_dir: Path) -> RunResult:
    """Benchmark DuckDB's buffer operation on sealed surfaces."""
    input_path, _ = _test_file(tmp_dir, "SEALED_2019")
    return _run_benchmark(
        operation="buffer",
        operation_descr="buffer sealed surfaces BEFL (~2.6M polygons), distance=1",
        input1_path=input_path,
        tmp_dir=tmp_dir,
        result_operation="buffer_sealed",
    )


def dissolve(tmp_dir: Path) -> RunResult:
    """Benchmark DuckDB's dissolve operation for the configured file format."""
    input_path, _ = _test_file(tmp_dir, "AGRIPRC_2018")
    return _run_benchmark(
        operation="dissolve",
        operation_descr="dissolve agri parcels BEFL (~500k polygons)",
        input1_path=input_path,
        tmp_dir=tmp_dir,
        result_operation="dissolve",
    )


def dissolve_groupby(tmp_dir: Path) -> RunResult:
    """Benchmark DuckDB's grouped dissolve for the configured file format."""
    input_path, _ = _test_file(tmp_dir, "AGRIPRC_2018")
    return _run_benchmark(
        operation="dissolve_groupby",
        operation_descr="dissolve agri parcels BEFL (~500k polys), groupby=GWSGRPH_LB",
        input1_path=input_path,
        tmp_dir=tmp_dir,
        result_operation="dissolve_groupby",
    )


def intersection(tmp_dir: Path) -> RunResult:
    """Benchmark DuckDB's intersection operation for the configured file format."""
    input1_path, _ = _test_file(tmp_dir, "AGRIPRC_2018")
    input2_path, _ = _test_file(tmp_dir, "AGRIPRC_2019")
    return _run_benchmark(
        operation="intersection",
        operation_descr="intersection of 2 agri parcel layers BEFL (2*~500k polygons)",
        input1_path=input1_path,
        tmp_dir=tmp_dir,
        input2_path=input2_path,
    )


def union(tmp_dir: Path) -> RunResult:
    """Benchmark DuckDB's union operation for the configured file format."""
    input1_path, _ = _test_file(tmp_dir, "AGRIPRC_2018")
    input2_path, _ = _test_file(tmp_dir, "AGRIPRC_2019")
    return _run_benchmark(
        operation="union",
        operation_descr="union of 2 agri parcel layers BEFL (2*~500k polygons)",
        input1_path=input1_path,
        tmp_dir=tmp_dir,
        input2_path=input2_path,
    )


def _join_by_location_intersects(tmp_dir: Path) -> RunResult:
    input1_path, _ = _test_file(tmp_dir, "AGRIPRC_2018")
    input2_path, _ = _test_file(tmp_dir, "AGRIPRC_2019")
    return _run_benchmark(
        operation="join_by_location_intersects",
        operation_descr="intersects join of 2 agri parcel layers BEFL (2*~500k polys)",
        input1_path=input1_path,
        tmp_dir=tmp_dir,
        input2_path=input2_path,
        result_operation="join_by_location_intersects",
    )


def _symdif_complexpolys_agriprc(tmp_dir: Path) -> RunResult:
    import geofileops as gfo  # noqa: PLC0415
    import shapely  # noqa: PLC0415

    import testdata  # noqa: PLC0415

    input1_path, input1_descr = testdata.TestFile.AGRIPRC_2018.get_file(tmp_dir)
    info = gfo.get_layerinfo(input1_path)
    bbox = shapely.box(*info.total_bounds).buffer(-10_000, join_style="mitre").bounds
    input2_path, input2_descr = testdata.create_testfile(
        bbox=bbox,
        geoms=3,
        polys_per_geom=4,
        points_per_poly=30_000,
        poly_width=30_000,
        poly_height=30_000,
        crs=info.crs,
        dst_dir=tmp_dir,
    )
    return _run_benchmark(
        operation="symdifference",
        operation_descr=f"symmetric difference of {input1_descr} and {input2_descr}",
        input1_path=input1_path,
        tmp_dir=tmp_dir,
        input2_path=input2_path,
        result_operation="symdif_complexpolys_agriprc",
    )
