from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pandas as pd
import pytest

import benchmarker
from benchmarker import _should_run_benchmark_function


def _run_fake_module(
    monkeypatch,
    tmp_path: Path,
    module: ModuleType,
    modules=None,
    discover_all: bool = False,
    **kwargs,
) -> None:
    monkeypatch.setattr(
        Path, "glob", lambda _self, _pattern: [Path("benchmarks_fake.py")]
    )
    monkeypatch.setattr(
        benchmarker,
        "importlib",
        SimpleNamespace(import_module=lambda *_args, **_kwargs: module),
    )
    benchmarker.run_benchmarks(
        benchmarks_subdir="fake_benchmarks",
        results_subdir=str(tmp_path / "results"),
        modules=None if discover_all else modules or ["benchmarks_fake"],
        **kwargs,
    )


def _add_benchmark_function(module: ModuleType, function) -> None:
    function.__module__ = module.__name__
    setattr(module, function.__name__, function)


def test_private_benchmark_is_skipped_during_discovery() -> None:
    assert not _should_run_benchmark_function("_stress_case", None)


def test_private_benchmark_can_be_selected_explicitly() -> None:
    assert _should_run_benchmark_function("_stress_case", ["_stress_case"])


def test_public_benchmark_discovery_and_whitelist() -> None:
    assert _should_run_benchmark_function("buffer", None)
    assert _should_run_benchmark_function("buffer", ["buffer"])
    assert not _should_run_benchmark_function("buffer", ["intersection"])


def test_run_benchmarks_without_module_defaults_keeps_tmp_dir_call(
    monkeypatch, tmp_path: Path
) -> None:
    module = ModuleType("benchmarks_fake")
    calls = []

    @contextmanager
    def _benchmark_configuration(**_config):
        raise AssertionError("configuration hook should not run")
        yield

    def buffer(tmp_dir: Path) -> None:
        calls.append(tmp_dir)

    setattr(module, "_benchmark_configuration", _benchmark_configuration)  # noqa: B010
    _add_benchmark_function(module, buffer)
    setattr(module, "contextmanager", contextmanager)  # noqa: B010

    _run_fake_module(monkeypatch, tmp_path, module)

    assert len(calls) == 1
    assert isinstance(calls[0], Path)


def test_run_benchmarks_repeats_module_defaults_and_filters_functions(
    monkeypatch, tmp_path: Path
) -> None:
    module = ModuleType("benchmarks_fake")
    active_configuration = []
    calls = []

    @contextmanager
    def _benchmark_configuration(file_format: str):
        active_configuration.append(file_format)
        try:
            yield
        finally:
            active_configuration.pop()

    def buffer(tmp_dir: Path) -> None:  # noqa: ARG001
        calls.append(("buffer", active_configuration[-1]))

    def dissolve(tmp_dir: Path) -> None:  # noqa: ARG001
        calls.append(("dissolve", active_configuration[-1]))

    setattr(module, "_benchmark_configuration", _benchmark_configuration)  # noqa: B010
    setattr(  # noqa: B010
        module,
        "DEFAULT_BENCHMARK_CONFIGS",
        [{"file_format": "gpkg"}, {"file_format": "geoparquet"}],
    )
    _add_benchmark_function(module, buffer)
    _add_benchmark_function(module, dissolve)

    _run_fake_module(
        monkeypatch,
        tmp_path,
        module,
        functions=["buffer", "_benchmark_configuration"],
    )

    assert calls == [("buffer", "gpkg"), ("buffer", "geoparquet")]
    assert active_configuration == []


def test_run_benchmarks_all_modules_uses_module_defaults(
    monkeypatch, tmp_path: Path
) -> None:
    module = ModuleType("benchmarks_fake")
    active_configuration = []
    calls = []

    @contextmanager
    def _benchmark_configuration(file_format: str):
        active_configuration.append(file_format)
        try:
            yield
        finally:
            active_configuration.pop()

    def buffer(tmp_dir: Path) -> None:  # noqa: ARG001
        calls.append(active_configuration[-1])

    setattr(module, "_benchmark_configuration", _benchmark_configuration)  # noqa: B010
    setattr(  # noqa: B010
        module,
        "DEFAULT_BENCHMARK_CONFIGS",
        [{"file_format": "gpkg"}, {"file_format": "geoparquet"}],
    )
    _add_benchmark_function(module, buffer)

    _run_fake_module(monkeypatch, tmp_path, module, discover_all=True)

    assert calls == ["gpkg", "geoparquet"]
    assert active_configuration == []


def test_tuple_module_specs_override_module_defaults(
    monkeypatch, tmp_path: Path
) -> None:
    module = ModuleType("benchmarks_fake")
    active_configuration = []
    calls = []

    @contextmanager
    def _benchmark_configuration(file_format: str):
        active_configuration.append(file_format)
        try:
            yield
        finally:
            active_configuration.pop()

    def buffer(tmp_dir: Path) -> None:  # noqa: ARG001
        calls.append(active_configuration[-1])

    setattr(module, "_benchmark_configuration", _benchmark_configuration)  # noqa: B010
    setattr(module, "DEFAULT_BENCHMARK_CONFIGS", [{"file_format": "gpkg"}])  # noqa: B010
    _add_benchmark_function(module, buffer)

    _run_fake_module(
        monkeypatch,
        tmp_path,
        module,
        modules=[
            ("benchmarks_fake", {"file_format": "geoparquet"}),
            ("benchmarks_fake", {"file_format": "duckdb"}),
        ],
    )

    assert calls == ["geoparquet", "duckdb"]
    assert active_configuration == []


def test_run_benchmarks_restores_module_configuration_on_error(
    monkeypatch, tmp_path: Path
) -> None:
    module = ModuleType("benchmarks_fake")
    active_configuration = []

    @contextmanager
    def _benchmark_configuration(file_format: str):
        active_configuration.append(file_format)
        try:
            yield
        finally:
            active_configuration.pop()

    def buffer(tmp_dir: Path) -> None:  # noqa: ARG001
        raise RuntimeError("benchmark failed")

    setattr(module, "_benchmark_configuration", _benchmark_configuration)  # noqa: B010
    _add_benchmark_function(module, buffer)

    with pytest.raises(RuntimeError, match="benchmark failed"):
        _run_fake_module(
            monkeypatch,
            tmp_path,
            module,
            modules=[("benchmarks_fake", {"file_format": "gpkg"})],
        )

    assert active_configuration == []


def test_available_memory_sampler_takes_initial_and_final_samples(monkeypatch) -> None:
    available_values = iter([1_000, 800])
    monkeypatch.setattr(
        benchmarker.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(available=next(available_values)),
    )

    with benchmarker._AvailableMemorySampler(interval_seconds=60) as sampler:
        pass

    assert [value for _, value in sampler.samples] == [1_000, 800]


def test_available_memory_sampler_takes_final_sample_on_exception(monkeypatch) -> None:
    available_values = iter([1_000, 800])
    monkeypatch.setattr(
        benchmarker.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(available=next(available_values)),
    )
    sampler = benchmarker._AvailableMemorySampler(interval_seconds=60)

    with pytest.raises(RuntimeError, match="benchmark failed"):
        with sampler:
            raise RuntimeError("benchmark failed")

    assert [value for _, value in sampler.samples] == [1_000, 800]


def test_record_result_memory_uses_each_result_interval() -> None:
    start = datetime(2026, 1, 1)
    samples = [
        (start, 1_200),
        (start + timedelta(seconds=0.1), 1_000),
        (start + timedelta(seconds=0.2), 800),
        (start + timedelta(seconds=0.25), 900),
        (start + timedelta(seconds=0.3), 950),
        (start + timedelta(seconds=0.35), 900),
        (start + timedelta(seconds=0.4), 700),
        (start + timedelta(seconds=0.5), 600),
        (start + timedelta(seconds=0.55), 650),
    ]
    first_result = benchmarker.RunResult("pkg", "1", "first", "first", 0.2)
    first_result.run_datetime = start + timedelta(seconds=0.25)
    second_result = benchmarker.RunResult("pkg", "1", "second", "second", 0.2)
    second_result.run_datetime = start + timedelta(seconds=0.55)

    benchmarker._record_result_memory(first_result, samples)
    benchmarker._record_result_memory(second_result, samples)

    assert first_result.max_memory_bytes == 400
    assert second_result.max_memory_bytes == 300


def test_record_result_memory_clamps_increase_and_leaves_missing_samples_null() -> None:
    start = datetime(2026, 1, 1)
    result = benchmarker.RunResult("pkg", "1", "op", "op", 0.2)
    result.run_datetime = start + timedelta(seconds=0.2)

    benchmarker._record_result_memory(
        result,
        [
            (start, 1_000),
            (start + timedelta(seconds=0.1), 1_100),
            (start + timedelta(seconds=0.2), 1_200),
        ],
    )
    assert result.max_memory_bytes == 0

    result_without_interval_samples = benchmarker.RunResult("pkg", "1", "op", "op", 0.1)
    result_without_interval_samples.run_datetime = start + timedelta(seconds=0.3)
    benchmarker._record_result_memory(
        result_without_interval_samples,
        [
            (start, 1_000),
            (start + timedelta(seconds=0.1), 900),
            (start + timedelta(seconds=0.5), 700),
        ],
    )

    assert result_without_interval_samples.max_memory_bytes is None


def test_run_benchmarks_records_each_result_interval(
    monkeypatch, tmp_path: Path
) -> None:
    result_end = datetime.now()
    samples = [
        (result_end - timedelta(seconds=0.2), 1_000),
        (result_end - timedelta(seconds=0.1), 900),
        (result_end + timedelta(seconds=0.01), 800),
    ]
    results = [
        benchmarker.RunResult("pkg", "1", "first", "first", 0.2),
        benchmarker.RunResult("pkg", "1", "second", "second", 0.1),
    ]
    for result in results:
        result.run_datetime = result_end

    class FixedMemorySampler:
        """Provide deterministic samples to the runner test."""

        def __enter__(self) -> "FixedMemorySampler":
            return self

        def __exit__(
            self,
            _exc_type: object,
            _exc_value: object,
            _traceback: object,
        ) -> None:
            pass

        @property
        def samples(self) -> list[tuple[datetime, int]]:
            return samples

    module = ModuleType("benchmarks_fake")

    def buffer(tmp_dir: Path) -> list[benchmarker.RunResult]:  # noqa: ARG001
        return results

    _add_benchmark_function(module, buffer)
    monkeypatch.setattr(benchmarker, "_AvailableMemorySampler", FixedMemorySampler)
    monkeypatch.setattr(
        benchmarker.reporter,
        "generate_reports",
        lambda *_args, **_kwargs: None,
    )

    _run_fake_module(monkeypatch, tmp_path, module)

    results_df = pd.read_csv(tmp_path / "results" / "benchmark_results.csv")
    assert results_df.columns.tolist() == [
        "run_datetime",
        "package",
        "package_version",
        "operation",
        "operation_descr",
        "secs_taken",
        "max_memory_bytes",
        "run_details",
    ]
    assert results_df["max_memory_bytes"].tolist() == [100, 100]
    assert "available_memory_start_bytes" not in results_df
    assert "available_memory_min_bytes" not in results_df
    assert "available_memory_drop_bytes" not in results_df


def test_record_result_memory_includes_post_return_sample_only_for_last_result() -> (
    None
):
    end = datetime(2026, 1, 1)
    samples = [
        (end - timedelta(seconds=0.2), 1_000),
        (end, 900),
        (end + timedelta(seconds=0.01), 700),
    ]
    first_result = benchmarker.RunResult("pkg", "1", "first", "first", 0.2)
    first_result.run_datetime = end
    last_result = benchmarker.RunResult("pkg", "1", "last", "last", 0.1)
    last_result.run_datetime = end

    benchmarker._record_result_memory(first_result, samples)
    benchmarker._record_result_memory(last_result, samples, include_final_sample=True)

    assert first_result.max_memory_bytes == 100
    assert last_result.max_memory_bytes == 300


def test_append_results_expands_legacy_csv_schema(tmp_path: Path) -> None:
    legacy_columns = [
        "run_datetime",
        "package",
        "package_version",
        "operation",
        "operation_descr",
        "secs_taken",
        "run_details",
    ]
    results_path = tmp_path / "benchmark_results.csv"
    pd.DataFrame(
        [
            [
                "2026-01-01",
                "pkg",
                "1.10",
                "old",
                "old",
                1.0,
                None,
            ],
        ],
        columns=legacy_columns,
    ).to_csv(results_path, index=False)
    result = benchmarker.RunResult("pkg", "2", "new", "new", 2.0)
    result.max_memory_bytes = 500

    benchmarker._append_results(results_path, [result])

    results_df = pd.read_csv(results_path, dtype={"package_version": str})
    assert results_df.columns.tolist() == [
        *legacy_columns[:6],
        "max_memory_bytes",
        *legacy_columns[6:],
    ]
    assert results_df.loc[0, "package"] == "pkg"
    assert results_df.loc[0, "package_version"] == "1.10"
    assert pd.isna(results_df.loc[0, "max_memory_bytes"])
    assert results_df.loc[1, "max_memory_bytes"] == 500
