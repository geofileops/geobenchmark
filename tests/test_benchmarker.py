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


def test_run_result_includes_os_in_run_details(monkeypatch) -> None:
    monkeypatch.setattr(benchmarker.platform, "system", lambda: "TestOS")
    details = {"driver": "test"}

    result = benchmarker.RunResult(
        "pkg", "1", "op", "description", 0.2, run_details=details
    )
    result_without_details = benchmarker.RunResult("pkg", "1", "op", "description", 0.2)

    assert result.run_details == {"driver": "test", "os": "TestOS"}
    assert details == {"driver": "test"}
    assert result_without_details.run_details == {"os": "TestOS"}


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


def test_memory_sampler_takes_initial_and_final_samples(monkeypatch) -> None:
    available_values = iter([1_000, 800])
    sample_events = []

    def virtual_memory():
        sample_events.append("sample")
        return SimpleNamespace(total=2_000, available=next(available_values))

    monkeypatch.setattr(
        benchmarker.gc, "collect", lambda: sample_events.append("collect")
    )
    monkeypatch.setattr(benchmarker.psutil, "virtual_memory", virtual_memory)

    with benchmarker._MemorySampler(interval_seconds=60) as sampler:
        pass

    assert [value for _, value in sampler.available_memory_samples] == [1_000, 800]
    assert [value for _, value in sampler.memory_used_samples] == [1_000, 1_200]
    assert sample_events == ["collect", "sample", "sample"]


def test_memory_sampler_takes_final_sample_on_exception(monkeypatch) -> None:
    available_values = iter([1_000, 800])
    monkeypatch.setattr(
        benchmarker.psutil,
        "virtual_memory",
        lambda: SimpleNamespace(total=2_000, available=next(available_values)),
    )
    sampler = benchmarker._MemorySampler(interval_seconds=60)

    with pytest.raises(RuntimeError, match="benchmark failed"):
        with sampler:
            raise RuntimeError("benchmark failed")

    assert [value for _, value in sampler.available_memory_samples] == [1_000, 800]


def test_memory_sampler_calculates_increase_for_each_result_interval() -> None:
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
    sampler = benchmarker._MemorySampler()
    sampler._total_memory_bytes = 2_000
    sampler._samples = samples

    assert (
        sampler.memory_increased_max(
            interval_start=start + timedelta(seconds=0.05),
            interval_end=start + timedelta(seconds=0.25),
        )
        == 400
    )
    assert (
        sampler.memory_increased_max(
            interval_start=start + timedelta(seconds=0.35),
            interval_end=start + timedelta(seconds=0.55),
        )
        == 300
    )


def test_memory_sampler_clamps_increase_and_returns_none_without_samples() -> None:
    start = datetime(2026, 1, 1)
    sampler = benchmarker._MemorySampler()
    sampler._total_memory_bytes = 2_000
    sampler._samples = [
        (start, 1_000),
        (start + timedelta(seconds=0.1), 1_100),
        (start + timedelta(seconds=0.2), 1_200),
    ]

    assert (
        sampler.memory_increased_max(
            interval_start=start + timedelta(seconds=0.2),
            interval_end=start + timedelta(seconds=0.3),
        )
        == 0
    )

    sampler._samples = [
        (start, 1_000),
        (start + timedelta(seconds=0.1), 900),
        (start + timedelta(seconds=0.5), 700),
    ]
    assert (
        sampler.memory_increased_max(
            interval_start=start + timedelta(seconds=0.2),
            interval_end=start + timedelta(seconds=0.3),
        )
        is None
    )


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

    class FixedMemorySampler(benchmarker._MemorySampler):
        """Provide deterministic samples to the runner test."""

        def __enter__(self) -> "FixedMemorySampler":
            self._total_memory_bytes = 2_000
            self._samples = samples
            return self

        def __exit__(
            self,
            _exc_type: object,
            _exc_value: object,
            _traceback: object,
        ) -> None:
            pass

    module = ModuleType("benchmarks_fake")

    def buffer(tmp_dir: Path) -> list[benchmarker.RunResult]:  # noqa: ARG001
        return results

    _add_benchmark_function(module, buffer)
    monkeypatch.setattr(benchmarker, "_MemorySampler", FixedMemorySampler)
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


def test_memory_sampler_includes_post_return_sample_only_for_last_result() -> None:
    end = datetime(2026, 1, 1)
    samples = [
        (end - timedelta(seconds=0.2), 1_000),
        (end, 900),
        (end + timedelta(seconds=0.01), 700),
    ]
    sampler = benchmarker._MemorySampler()
    sampler._total_memory_bytes = 2_000
    sampler._samples = samples

    assert (
        sampler.memory_increased_max(
            interval_start=end - timedelta(seconds=0.2),
            interval_end=end,
        )
        == 100
    )
    assert (
        sampler.memory_increased_max(
            interval_start=end - timedelta(seconds=0.1),
            interval_end=end,
            include_final_sample=True,
        )
        == 300
    )


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
