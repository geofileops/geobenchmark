from contextlib import contextmanager
from pathlib import Path
from types import ModuleType

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
        Path, "glob", lambda self, pattern: [Path("benchmarks_fake.py")]
    )
    monkeypatch.setattr(
        benchmarker.importlib,
        "import_module",
        lambda *args, **kwargs: module,
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
    def _benchmark_configuration(**config):
        raise AssertionError("configuration hook should not run")
        yield

    def buffer(tmp_dir: Path) -> None:
        calls.append(tmp_dir)

    setattr(module, "_benchmark_configuration", _benchmark_configuration)
    _add_benchmark_function(module, buffer)
    setattr(module, "contextmanager", contextmanager)

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

    def buffer(tmp_dir: Path) -> None:
        calls.append(("buffer", active_configuration[-1]))

    def dissolve(tmp_dir: Path) -> None:
        calls.append(("dissolve", active_configuration[-1]))

    setattr(module, "_benchmark_configuration", _benchmark_configuration)
    setattr(
        module,
        "DEFAULT_BENCHMARK_CONFIGS",
        [
            {"file_format": "gpkg"},
            {"file_format": "geoparquet"},
        ],
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

    def buffer(tmp_dir: Path) -> None:
        calls.append(active_configuration[-1])

    setattr(module, "_benchmark_configuration", _benchmark_configuration)
    setattr(
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

    def buffer(tmp_dir: Path) -> None:
        calls.append(active_configuration[-1])

    setattr(module, "_benchmark_configuration", _benchmark_configuration)
    setattr(module, "DEFAULT_BENCHMARK_CONFIGS", [{"file_format": "gpkg"}])
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

    def buffer(tmp_dir: Path) -> None:
        raise RuntimeError("benchmark failed")

    setattr(module, "_benchmark_configuration", _benchmark_configuration)
    _add_benchmark_function(module, buffer)

    with pytest.raises(RuntimeError, match="benchmark failed"):
        _run_fake_module(
            monkeypatch,
            tmp_path,
            module,
            modules=[("benchmarks_fake", {"file_format": "gpkg"})],
        )

    assert active_configuration == []
