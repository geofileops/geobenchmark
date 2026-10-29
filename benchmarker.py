"""Module for benchmarking."""

import datetime
import importlib
import inspect
import logging
import tempfile
from contextlib import nullcontext
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

import reporter

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

ModuleSpec = str | tuple[str, dict[str, Any]]


class RunResult:
    """The result of a benchmark run."""

    def __init__(  # noqa: PLR0917
        self,
        package: str,
        package_version: str,
        operation: str,
        operation_descr: str,
        secs_taken: float,
        run_details: dict | None = None,
    ) -> None:
        """Constructor for a RunResult.

        Args:
            package (str): Package being benchmarked.
            package_version (str): Version of the package.
            operation (str): Operation name.
            operation_descr (str): Description of the operation.
            secs_taken (float): Seconds the operation took.
            run_details (dict, optional): (Important) details of this specific
                run with impact on performance. Eg. # CPU's used,...
        """
        self.run_datetime = datetime.datetime.now()
        self.package = package
        self.package_version = package_version
        self.operation = operation
        self.operation_descr = operation_descr
        self.secs_taken = secs_taken
        self.run_details = run_details

    def __repr__(self) -> str:
        """Return a representation of the benchmark result."""
        return f"{self.__class__}({self.__dict__})"


def _should_run_benchmark_function(
    function_name: str, functions: list[str] | None
) -> bool:
    if functions is None:
        return not function_name.startswith("_")
    return function_name in functions


def _normalize_module_specs(
    modules: list[ModuleSpec] | None,
) -> tuple[set[str], dict[str, list[dict[str, Any]]]]:
    if modules is None:
        return set(), {}

    default_modules = set()
    explicit_configurations: dict[str, list[dict[str, Any]]] = {}
    for module_spec in modules:
        if isinstance(module_spec, str):
            if module_spec in explicit_configurations:
                raise ValueError(
                    f"Module {module_spec!r} cannot have both default and "
                    "explicit configurations in modules"
                )
            default_modules.add(module_spec)
            continue

        if (
            not isinstance(module_spec, tuple)
            or len(module_spec) != 2
            or not isinstance(module_spec[0], str)
            or not isinstance(module_spec[1], dict)
        ):
            raise TypeError(
                "Each module must be a name string or a "
                "(module_name, configuration_dict) tuple"
            )

        module_name, configuration = module_spec
        if module_name in default_modules:
            raise ValueError(
                f"Module {module_name!r} cannot have both default and explicit "
                "configurations in modules"
            )
        explicit_configurations.setdefault(module_name, []).append(configuration)

    return default_modules, explicit_configurations


def run_benchmarks(
    benchmarks_subdir: str = "benchmarks",
    results_subdir: str = "results",
    results_filename: str = "benchmark_results.csv",
    modules: list[ModuleSpec] | None = None,
    functions: list[str] | None = None,
) -> None:
    """Run selected benchmarks and append their results to the report."""
    # Init logging
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d|%(levelname)s|%(name)s|%(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )

    # Discover and run all benchmark implementations
    tmp_dir = Path(tempfile.gettempdir()) / "geobenchmark"
    logger.info(f"tmpdir: {tmp_dir}")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    benchmarks_dir = Path(__file__).parent / benchmarks_subdir
    default_modules, explicit_configurations = _normalize_module_specs(modules)
    results = []
    for file in benchmarks_dir.glob("benchmarks_*.py"):
        module_name = file.stem
        if (not module_name.startswith("_")) and (module_name not in globals()):
            if (
                modules is not None
                and module_name not in default_modules
                and module_name not in explicit_configurations
            ):
                # Benchmark whitelist specified, and this one isn't in it
                logger.info(f"skip module {module_name}: not in modules: {modules}")
                continue

            benchmark_implementation = importlib.import_module(
                f"{benchmarks_subdir}.{module_name}", __package__
            )
            module_configuration_hook = getattr(
                benchmark_implementation, "_benchmark_configuration", None
            )
            available_functions = [
                (function_name, function)
                for function_name, function in inspect.getmembers(
                    benchmark_implementation, inspect.isfunction
                )
                if function.__module__ == benchmark_implementation.__name__
                and function is not module_configuration_hook
            ]

            module_configurations: Sequence[dict[str, Any] | None]
            if module_name in explicit_configurations:
                module_configurations = explicit_configurations[module_name]
            elif modules is None or module_name in default_modules:
                module_defaults = getattr(
                    benchmark_implementation, "DEFAULT_BENCHMARK_CONFIGS", None
                )
                if module_defaults is None:
                    module_configurations = [None]
                elif not isinstance(module_defaults, (list, tuple)):
                    raise TypeError(
                        f"{module_name}.DEFAULT_BENCHMARK_CONFIGS must be a list "
                        "or tuple of configuration dictionaries"
                    )
                elif not module_defaults:
                    raise ValueError(
                        f"{module_name}.DEFAULT_BENCHMARK_CONFIGS cannot be empty"
                    )
                elif any(
                    not isinstance(configuration, dict)
                    for configuration in module_defaults
                ):
                    raise TypeError(
                        f"{module_name}.DEFAULT_BENCHMARK_CONFIGS must contain "
                        "only configuration dictionaries"
                    )
                else:
                    module_configurations = module_defaults
            else:
                module_configurations = [None]

            if any(
                configuration is not None for configuration in module_configurations
            ):
                if not module_configurations:
                    raise ValueError(
                        f"Configurations for {module_name!r} cannot be empty"
                    )
                if not callable(module_configuration_hook):
                    raise ValueError(
                        f"Module {module_name!r} has configurations but "
                        "does not define _benchmark_configuration"
                    )

            # Run the functions in this benchmark
            for module_configuration in module_configurations:
                if module_configuration is None:
                    configuration_context = nullcontext()
                else:
                    assert callable(module_configuration_hook)
                    configuration_context = module_configuration_hook(
                        **module_configuration
                    )
                with configuration_context:
                    for function_name, function in available_functions:
                        if not _should_run_benchmark_function(function_name, functions):
                            # Function whitelist specified, and this one isn't in it
                            logger.info(
                                f"skip function {function_name}: not in functions: "
                                f"{functions}"
                            )
                            continue

                        # Run the benchmark function
                        logger.info(
                            f"{benchmarks_subdir}.{module_name}.{function_name} start"
                        )
                        function_results = function(tmp_dir=tmp_dir)
                        if isinstance(function_results, list) is False:
                            function_results = [function_results]
                        for function_result in function_results:
                            if isinstance(function_result, RunResult) is True:
                                logger.info(
                                    f"{benchmarks_subdir}.{module_name}."
                                    f"{function_name} ready in "
                                    f"{function_result.secs_taken:.2f} s"
                                )
                                results.append(function_result)
                            else:
                                logger.warning(
                                    f"{benchmarks_subdir}.{module_name}."
                                    f"{function_name} ignored: instead of a "
                                    "RunResult it returned "
                                    f"{function_result}"
                                )

    # Add results to csv file
    if len(results) > 0:
        results_dir = Path(__file__).resolve().parent / results_subdir
        results_dir.mkdir(parents=True, exist_ok=True)
        results_path = results_dir / results_filename
        results_dictlist = [vars(result) for result in results]
        results_df = pd.DataFrame(results_dictlist)
        if not results_path.exists():
            results_df.to_csv(results_path, index=False)
        else:
            results_df.to_csv(results_path, index=False, mode="a", header=False)

        # Generate reports
        reporter.generate_reports(results_path, output_dir=results_dir)


if __name__ == "__main__":
    run_benchmarks()
