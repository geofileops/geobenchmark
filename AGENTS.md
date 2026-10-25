# Repository Guidance

## Benchmark conventions
- Benchmark modules use the `benchmarks_*.py` naming pattern.
- Public benchmark functions receive `tmp_dir` and return a `RunResult` or a
  list of results. See [benchmarker.py](benchmarker.py) for discovery and
  result handling.
- Use the shared test-data helpers in [testdata.py](testdata.py) and keep
  vector-operation scenarios comparable to the read-operate-write workflow in
  [README.md](README.md). Clearly describe any deviations.
- Before running a suite, inspect its runner's module and function selection.
  Runs append to results CSVs and regenerate reports; the vector runner
  currently selects GDAL CLI in its debug configuration. See
  [run_benchmarks_vector_ops.py](run_benchmarks_vector_ops.py).
- Use the environment file for the suite being changed. The README's generic
  environment setup example may not match the environment files present.

## Quality checks
- Follow the Python and Ruff settings in [project.toml](project.toml),
  including the 88-character line limit.
- Run the configured pre-commit checks when practical. They include Ruff
  formatting, Ruff linting, and mypy; CI runs pre-commit as well. See
  [.pre-commit-config.yaml](.pre-commit-config.yaml).
- Before running tests or pre-commit, activate the environment for the suite
  being checked. Activation may set variables required by native dependencies
  such as GDAL, so avoid invoking the environment's Python directly when those
  variables are needed.