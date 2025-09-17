# Repository Guidelines

## Project Structure & Module Organization
- `paracore/` houses the core package; start with `cli.py` for the command-line entrypoint and `api.py` for the Python API helpers.
- `config.py`, `dask_helpers.py`, and `submitit_backend.py` wrap Slurm-oriented configuration and execution primitives; keep related utilities beside these modules.
- `examples/` contains runnable snippets, including `examples/commands.txt` for batch submission templates.
- `tests/` mirrors the package layout; create new test files as `tests/test_<feature>.py` and co-locate fixtures near their subject.

## Build, Test, and Development Commands
- `uv pip install -e .[dev]` sets up an editable install with developer dependencies.
- `uv run pytest` executes the full test suite.
- `uv run mypy paracore` enforces type safety, which CI assumes passes.
- `uv run ruff check paracore tests` runs linting; pair with `uv run ruff format` before submitting.

## Coding Style & Naming Conventions
- Follow PEP 8 with `ruff` and `black` defaults; keep line length ≤100 characters.
- Use `snake_case` for functions and modules, `PascalCase` for classes, and descriptive Slurm resource flags when constructing command wrappers.
- Add type hints everywhere; mypy’s strict mode rejects untyped defs.
- Prefer straightforward, synchronous flows; document non-obvious asynchronous or Slurm-specific behavior inline.

## Testing Guidelines
- Write pytest tests that exercise both CLI flows (via `paracore.cli.main`) and API helpers (`run_cmd`, `map_cmds`).
- Name tests `test_<subject>_<behavior>` and minimize reliance on real clusters by faking submitit clients.
- Aim for coverage of resource parsing, error propagation, and job orchestration logic before touching new features.

## Commit & Pull Request Guidelines
- Follow the repo’s short imperative commit style (e.g., `cleanup`, `linting`, `add submitit hooks`).
- Reference related issues or Slurm tickets in the body when applicable and explain validation steps.
- For PRs, include a summary of changes, expected impact, and local command output (`pytest`, `mypy`, `ruff`).
- Attach CLI transcripts or config snippets when changes affect job submission defaults.

## Potential Enhancements Before Cluster Rollout
- Swap `subprocess.run(..., capture_output=True)` for streaming stdout/stderr so verbose jobs avoid OOM on workers.
- Enforce `max_array_parallelism` from config when setting `array_parallelism` to protect the scheduler from spikes.
- Implement `paracore status` with thin `sacct`/`squeue` wrappers for quick job inspection at scale.
- Warn or chunk when `map_cmds` materializes very large iterables to keep driver memory stable.
- Add optional logging/dry-run output of resolved Slurm params to double-check resources during onboarding.
