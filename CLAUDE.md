# PARACORE — v1 Development Plan (PLAN.md)

## Implementation Status

### ✅ Completed (v1.0)
- **M0 — Scaffold**: Project structure, packaging (pyproject.toml), .gitignore
- **M1 — Submitit Core**: SubmitHandle, run_cmd, map_cmds, map_func with full parameter support
- **M2 — Config Layer**: YAML-based layered config with cluster profiles, resolution, and job naming
- **M3 — Retries**: Wrapper-level retry logic with exponential backoff and jitter
- **M4 — Autotune Pilot**: Pilot execution with time/RSS measurement and resource recommendations
- **M5 — Docs & Examples**: README, 5 comprehensive examples covering all features
- **M6 — Tests**: Unit tests for config, API, and backend components

### 🚀 Next Steps
1. **Testing on Real Cluster**: Deploy to dev cluster and verify:
   - Slurm job submission works correctly
   - Environment propagation and env_setup commands
   - Log file paths and permissions
   - Array job throttling behavior
   - Resource measurement accuracy

2. **Production Hardening**:
   - Add partition validation via `sinfo`
   - Improve error messages and failure diagnostics
   - Add logging/debugging capabilities
   - Handle edge cases (non-picklable functions, large arrays)

3. **Documentation Enhancement**:
   - Expand README with installation instructions
   - Add troubleshooting guide
   - Document best practices for large-scale jobs
   - Add API reference documentation

4. **Future Features (v1.1+)**:
   - Implement Dask helpers (start_dask_slurm, attach_dask)
   - Add audit manifest generation
   - Implement node-local scratch management
   - Add capacity-aware throttling
   - Kubernetes backend for GPU jobs

## Overview
Build **paracore**, a thin, opinionated wrapper over Slurm via **Submitit** that standardizes job submission for compute workloads. Provide a clean API with smart defaults, retry logic, environment handling, and optional pilot-based autotuning. Keep the door open for later helpers that start/attach **Dask** clusters or submit jobs to **Kubernetes**, without wrapping their APIs.

---

## Scope & Non‑Goals
**In scope (v1):**
- Submitit-based job submission (`run_cmd`, `map_cmds`, `map_func`)
- Config system with cluster profiles & per-call overrides
- Resource tuning knobs (partition, cpus, memory, walltime, array throttling)
- Retries with backoff
- Environment handling (inherit by default; optional `env_setup` and `env` dict)
- Job naming (`job_name`) with templates and sanitized defaults
- Pilot → Autotune helper to recommend resources
- Minimal, intuitive API + examples

**Out of scope (v1):**
- Wrapping Dask APIs or building a workflow/DAG engine
- Kubernetes backend (will come later)
- Custom monitoring UI (rely on Slurm logs and Submitit handles)

---

## High-Level Architecture
```
paracore/
  __init__.py
  api.py                    # public API (run_cmd, map_cmds, map_func, autotune_from_pilot)
  submitit_backend.py       # thin wrapper around submitit
  config.py                 # layered config + cluster profiles
  dask_helpers.py           # (v1.1) start/attach helpers only, no API wrapping
  examples/                 # runnable snippets
  docs/                     # short guides
```

---

## Public API (v1)

```python
# paracore.api

from typing import Iterable, Callable, Literal, Mapping, Any

class SubmitHandle:
    job_id: str
    job_name: str
    array_index: int | None
    stdout_path: str | None
    stderr_path: str | None
    def result(self, timeout: float | None = None) -> Any: ...
    def done(self) -> bool: ...
    def cancel(self) -> None: ...

def run_cmd(
    cmd: str,
    *,
    job_name: str | None = None,
    partition: str | None = None,
    time_min: int | None = None,
    cpus_per_task: int | None = None,
    mem_gb: int | None = None,
    env_setup: str | None = None,                # e.g., "envy prod"
    env: Mapping[str, str] | None = None,        # extra environment variables
    env_merge: Literal["inherit","replace"] = "inherit",
    jitter_s: float = 0.0,
    account: str | None = None,
    qos: str | None = None,
    extra: Mapping[str, Any] | None = None,      # raw #SBATCH extras
    retries: int = 0,
    retry_backoff_s: float = 30.0,
) -> SubmitHandle: ...

def map_cmds(
    cmds: Iterable[str],
    *,
    job_name: str | None = None,
    partition: str | None = None,
    time_min: int | None = None,
    cpus_per_task: int | None = None,
    mem_gb: int | None = None,
    env_setup: str | None = None,
    env: Mapping[str, str] | None = None,
    env_merge: Literal["inherit","replace"] = "inherit",
    jitter_s: float = 0.0,
    array_parallelism: int | None = None,        # -> "--array ...%N"
    account: str | None = None,
    qos: str | None = None,
    extra: Mapping[str, Any] | None = None,
    retries: int = 0,
    retry_backoff_s: float = 30.0,
) -> list[SubmitHandle]: ...

def map_func(
    fn: Callable[[Any], Any], items: Iterable[Any],
    *,
    job_name: str | None = None,
    partition: str | None = None,
    time_min: int | None = None,
    cpus_per_task: int | None = None,
    mem_gb: int | None = None,
    env_setup: str | None = None,
    env: Mapping[str, str] | None = None,
    env_merge: Literal["inherit","replace"] = "inherit",
    jitter_s: float = 0.0,
    array_parallelism: int | None = None,
    account: str | None = None,
    qos: str | None = None,
    extra: Mapping[str, Any] | None = None,
    retries: int = 0,
    retry_backoff_s: float = 30.0,
) -> list[SubmitHandle]: ...

def autotune_from_pilot(
    sample_cmds_or_items: Iterable[str | Any],
    *,
    runner: Literal["cmds","func"],
    fn: Callable | None = None,        # required if runner="func"
    sample_size: int = 20,
    measurement: Literal["time_only","time_and_rss"] = "time_and_rss",
    partition: str | None = None,
    cpus_per_task_guess: int = 4,
    mem_gb_guess: int = 8,
    time_min_guess: int = 30,
    env_setup: str | None = None,
) -> dict:  # returns {time_min, mem_gb, cpus_per_task, array_parallelism}
    ...
```

**Notes**
- **Defaults**: inherit parent env; `env_setup` optional; explicit `env` overlay with `env_merge="inherit"` or full `replace`.
- **Retries**: wrapper-level resubmission with backoff; small jitter permitted.
- **Array throttling**: user-provided `array_parallelism` or config default.

---

## Config System

### Layering & Precedence
1) Built-in safe defaults → 2) User `~/.paracore.yaml` → 3) Project `./paracore.yaml` → 4) Per-call overrides.

### Example YAML
```yaml
active_cluster: hpc1
naming:
  default_job_name: "{project}-{cluster}-{partition}"
  max_len: 80
clusters:
  hpc1:
    default_env: "envy prod"
    io_scratch: "$TMPDIR"
    slurm:
      partition: "compute"
      account: "compute"
      qos: null
      cpus_per_task: 4
      mem_gb: 16
      time_min: 60
      max_array_parallelism: 200
      start_jitter_s: 0
      extra: {}
  dev:
    default_env: "envy test"
    slurm:
      partition: "short"
      time_min: 30
      max_array_parallelism: 40
```

### Behavior
- `resolve(cluster=None, **overrides)` → flat dict of effective params.
- **Partition validation** (optional): `sinfo` check; clear error if invalid.
- Unknown fields → pass via `extra` to Submitit/Slurm.

---

## Job Naming
- API takes `job_name`; if omitted, render from template using tags where available:
  - Template variables: `{project}`, `{cluster}`, `{partition}`, `{env}` (and custom tags).
- Sanitize illegal chars and truncate to `max_len`, appending a short hash if needed.
- For arrays, logs include `%A_%a` in filenames for shard traceability.

---

## Submitit Backend (v1 behavior)
- Map API fields to Submitit:
  - `name=job_name` → `#SBATCH --job-name`
  - `slurm_partition`, `cpus_per_task`, `mem_gb`, `timeout_min`
  - `slurm_additional_parameters` for `output/error` file templates and raw extras
  - `slurm_account`, `slurm_qos` when provided
- **Env handling**:
  - Default: inherit current env.
  - If `env_setup` provided, run it once at task start.
  - Apply `env` dict per policy (`inherit` overlay or `replace`).
- **Retries**:
  - Catch failure of `job.result()`; resubmit the same payload up to `retries`, sleeping `retry_backoff_s * 2**attempt` (± jitter).

---

## Autotune From Pilot
**Goal:** suggest time/memory/threads/throttle from a small sample to protect queues and reduce trial-and-error.

- Run `sample_size` items (random subset).
- Measure **duration** and (optionally) **Max RSS**:
  - `time_only`: wall-clock timings inside the task.
  - `time_and_rss`: wrap the command with `/usr/bin/time -v` or parse Slurm step metrics if available.
- Compute recommendations:
  - `time_min = ceil(p95_duration * 1.3 / 60)`
  - `mem_gb = ceil(max_rss_mb * 1.3 / 1024)`
  - `cpus_per_task`: start with guess; (optional v1.1) micro-pilot 1→2→4 threads for saturation.
  - `array_parallelism`: use profile default or compute conservative value (e.g., 20–40% of partition cores if we add `sinfo` probing).

Return a dict suitable for direct splatting into `map_cmds/map_func`.

---

## Examples To Ship (examples/)

1. **Single job (Python data processor)**  
   `run_cmd("python process_data.py inputs/config_001.json results/output_001.json", partition="compute", job_name="data-process")`

2. **Array over configs, throttled**  
   `map_cmds([ ... ], partition="compute", array_parallelism=120, job_name="batch-process", retries=1)`

3. **map_func for Python function**  
   Top-level `def process_one(path)->str` writing shard outputs; submit with environment setup.

4. **Autotune pilot → full run**  
   Use `autotune_from_pilot` to pick `time_min/mem_gb/cpus_per_task`, then call `map_cmds` with suggestions.

5. **Environment policies**  
   Inherit-only, explicit overlay, and env replacement examples.


```python
# Example usage
from paracore.api import run_cmd, map_cmds, autotune_from_pilot

# single job
job = run_cmd(
    "python process_data.py inputs/config_001.json results/output_001.json",
    partition="compute",
    time_min=60, cpus_per_task=4, mem_gb=16,
    env={"APP_MODE": "production"},
    retries=1, retry_backoff_s=60,
)

# pilot → suggest knobs
suggest = autotune_from_pilot(
    sample_cmds_or_items=[f"inputs/config_{i:03d}.json" for i in range(40)],
    runner="cmds",
    sample_size=12,
)
# full run with suggested knobs
cmds = [f"python process_data.py inputs/config_{i:04d}.json results/output_{i:04d}.json" for i in range(100)]
jobs = map_cmds(
    cmds,
    partition="compute",
    **suggest,
    array_parallelism=20,        # or leave to config default
    retries=1,
)
```

```python 
# myjobs.py (top-level functions are best for pickling)
def process_path(cfg_path: str) -> dict:
    # load JSON, compute something in Python (or call a lib)
    import json
    with open(cfg_path) as f:
        cfg = json.load(f)
    # transform data
    return {"id": cfg["id"], "result": cfg["value"] * 2.5}

# submitter.py
from paracore.api import map_func

cfgs = [f"inputs/config_{i:04d}.json" for i in range(100)]
jobs = map_func(
    fn=__import__("myjobs").process_path,   # or from myjobs import process_path
    items=cfgs,
    partition="compute",
    cpus_per_task=4, mem_gb=16, time_min=60,
    retries=1
)

results = [j.result() for j in jobs]  # list of dicts
```

---

## Milestones

**M0 — Scaffold**
- Repo, packaging, lint/format, CI stub.

**M1 — Submitit Core**
- `SubmitHandle`, `run_cmd`, `map_cmds`, `map_func` with env handling, job naming, array throttle, partitions.

**M2 — Config Layer**
- YAML loader, layering, resolution, optional partition validation.

**M3 — Retries**
- Wrapper-level retries with backoff; tests for success/failure.

**M4 — Autotune Pilot**
- Pilot executor, time/RSS parsing, recommendation logic; example.

**M5 — Docs & Examples**
- README, PLAN.md, quickstart, examples.

**M6 — QA on Dev Cluster**
- Dry runs, log paths, permissions, env propagation verification.

---

## Roadmap (Post v1)

**v1.1**
- Dask helpers: `start_dask_slurm`, `attach_dask` (no API wrapping).
- Audit manifest (CSV/JSONL): job_id, job_name, tags, resources.
- Node-local scratch helper with safe move.
- Optional capacity-aware throttle using `sinfo`.

**v1.2**
- In-allocation Dask pool helper (scheduler+workers within one Slurm job).
- Simple sacct integration to classify retryable vs permanent failures.

**v2.0**
- Kubernetes backend for GPU jobs; mirror API shape to Slurm.
- Optional metrics exporter (Prometheus) and minimal status CLI.

---

## Testing Strategy
- **Unit**: config resolution, env merge, name sanitization, retry loop, argument pass-through.
- **Integration**: submit harmless jobs (e.g., `hostname`, `sleep`) on dev cluster; verify logs, job names, array indices.
- **Autotune edge cases**: long-tail durations, memory spikes, small sample bias.
- **Failure paths**: bad partition, bad commands, non-picklable functions, retries exhausted.

---

## Operational Guidelines
- Encourage **explicit `job_name`** for production runs.
- Prefer **file paths** (not large objects) for inputs/outputs in distributed `map_func`.
- Use **array throttling** to avoid overwhelming the scheduler/FS.
- Document **when to use Dask helpers** (fine-grained Python tasks, interactive DAGs) vs paracore (chunky batch jobs).
