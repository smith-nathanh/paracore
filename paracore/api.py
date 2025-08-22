"""Public API for paracore."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Literal, Mapping, Optional, Union

from paracore.config import get_config
from paracore.submitit_backend import SubmititBackend


@dataclass
class SubmitHandle:
    """Handle to a submitted job."""

    job_id: str
    job_name: str
    array_index: Optional[int] = None
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None
    _backend_job: Any = None

    def result(self, timeout: Optional[float] = None) -> Any:
        """Wait for and return job result."""
        if self._backend_job is None:
            raise RuntimeError("No backend job associated with this handle")
        return self._backend_job.result(timeout=timeout)

    def done(self) -> bool:
        """Check if job is done."""
        if self._backend_job is None:
            raise RuntimeError("No backend job associated with this handle")
        return self._backend_job.done()

    def cancel(self) -> None:
        """Cancel the job."""
        if self._backend_job is None:
            raise RuntimeError("No backend job associated with this handle")
        self._backend_job.cancel()


def run_cmd(
    cmd: str,
    *,
    job_name: Optional[str] = None,
    partition: Optional[str] = None,
    time_min: Optional[int] = None,
    cpus_per_task: Optional[int] = None,
    mem_gb: Optional[int] = None,
    env_setup: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    env_merge: Literal["inherit", "replace"] = "inherit",
    jitter_s: float = 0.0,
    account: Optional[str] = None,
    qos: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
    retries: int = 0,
    retry_backoff_s: float = 30.0,
) -> SubmitHandle:
    """Submit a single command to the cluster."""
    config = get_config()
    backend = SubmititBackend(config)

    # Apply jitter if requested
    if jitter_s > 0:
        time.sleep(random.uniform(0, jitter_s))

    # Submit with retries
    attempt = 0
    while True:
        try:
            return backend.submit_cmd(
                cmd=cmd,
                job_name=job_name,
                partition=partition,
                time_min=time_min,
                cpus_per_task=cpus_per_task,
                mem_gb=mem_gb,
                env_setup=env_setup,
                env=env,
                env_merge=env_merge,
                account=account,
                qos=qos,
                extra=extra,
            )
        except Exception:
            if attempt >= retries:
                raise
            attempt += 1
            backoff = retry_backoff_s * (2 ** (attempt - 1))
            time.sleep(backoff + random.uniform(0, backoff * 0.1))


def map_cmds(
    cmds: Iterable[str],
    *,
    job_name: Optional[str] = None,
    partition: Optional[str] = None,
    time_min: Optional[int] = None,
    cpus_per_task: Optional[int] = None,
    mem_gb: Optional[int] = None,
    env_setup: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    env_merge: Literal["inherit", "replace"] = "inherit",
    jitter_s: float = 0.0,
    array_parallelism: Optional[int] = None,
    account: Optional[str] = None,
    qos: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
    retries: int = 0,
    retry_backoff_s: float = 30.0,
) -> list[SubmitHandle]:
    """Submit an array of commands to the cluster."""
    config = get_config()
    backend = SubmititBackend(config)

    # Apply jitter if requested
    if jitter_s > 0:
        time.sleep(random.uniform(0, jitter_s))

    # Submit with retries
    attempt = 0
    while True:
        try:
            return backend.submit_cmd_array(
                cmds=list(cmds),
                job_name=job_name,
                partition=partition,
                time_min=time_min,
                cpus_per_task=cpus_per_task,
                mem_gb=mem_gb,
                env_setup=env_setup,
                env=env,
                env_merge=env_merge,
                array_parallelism=array_parallelism,
                account=account,
                qos=qos,
                extra=extra,
            )
        except Exception:
            if attempt >= retries:
                raise
            attempt += 1
            backoff = retry_backoff_s * (2 ** (attempt - 1))
            time.sleep(backoff + random.uniform(0, backoff * 0.1))


def map_func(
    fn: Callable[[Any], Any],
    items: Iterable[Any],
    *,
    job_name: Optional[str] = None,
    partition: Optional[str] = None,
    time_min: Optional[int] = None,
    cpus_per_task: Optional[int] = None,
    mem_gb: Optional[int] = None,
    env_setup: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
    env_merge: Literal["inherit", "replace"] = "inherit",
    jitter_s: float = 0.0,
    array_parallelism: Optional[int] = None,
    account: Optional[str] = None,
    qos: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
    retries: int = 0,
    retry_backoff_s: float = 30.0,
) -> list[SubmitHandle]:
    """Submit a function mapped over items to the cluster."""
    config = get_config()
    backend = SubmititBackend(config)

    # Apply jitter if requested
    if jitter_s > 0:
        time.sleep(random.uniform(0, jitter_s))

    # Submit with retries
    attempt = 0
    while True:
        try:
            return backend.submit_func_array(
                fn=fn,
                items=list(items),
                job_name=job_name,
                partition=partition,
                time_min=time_min,
                cpus_per_task=cpus_per_task,
                mem_gb=mem_gb,
                env_setup=env_setup,
                env=env,
                env_merge=env_merge,
                array_parallelism=array_parallelism,
                account=account,
                qos=qos,
                extra=extra,
            )
        except Exception:
            if attempt >= retries:
                raise
            attempt += 1
            backoff = retry_backoff_s * (2 ** (attempt - 1))
            time.sleep(backoff + random.uniform(0, backoff * 0.1))


def autotune_from_pilot(
    sample_cmds_or_items: Iterable[Union[str, Any]],
    *,
    runner: Literal["cmds", "func"],
    fn: Optional[Callable] = None,
    sample_size: int = 20,
    measurement: Literal["time_only", "time_and_rss"] = "time_and_rss",
    partition: Optional[str] = None,
    cpus_per_task_guess: int = 4,
    mem_gb_guess: int = 8,
    time_min_guess: int = 30,
    env_setup: Optional[str] = None,
) -> dict[str, Any]:
    """Run a pilot sample and suggest resource parameters."""
    config = get_config()
    backend = SubmititBackend(config)

    # Sample items
    items_list = list(sample_cmds_or_items)
    if len(items_list) > sample_size:
        import random

        items_list = random.sample(items_list, sample_size)

    # Run pilot
    if runner == "cmds":
        pilot_jobs = backend.submit_cmd_array(
            cmds=items_list,
            job_name="paracore-pilot",
            partition=partition,
            time_min=time_min_guess,
            cpus_per_task=cpus_per_task_guess,
            mem_gb=mem_gb_guess,
            env_setup=env_setup,
            array_parallelism=None,
            measure_resources=measurement == "time_and_rss",
        )
    else:
        if fn is None:
            raise ValueError("fn must be provided when runner='func'")
        pilot_jobs = backend.submit_func_array(
            fn=fn,
            items=items_list,
            job_name="paracore-pilot",
            partition=partition,
            time_min=time_min_guess,
            cpus_per_task=cpus_per_task_guess,
            mem_gb=mem_gb_guess,
            env_setup=env_setup,
            array_parallelism=None,
            measure_resources=measurement == "time_and_rss",
        )

    # Collect results
    durations = []
    max_rss_mb = 0

    for job in pilot_jobs:
        try:
            result = job.result()
            if isinstance(result, dict) and "_paracore_metrics" in result:
                metrics = result["_paracore_metrics"]
                durations.append(metrics.get("duration_s", 0))
                max_rss_mb = max(max_rss_mb, metrics.get("max_rss_mb", 0))
        except Exception:
            pass

    if not durations:
        # Fallback to guesses if pilot failed
        return {
            "time_min": time_min_guess,
            "mem_gb": mem_gb_guess,
            "cpus_per_task": cpus_per_task_guess,
            "array_parallelism": config.get_cluster_config()
            .get("slurm", {})
            .get("max_array_parallelism", 100),
        }

    # Compute recommendations
    import math

    p95_duration = sorted(durations)[int(len(durations) * 0.95)]

    recommendations = {
        "time_min": math.ceil(p95_duration * 1.3 / 60),
        "mem_gb": math.ceil(max_rss_mb * 1.3 / 1024) if max_rss_mb > 0 else mem_gb_guess,
        "cpus_per_task": cpus_per_task_guess,
        "array_parallelism": config.get_cluster_config()
        .get("slurm", {})
        .get("max_array_parallelism", 100),
    }

    return recommendations
