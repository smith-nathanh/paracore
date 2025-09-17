"""Public API for paracore."""

from __future__ import annotations

import math
import random
import subprocess
import time
from typing import Any, Callable, Iterable, Literal, Mapping, Optional, Union

from paracore.config import get_config
from paracore.submitit_backend import SubmititBackend
from paracore.types import SubmitHandle


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
        except Exception as exc:
            if isinstance(exc, KeyboardInterrupt):
                raise
            # Only retry on known transient errors
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
        except Exception as exc:
            if isinstance(exc, KeyboardInterrupt):
                raise
            # Only retry on known transient errors
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
        except Exception as exc:
            if isinstance(exc, KeyboardInterrupt):
                raise
            # Only retry on known transient errors
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
            collect_metrics=True,
            measure_memory=measurement == "time_and_rss",
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
            collect_metrics=True,
            measure_memory=measurement == "time_and_rss",
        )

    # Collect results
    durations = []
    max_rss_mb = 0
    failed_jobs = 0

    for job in pilot_jobs:
        try:
            result = job.result()
            if isinstance(result, dict) and "_paracore_metrics" in result:
                metrics = result["_paracore_metrics"]
                durations.append(metrics.get("duration_s", 0))
                max_rss_mb = max(max_rss_mb, metrics.get("max_rss_mb", 0))
        except (subprocess.SubprocessError, RuntimeError, OSError):
            # Track pilot job failure but continue with remaining results
            failed_jobs += 1
            # In production, consider logging: logger.warning(f"Pilot job {job.job_id} failed: {e}")
            continue

    if not durations:
        # All pilot jobs failed - return guesses with warning flag
        recommendations = {
            "time_min": time_min_guess,
            "mem_gb": mem_gb_guess,
            "cpus_per_task": cpus_per_task_guess,
            "array_parallelism": config.get_cluster_config()
            .get("slurm", {})
            .get("max_array_parallelism", 100),
            "_warning": f"All {len(pilot_jobs)} pilot jobs failed. Using default guesses.",
        }
        return recommendations

    # Compute recommendations
    # Calculate p95 with bounds checking
    sorted_durations = sorted(durations)
    p95_index = min(int(len(sorted_durations) * 0.95), len(sorted_durations) - 1)
    p95_duration = sorted_durations[p95_index] if sorted_durations else time_min_guess * 60

    recommendations = {
        "time_min": math.ceil(p95_duration * 1.3 / 60),
        "mem_gb": math.ceil(max_rss_mb * 1.3 / 1024) if max_rss_mb > 0 else mem_gb_guess,
        "cpus_per_task": cpus_per_task_guess,
        "array_parallelism": config.get_cluster_config()
        .get("slurm", {})
        .get("max_array_parallelism", 100),
    }

    if failed_jobs > 0:
        recommendations["_info"] = (
            f"Based on {len(durations)}/{len(pilot_jobs)} successful pilot jobs."
        )

    return recommendations
