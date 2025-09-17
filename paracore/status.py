"""Utilities for inspecting job status."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from submitit.slurm import slurm


@dataclass
class JobStatus:
    """Structured status information for a Slurm job."""

    job_id: str
    state: str
    info: Dict[str, str]
    stdout_path: Optional[Path]
    stderr_path: Optional[Path]
    note: Optional[str] = None


def _infer_task(job_id: str) -> Optional[int]:
    """Infer task id from a job id if it encodes an array index."""
    if "_" not in job_id:
        return None
    base, suffix = job_id.split("_", 1)
    if not base or not suffix:
        return None
    # Some Slurm installations use ``jobid_task``
    try:
        return int(suffix)
    except ValueError:
        return None


def _load_job(job_id: str, log_dir: Path) -> slurm.SlurmJob:
    """Instantiate a Submitit SlurmJob for inspection."""
    task_id = _infer_task(job_id)
    tasks = (task_id,) if task_id is not None else (0,)
    return slurm.SlurmJob(folder=log_dir, job_id=job_id, tasks=tasks)


def get_job_status(
    job_id: str,
    *,
    log_dir: Path,
    refresh: bool = False,
) -> JobStatus:
    """Collect status information for the given job id.

    This function prefers Slurm's sacct/squeue data when available but
    gracefully degrades to filesystem heuristics when those commands are
    unavailable (e.g., during local testing).
    """
    job = _load_job(job_id, log_dir)

    note: Optional[str] = None
    info: Dict[str, str] = {}
    state = "UNKNOWN"

    # Prefer Slurm state when available
    try:
        state = job.state
        if refresh:
            # Force the watcher to refresh from sacct/squeue
            job.get_info(mode="force")
        else:
            # Populate cache without forcing a remote call when possible
            info = job.get_info(mode="cache")
    except Exception as exc:  # pragma: no cover - exercised via fallback tests
        note = f"Slurm state unavailable: {exc}"

    # Fallback heuristics based on job artifacts
    stdout_path = job.paths.stdout if job.paths.stdout.exists() else None
    stderr_path = job.paths.stderr if job.paths.stderr.exists() else None
    result_exists = job.paths.result_pickle.exists()

    if state == "UNKNOWN":
        if result_exists:
            state = "COMPLETED"
        elif stdout_path or stderr_path:
            state = "RUNNING"

    # Force info refresh if we deferred earlier
    if not info:
        try:
            info = job.get_info(mode="force" if refresh else "cache")
        except Exception as exc:  # pragma: no cover - fallback path
            if note is None:
                note = f"Could not read sacct info: {exc}"

    return JobStatus(
        job_id=job_id,
        state=state,
        info=info,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        note=note,
    )


__all__ = ["JobStatus", "get_job_status"]
