"""Dask helpers for paracore (placeholder for v1.1)."""

from typing import Any, Dict, Optional


def start_dask_slurm(**kwargs) -> Dict[str, Any]:
    """Start a Dask cluster on Slurm (to be implemented in v1.1)."""
    raise NotImplementedError("Dask helpers will be available in v1.1")


def attach_dask(scheduler_address: str) -> Any:
    """Attach to an existing Dask cluster (to be implemented in v1.1)."""
    raise NotImplementedError("Dask helpers will be available in v1.1")