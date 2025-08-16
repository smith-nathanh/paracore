"""Paracore - A thin wrapper over Slurm via Submitit for compute workloads."""

from paracore.api import (
    SubmitHandle,
    run_cmd,
    map_cmds,
    map_func,
    autotune_from_pilot,
)

__version__ = "1.0.0"
__all__ = [
    "SubmitHandle",
    "run_cmd",
    "map_cmds",
    "map_func",
    "autotune_from_pilot",
]