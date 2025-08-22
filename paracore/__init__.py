"""Paracore - A thin wrapper over Slurm via Submitit for compute workloads."""

from paracore.api import (
    autotune_from_pilot,
    map_cmds,
    map_func,
    run_cmd,
)
from paracore.types import SubmitHandle

__version__ = "1.0.0"
__all__ = [
    "SubmitHandle",
    "autotune_from_pilot",
    "map_cmds",
    "map_func",
    "run_cmd",
]
