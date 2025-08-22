"""Common types for paracore."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


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
