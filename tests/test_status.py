"""Tests for job status helpers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from paracore.status import JobStatus, get_job_status


class DummyJob:
    """Minimal stand-in for SlurmJob used in tests."""

    def __init__(
        self,
        *,
        job_id: str,
        stdout: Path,
        stderr: Path,
        result_pickle: Path,
        state_value: str | None = None,
        state_exc: Exception | None = None,
        info: dict[str, str] | None = None,
        info_exc: Exception | None = None,
    ) -> None:
        self.job_id = job_id
        self._state_value = state_value
        self._state_exc = state_exc
        self._info = info or {}
        self._info_exc = info_exc
        self.paths = SimpleNamespace(
            stdout=stdout,
            stderr=stderr,
            result_pickle=result_pickle,
        )

    @property
    def state(self) -> str:
        if self._state_exc:
            raise self._state_exc
        return self._state_value or "UNKNOWN"

    def get_info(self, mode: str = "cache") -> dict[str, str]:
        if self._info_exc:
            raise self._info_exc
        return self._info


@pytest.fixture
def tmp_paths(tmp_path: Path) -> dict[str, Path]:
    """Create reusable stdout/stderr/result paths."""
    stdout = tmp_path / "123.log.out"
    stderr = tmp_path / "123.log.err"
    result = tmp_path / "123.pkl"
    return {"stdout": stdout, "stderr": stderr, "result": result}


def test_get_job_status_with_slurm_data(monkeypatch: pytest.MonkeyPatch, tmp_paths: dict[str, Path]):
    """State and info should be surfaced directly when Slurm responds."""
    tmp_paths["stdout"].touch()
    tmp_paths["stderr"].touch()

    dummy_job = DummyJob(
        job_id="123",
        stdout=tmp_paths["stdout"],
        stderr=tmp_paths["stderr"],
        result_pickle=tmp_paths["result"],
        state_value="RUNNING",
        info={"State": "RUNNING", "Partition": "gpu"},
    )

    monkeypatch.setattr("paracore.status._load_job", lambda job_id, log_dir: dummy_job)

    status = get_job_status("123", log_dir=tmp_paths["stdout"].parent)

    assert isinstance(status, JobStatus)
    assert status.job_id == "123"
    assert status.state == "RUNNING"
    assert status.info["State"] == "RUNNING"
    assert status.stdout_path == tmp_paths["stdout"]
    assert status.stderr_path == tmp_paths["stderr"]
    assert status.note is None


def test_get_job_status_uses_filesystem_fallback(monkeypatch: pytest.MonkeyPatch, tmp_paths: dict[str, Path]):
    """Fallback to heuristics when Slurm is unavailable."""
    tmp_paths["result"].touch()

    dummy_job = DummyJob(
        job_id="999",
        stdout=tmp_paths["stdout"],
        stderr=tmp_paths["stderr"],
        result_pickle=tmp_paths["result"],
        state_exc=RuntimeError("sacct missing"),
        info_exc=RuntimeError("no sacct"),
    )

    monkeypatch.setattr("paracore.status._load_job", lambda job_id, log_dir: dummy_job)

    status = get_job_status("999", log_dir=tmp_paths["stdout"].parent)

    assert status.state == "COMPLETED"
    assert status.stdout_path is None  # file does not exist
    assert status.stderr_path is None
    assert status.note is not None
    assert "sacct missing" in status.note


def test_get_job_status_marks_running_when_logs_exist(monkeypatch: pytest.MonkeyPatch, tmp_paths: dict[str, Path]):
    """Presence of log files should infer RUNNING if state is unknown."""
    tmp_paths["stdout"].touch()

    dummy_job = DummyJob(
        job_id="555",
        stdout=tmp_paths["stdout"],
        stderr=tmp_paths["stderr"],
        result_pickle=tmp_paths["result"],
        state_value="UNKNOWN",
        info={},
    )

    monkeypatch.setattr("paracore.status._load_job", lambda job_id, log_dir: dummy_job)

    status = get_job_status("555", log_dir=tmp_paths["stdout"].parent)

    assert status.state == "RUNNING"
    assert status.stdout_path == tmp_paths["stdout"]
    assert status.stderr_path is None
