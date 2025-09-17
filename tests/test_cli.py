"""CLI-level tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from paracore.cli import cmd_status
from paracore.status import JobStatus


def test_cmd_status_prints_status(monkeypatch, capsys, tmp_path):
    """cmd_status should print a readable status summary."""
    statuses = {
        "123": JobStatus(
            job_id="123",
            state="COMPLETED",
            info={"State": "COMPLETED", "Elapsed": "00:10:00"},
            stdout_path=tmp_path / "out.log",
            stderr_path=None,
            note=None,
        ),
        "456": JobStatus(
            job_id="456",
            state="RUNNING",
            info={},
            stdout_path=None,
            stderr_path=None,
            note="Slurm state unavailable: sacct missing",
        ),
    }

    monkeypatch.setattr(
        "paracore.cli.get_job_status", lambda job_id, log_dir, refresh=False: statuses[job_id]
    )

    args = SimpleNamespace(job_ids=["123", "456"], log_dir=str(tmp_path), refresh=False)
    cmd_status(args)

    out = capsys.readouterr().out
    assert "123: COMPLETED" in out
    assert "sacct: State=COMPLETED" in out
    assert str(tmp_path / "out.log") in out
    assert "456: RUNNING" in out
    assert "note:" in out


def test_cmd_status_requires_job_ids(capsys):
    """cmd_status should exit with code 1 when no job ids are provided."""
    args = SimpleNamespace(job_ids=[], log_dir="submitit_logs", refresh=False)

    with pytest.raises(SystemExit) as exc:
        cmd_status(args)

    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "Provide at least one job id" in err
