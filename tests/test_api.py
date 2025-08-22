"""Tests for public API."""

import time
from unittest.mock import Mock, patch

import pytest

from paracore import SubmitHandle, autotune_from_pilot, map_cmds, map_func, run_cmd


@patch("paracore.api.SubmititBackend")
def test_run_cmd(mock_backend_class):
    """Test run_cmd function."""
    # Setup mock
    mock_backend = Mock()
    mock_backend_class.return_value = mock_backend

    mock_handle = SubmitHandle(
        job_id="12345",
        job_name="test-job",
        stdout_path="/logs/stdout",
        stderr_path="/logs/stderr",
    )
    mock_backend.submit_cmd.return_value = mock_handle

    # Call function
    result = run_cmd(
        "echo hello",
        job_name="test-job",
        partition="short",
        time_min=30,
        cpus_per_task=2,
        mem_gb=8,
    )

    # Verify
    assert result.job_id == "12345"
    assert result.job_name == "test-job"

    mock_backend.submit_cmd.assert_called_once()
    call_kwargs = mock_backend.submit_cmd.call_args.kwargs
    assert call_kwargs["cmd"] == "echo hello"
    assert call_kwargs["partition"] == "short"
    assert call_kwargs["time_min"] == 30


@patch("paracore.api.SubmititBackend")
def test_map_cmds(mock_backend_class):
    """Test map_cmds function."""
    # Setup mock
    mock_backend = Mock()
    mock_backend_class.return_value = mock_backend

    mock_handles = [
        SubmitHandle(job_id=f"1234{i}", job_name="array-job", array_index=i) for i in range(3)
    ]
    mock_backend.submit_cmd_array.return_value = mock_handles

    # Call function
    cmds = ["echo 1", "echo 2", "echo 3"]
    results = map_cmds(
        cmds,
        job_name="array-job",
        partition="compute",
        array_parallelism=10,
    )

    # Verify
    assert len(results) == 3
    assert results[0].job_id == "12340"
    assert results[1].array_index == 1

    mock_backend.submit_cmd_array.assert_called_once()
    call_kwargs = mock_backend.submit_cmd_array.call_args.kwargs
    assert call_kwargs["cmds"] == cmds
    assert call_kwargs["array_parallelism"] == 10


@patch("paracore.api.SubmititBackend")
def test_map_func(mock_backend_class):
    """Test map_func function."""
    # Setup mock
    mock_backend = Mock()
    mock_backend_class.return_value = mock_backend

    mock_handles = [
        SubmitHandle(job_id=f"2345{i}", job_name="func-job", array_index=i) for i in range(5)
    ]
    mock_backend.submit_func_array.return_value = mock_handles

    # Define test function
    def process_item(x):
        return x * 2

    # Call function
    items = [1, 2, 3, 4, 5]
    results = map_func(
        process_item,
        items,
        job_name="func-job",
        partition="compute",
    )

    # Verify
    assert len(results) == 5
    assert all(h.job_name == "func-job" for h in results)

    mock_backend.submit_func_array.assert_called_once()
    call_kwargs = mock_backend.submit_func_array.call_args.kwargs
    assert call_kwargs["fn"] == process_item
    assert call_kwargs["items"] == items


@patch("paracore.api.SubmititBackend")
def test_retry_logic(mock_backend_class):
    """Test retry logic on failure."""
    # Setup mock
    mock_backend = Mock()
    mock_backend_class.return_value = mock_backend

    # First two calls fail, third succeeds
    mock_backend.submit_cmd.side_effect = [
        Exception("Network error"),
        Exception("Timeout"),
        SubmitHandle(job_id="99999", job_name="retry-job"),
    ]

    # Call with retries
    start_time = time.time()
    result = run_cmd(
        "echo retry",
        retries=2,
        retry_backoff_s=0.1,  # Short backoff for testing
    )
    elapsed = time.time() - start_time

    # Verify
    assert result.job_id == "99999"
    assert mock_backend.submit_cmd.call_count == 3
    assert elapsed >= 0.3  # At least 0.1 + 0.2 backoff


@patch("paracore.api.SubmititBackend")
def test_autotune_from_pilot(mock_backend_class):
    """Test autotune_from_pilot function."""
    # Setup mock
    mock_backend = Mock()
    mock_backend_class.return_value = mock_backend

    # Mock pilot job results
    mock_jobs = []
    for i in range(5):
        job = Mock()
        job.result.return_value = {
            "_paracore_metrics": {
                "duration_s": 45 + i * 5,  # 45, 50, 55, 60, 65
                "max_rss_mb": 1000 + i * 100,  # 1000-1400 MB
            }
        }
        mock_jobs.append(job)

    mock_backend.submit_cmd_array.return_value = mock_jobs

    # Call function
    sample_cmds = [f"cmd {i}" for i in range(10)]
    suggestions = autotune_from_pilot(
        sample_cmds,
        runner="cmds",
        sample_size=5,
        measurement="time_and_rss",
        partition="compute",
    )

    # Verify recommendations
    # p95 of [45, 50, 55, 60, 65] = 65, * 1.3 / 60 = ~1.4 = 2 min
    assert suggestions["time_min"] >= 1
    # max RSS = 1400 MB * 1.3 / 1024 = ~1.8 = 2 GB
    assert suggestions["mem_gb"] >= 1
    assert "cpus_per_task" in suggestions
    assert "array_parallelism" in suggestions


def test_submit_handle_methods():
    """Test SubmitHandle methods."""
    mock_job = Mock()
    mock_job.result.return_value = "test result"
    mock_job.done.return_value = True

    handle = SubmitHandle(
        job_id="123",
        job_name="test",
        _backend_job=mock_job,
    )

    # Test result
    assert handle.result() == "test result"
    mock_job.result.assert_called_once_with(timeout=None)

    # Test done
    assert handle.done() is True
    mock_job.done.assert_called_once()

    # Test cancel
    handle.cancel()
    mock_job.cancel.assert_called_once()


def test_submit_handle_no_backend():
    """Test SubmitHandle without backend job raises errors."""
    handle = SubmitHandle(job_id="123", job_name="test")

    with pytest.raises(RuntimeError, match="No backend job"):
        handle.result()

    with pytest.raises(RuntimeError, match="No backend job"):
        handle.done()

    with pytest.raises(RuntimeError, match="No backend job"):
        handle.cancel()
