"""Tests for Submitit backend."""

from unittest.mock import Mock, patch

import pytest

from paracore.config import Config
from paracore.submitit_backend import SubmititBackend


@pytest.fixture
def mock_config():
    """Create a mock config."""
    config = Mock(spec=Config)
    config.resolve.return_value = {
        "cluster": "test",
        "partition": "compute",
        "time_min": 60,
        "cpus_per_task": 4,
        "mem_gb": 16,
        "account": None,
        "qos": None,
        "max_array_parallelism": 100,
        "extra": {},
    }
    config.format_job_name.return_value = "test-job"
    return config


@patch("paracore.submitit_backend.submitit.AutoExecutor")
def test_submit_cmd(mock_executor_class, mock_config):
    """Test submitting a single command."""
    # Setup mock executor
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor

    mock_job = Mock()
    mock_job.job_id = "12345"
    mock_job.paths.stdout = "/logs/stdout"
    mock_job.paths.stderr = "/logs/stderr"
    mock_executor.submit.return_value = mock_job

    # Setup mock subprocess
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = Mock(returncode=0, stdout="output", stderr="")

        # Submit command
        backend = SubmititBackend(mock_config)
        handle = backend.submit_cmd(
            cmd="echo hello",
            partition="compute",
            time_min=30,
        )

    # Verify
    assert handle.job_id == "12345"
    assert handle.job_name == "test-job"
    assert handle.stdout_path == "/logs/stdout"

    # Check executor configuration
    mock_executor.update_parameters.assert_called_once()
    params = mock_executor.update_parameters.call_args.kwargs
    assert params["job_name"] == "test-job"
    assert params["partition"] == "compute"
    assert params["time"] == 60  # from resolved config


@patch("paracore.submitit_backend.submitit.AutoExecutor")
def test_submit_cmd_array(mock_executor_class, mock_config):
    """Test submitting an array of commands."""
    # Setup mock executor
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor

    mock_jobs = []
    for i in range(3):
        job = Mock()
        job.job_id = f"1234{i}"
        job.paths.stdout = f"/logs/stdout.{i}"
        job.paths.stderr = f"/logs/stderr.{i}"
        mock_jobs.append(job)

    mock_executor.map_array.return_value = mock_jobs

    # Submit array
    backend = SubmititBackend(mock_config)
    handles = backend.submit_cmd_array(
        cmds=["echo 1", "echo 2", "echo 3"],
        array_parallelism=10,
    )

    # Verify
    assert len(handles) == 3
    assert handles[0].job_id == "12340"
    assert handles[1].array_index == 1
    assert handles[2].stdout_path == "/logs/stdout.2"

    # Check array parallelism
    params = mock_executor.update_parameters.call_args.kwargs
    assert params.get("array_parallelism") == 10


def test_env_wrapper(mock_config):
    """Test environment wrapper functionality."""
    backend = SubmititBackend(mock_config)

    # Test inherit mode
    with patch.dict("os.environ", {"EXISTING": "value"}):
        wrapper = backend._prepare_env_wrapper(
            env={"NEW": "added"},
            env_merge="inherit",
        )

        def test_fn():
            import os

            return os.environ.get("EXISTING"), os.environ.get("NEW")

        wrapped = wrapper(test_fn)
        existing, new = wrapped()

        assert existing == "value"
        assert new == "added"

    # Test replace mode
    wrapper = backend._prepare_env_wrapper(
        env={"ONLY": "this"},
        env_merge="replace",
    )

    def test_fn2():
        import os

        return len(os.environ), os.environ.get("ONLY")

    wrapped2 = wrapper(test_fn2)
    with patch.dict("os.environ", {"EXISTING": "value", "OTHER": "stuff"}):
        env_len, only = wrapped2()

        assert env_len == 1
        assert only == "this"


@patch("subprocess.run")
def test_env_setup(mock_run, mock_config):
    """Test env_setup command execution."""
    backend = SubmititBackend(mock_config)

    # Success case
    mock_run.return_value = Mock(returncode=0, stdout="", stderr="")

    wrapper = backend._prepare_env_wrapper(env_setup="source activate prod")

    def test_fn():
        return "result"

    wrapped = wrapper(test_fn)
    result = wrapped()

    assert result == "result"
    mock_run.assert_called_once_with(
        "source activate prod",
        shell=True,
        capture_output=True,
        text=True,
    )

    # Failure case
    mock_run.return_value = Mock(returncode=1, stdout="", stderr="Setup failed")

    with pytest.raises(RuntimeError, match="env_setup failed"):
        wrapped()


@patch("paracore.submitit_backend.submitit.AutoExecutor")
@patch("subprocess.run")
def test_resource_measurement(mock_run, mock_executor_class, mock_config):
    """Test resource measurement in pilot mode."""
    # Setup mock executor
    mock_executor = Mock()
    mock_executor_class.return_value = mock_executor

    mock_job = Mock()
    mock_job.job_id = "99999"
    mock_job.paths.stdout = "/logs/stdout"
    mock_job.paths.stderr = "/logs/stderr"
    mock_executor.map_array.return_value = [mock_job]

    # Mock time command output
    time_output = """
Command exited with non-zero status 0
    User time (seconds): 10.50
    System time (seconds): 2.30
    Maximum resident set size (kbytes): 2048000
    """

    mock_run.return_value = Mock(
        returncode=0,
        stdout="command output",
        stderr=time_output,
    )

    # Submit with measurement
    backend = SubmititBackend(mock_config)
    handles = backend.submit_cmd_array(
        cmds=["python script.py"],
        measure_resources=True,
    )

    # Get the submitted function and call it
    map_array_call = mock_executor.map_array.call_args
    runner_fn = map_array_call[0][0]
    result = runner_fn(0)

    # Verify metrics
    assert "_paracore_metrics" in result
    metrics = result["_paracore_metrics"]
    assert metrics["max_rss_mb"] == 2000  # 2048000 KB / 1024
    assert "duration_s" in metrics
    assert result["output"] == "command output"
