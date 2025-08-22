"""Submitit backend implementation."""

from __future__ import annotations

import os
import subprocess
import time
from typing import Any, Callable, List, Literal, Mapping, Optional

import submitit

from paracore.config import Config
from paracore.types import SubmitHandle


class SubmititBackend:
    """Thin wrapper around Submitit for job submission."""

    def __init__(self, config: Config):
        self.config = config

    def _setup_executor(
        self,
        job_name: str,
        partition: Optional[str] = None,
        time_min: Optional[int] = None,
        cpus_per_task: Optional[int] = None,
        mem_gb: Optional[int] = None,
        account: Optional[str] = None,
        qos: Optional[str] = None,
        array_parallelism: Optional[int] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> submitit.AutoExecutor:
        """Configure a Submitit executor with our parameters."""
        # Resolve configuration
        resolved = self.config.resolve(
            partition=partition,
            time_min=time_min,
            cpus_per_task=cpus_per_task,
            mem_gb=mem_gb,
            account=account,
            qos=qos,
            array_parallelism=array_parallelism,
            extra=extra,
        )

        # Create executor
        executor = submitit.AutoExecutor(folder="submitit_logs")

        # Update parameters
        slurm_params = {
            "job_name": job_name,
            "partition": resolved["partition"],
            "time": resolved["time_min"],
            "cpus_per_task": resolved["cpus_per_task"],
            "mem_gb": resolved["mem_gb"],
        }

        if resolved.get("account"):
            slurm_params["account"] = resolved["account"]

        if resolved.get("qos"):
            slurm_params["qos"] = resolved["qos"]

        # Handle array parallelism
        if array_parallelism is not None:
            slurm_params["array_parallelism"] = array_parallelism

        # Add extra parameters
        if extra:
            slurm_params["slurm_additional_parameters"] = extra

        executor.update_parameters(**slurm_params)

        return executor

    def _prepare_env_wrapper(
        self,
        env_setup: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        env_merge: Literal["inherit", "replace"] = "inherit",
    ) -> Callable:
        """Create a wrapper that sets up the environment before running the task."""

        def env_wrapper(task_fn: Callable) -> Callable:
            def wrapped(*args, **kwargs):
                # Handle environment
                if env_merge == "replace" and env:
                    # Preserve essential system and Slurm variables
                    essential_vars = {
                        "PATH",
                        "HOME",
                        "USER",
                        "LOGNAME",
                        "SHELL",
                        "TERM",
                        "LD_LIBRARY_PATH",
                        "PYTHONPATH",
                        "TMPDIR",
                        "TEMP",
                        "TMP",
                        "LANG",
                        "LC_ALL",
                        "LC_CTYPE",
                        "TZ",
                        "HOSTNAME",
                    }
                    preserved = {}
                    for key, value in os.environ.items():
                        # Preserve essential vars and all Slurm-related variables
                        if key in essential_vars or key.startswith("SLURM_"):
                            preserved[key] = value

                    # Clear and restore with preserved + user env
                    os.environ.clear()
                    os.environ.update(preserved)
                    os.environ.update(env)
                elif env_merge == "inherit" and env:
                    # Merge with current environment
                    os.environ.update(env)

                # Run env_setup command if provided
                if env_setup:
                    setup_result = subprocess.run(
                        env_setup,
                        shell=True,
                        capture_output=True,
                        text=True,
                    )
                    if setup_result.returncode != 0:
                        raise RuntimeError(f"env_setup failed: {setup_result.stderr}")

                # Run the actual task
                return task_fn(*args, **kwargs)

            return wrapped

        return env_wrapper

    def submit_cmd(
        self,
        cmd: str,
        job_name: Optional[str] = None,
        partition: Optional[str] = None,
        time_min: Optional[int] = None,
        cpus_per_task: Optional[int] = None,
        mem_gb: Optional[int] = None,
        env_setup: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        env_merge: Literal["inherit", "replace"] = "inherit",
        account: Optional[str] = None,
        qos: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> SubmitHandle:
        """Submit a single command."""
        # Format job name
        if job_name is None:
            job_name = self.config.format_job_name(
                None,
                partition=partition,
                env=env_setup,
            )

        # Setup executor
        executor = self._setup_executor(
            job_name=job_name,
            partition=partition,
            time_min=time_min,
            cpus_per_task=cpus_per_task,
            mem_gb=mem_gb,
            account=account,
            qos=qos,
            extra=extra,
        )

        # Create command runner
        env_wrapper = self._prepare_env_wrapper(env_setup, env, env_merge)

        def run_command():
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise RuntimeError(f"Command failed: {result.stderr}")
            return result.stdout

        wrapped_fn = env_wrapper(run_command)

        # Submit job
        job = executor.submit(wrapped_fn)

        return SubmitHandle(
            job_id=job.job_id,
            job_name=job_name,
            stdout_path=str(job.paths.stdout),
            stderr_path=str(job.paths.stderr),
            _backend_job=job,
        )

    def submit_cmd_array(
        self,
        cmds: List[str],
        job_name: Optional[str] = None,
        partition: Optional[str] = None,
        time_min: Optional[int] = None,
        cpus_per_task: Optional[int] = None,
        mem_gb: Optional[int] = None,
        env_setup: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        env_merge: Literal["inherit", "replace"] = "inherit",
        array_parallelism: Optional[int] = None,
        account: Optional[str] = None,
        qos: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
        measure_resources: bool = False,
    ) -> List[SubmitHandle]:
        """Submit an array of commands."""
        # Format job name
        if job_name is None:
            job_name = self.config.format_job_name(
                None,
                partition=partition,
                env=env_setup,
            )

        # Setup executor
        executor = self._setup_executor(
            job_name=job_name,
            partition=partition,
            time_min=time_min,
            cpus_per_task=cpus_per_task,
            mem_gb=mem_gb,
            account=account,
            qos=qos,
            array_parallelism=array_parallelism,
            extra=extra,
        )

        # Create command runners
        env_wrapper = self._prepare_env_wrapper(env_setup, env, env_merge)

        def make_runner(cmd: str):
            def run_command():
                start_time = time.time()

                if measure_resources:
                    # Use /usr/bin/time to measure resources
                    wrapped_cmd = f"/usr/bin/time -v {cmd}"
                    result = subprocess.run(
                        wrapped_cmd,
                        shell=True,
                        capture_output=True,
                        text=True,
                    )

                    # Parse time output
                    max_rss_mb = 0
                    for line in result.stderr.split("\n"):
                        if "Maximum resident set size" in line:
                            try:
                                max_rss_kb = int(line.split()[-1])
                                max_rss_mb = max_rss_kb / 1024
                            except (ValueError, IndexError):
                                pass

                    duration_s = time.time() - start_time

                    if result.returncode != 0:
                        raise RuntimeError(f"Command failed: {result.stderr}")

                    return {
                        "_paracore_metrics": {
                            "duration_s": duration_s,
                            "max_rss_mb": max_rss_mb,
                        },
                        "output": result.stdout,
                    }
                else:
                    result = subprocess.run(
                        cmd,
                        shell=True,
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode != 0:
                        raise RuntimeError(f"Command failed: {result.stderr}")
                    return result.stdout

            return env_wrapper(run_command)

        # Submit array job
        runners = [make_runner(cmd) for cmd in cmds]
        jobs = executor.map_array(lambda i: runners[i](), range(len(cmds)))

        # Create handles
        handles = []
        for i, job in enumerate(jobs):
            handles.append(
                SubmitHandle(
                    job_id=job.job_id,
                    job_name=job_name,
                    array_index=i,
                    stdout_path=str(job.paths.stdout),
                    stderr_path=str(job.paths.stderr),
                    _backend_job=job,
                )
            )

        return handles

    def submit_func_array(
        self,
        fn: Callable[[Any], Any],
        items: List[Any],
        job_name: Optional[str] = None,
        partition: Optional[str] = None,
        time_min: Optional[int] = None,
        cpus_per_task: Optional[int] = None,
        mem_gb: Optional[int] = None,
        env_setup: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
        env_merge: Literal["inherit", "replace"] = "inherit",
        array_parallelism: Optional[int] = None,
        account: Optional[str] = None,
        qos: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
        measure_resources: bool = False,
    ) -> List[SubmitHandle]:
        """Submit a function mapped over items."""
        # Format job name
        if job_name is None:
            job_name = self.config.format_job_name(
                None,
                partition=partition,
                env=env_setup,
            )

        # Setup executor
        executor = self._setup_executor(
            job_name=job_name,
            partition=partition,
            time_min=time_min,
            cpus_per_task=cpus_per_task,
            mem_gb=mem_gb,
            account=account,
            qos=qos,
            array_parallelism=array_parallelism,
            extra=extra,
        )

        # Create wrapped function
        env_wrapper = self._prepare_env_wrapper(env_setup, env, env_merge)

        if measure_resources:

            def wrapped_fn(item):
                start_time = time.time()
                result = fn(item)
                duration_s = time.time() - start_time

                # Try to get memory usage (simplified for Python functions)
                import resource

                max_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

                return {
                    "_paracore_metrics": {
                        "duration_s": duration_s,
                        "max_rss_mb": max_rss_mb,
                    },
                    "result": result,
                }
        else:
            wrapped_fn = fn

        final_fn = env_wrapper(wrapped_fn)

        # Submit array job
        jobs = executor.map_array(final_fn, items)

        # Create handles
        handles = []
        for i, job in enumerate(jobs):
            handles.append(
                SubmitHandle(
                    job_id=job.job_id,
                    job_name=job_name,
                    array_index=i,
                    stdout_path=str(job.paths.stdout),
                    stderr_path=str(job.paths.stderr),
                    _backend_job=job,
                )
            )

        return handles
