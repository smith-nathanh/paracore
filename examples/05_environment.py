"""Example: Different environment handling strategies."""

from paracore import run_cmd, map_cmds

# Example 1: Inherit parent environment (default)
job1 = run_cmd(
    "echo $PATH && python check_env.py",
    job_name="env-inherit",
    partition="short",
)

# Example 2: Add environment variables to inherited env
job2 = run_cmd(
    "echo $APP_MODE && echo $DEBUG_LEVEL",
    env={"APP_MODE": "production", "DEBUG_LEVEL": "info"},
    env_merge="inherit",  # This is the default
    job_name="env-overlay",
    partition="short",
)

# Example 3: Replace entire environment
job3 = run_cmd(
    "env | sort",  # Show all environment variables
    env={
        "PATH": "/usr/bin:/bin",
        "HOME": "/home/user",
        "APP_MODE": "minimal",
    },
    env_merge="replace",
    job_name="env-replace",
    partition="short",
)

# Example 4: Use env_setup command
job4 = run_cmd(
    "python examples/process_data.py inputs/config_0000.json",
    env_setup="source /opt/envs/prod/bin/activate",
    env={"ANALYSIS_TYPE": "full"},
    job_name="env-setup",
    partition="compute",
)

# Example 5: Array job with consistent environment
cmds = [f"python examples/process_data.py inputs/config_{i:04d}.json --mode quick" for i in range(10)]
jobs = map_cmds(
    cmds,
    env={"PROCESSING_MODE": "parallel", "LOG_LEVEL": "warning"},
    partition="compute",
    array_parallelism=5,
    job_name="env-array",
)

print(f"Submitted environment test jobs:")
print(f"  Inherit: {job1.job_id}")
print(f"  Overlay: {job2.job_id}")
print(f"  Replace: {job3.job_id}")
print(f"  Setup: {job4.job_id}")
print(f"  Array: {len(jobs)} jobs")