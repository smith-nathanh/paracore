"""Example: Submit a single job."""

from paracore import run_cmd

# Submit a single command
job = run_cmd(
    "python examples/process_data.py inputs/config_0001.json results/output_0001.json",
    partition="compute",
    time_min=60,
    cpus_per_task=4,
    mem_gb=16,
    env={"APP_MODE": "production"},
    job_name="data-processing",
    retries=1,
    retry_backoff_s=60,
)

print(f"Submitted job {job.job_id} ({job.job_name})")
print(f"Logs will be at: {job.stdout_path}")

# Wait for result
try:
    result = job.result(timeout=3600)  # Wait up to 1 hour
    print("Job completed successfully")
    print(f"Output: {result}")
except Exception as e:
    print(f"Job failed: {e}")
