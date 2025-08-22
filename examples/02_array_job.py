"""Example: Submit an array job with throttling."""

from paracore import map_cmds

# Generate commands for batch processing
cmds = [
    f"python examples/process_data.py inputs/config_{i:04d}.json results/output_{i:04d}.json"
    for i in range(100)  # Reduced for testing
]

# Submit array job with throttling
jobs = map_cmds(
    cmds,
    partition="compute",
    time_min=120,
    cpus_per_task=4,
    mem_gb=16,
    array_parallelism=20,  # Limit concurrent jobs
    job_name="batch-processing",
    retries=1,
)

print(f"Submitted {len(jobs)} jobs")
print(f"First job ID: {jobs[0].job_id}")

# Wait for all results
results = []
failed_jobs = []

for i, job in enumerate(jobs):
    try:
        result = job.result()
        results.append(result)
        if i % 100 == 0:
            print(f"Completed {i+1}/{len(jobs)} jobs")
    except Exception as e:
        failed_jobs.append((i, job.job_id, str(e)))

print(f"Completed {len(results)} jobs successfully")
if failed_jobs:
    print(f"Failed jobs: {len(failed_jobs)}")
    for idx, job_id, error in failed_jobs[:5]:  # Show first 5 failures
        print(f"  Job {idx} ({job_id}): {error}")
