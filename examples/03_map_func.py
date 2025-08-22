"""Example: Map a Python function over items."""

import json
import time

from paracore import map_func


def process_config_file(cfg_path: str) -> dict:
    """Load config and process data."""
    with open(cfg_path) as f:
        cfg = json.load(f)

    # Simulate some processing
    time.sleep(0.1)

    # Transform the data
    result_value = cfg.get("value", 0) * 2.5

    return {
        "id": cfg["id"],
        "result": result_value,
        "input_file": cfg_path,
        "status": "processed",
    }


# List of config files to process
cfgs = [f"inputs/config_{i:04d}.json" for i in range(50)]  # Reduced for testing

# Submit function mapped over configs
jobs = map_func(
    fn=process_config_file,
    items=cfgs,
    partition="compute",
    cpus_per_task=4,
    mem_gb=16,
    time_min=60,
    job_name="data-transform",
    retries=1,
)

print(f"Submitted {len(jobs)} processing jobs")

# Collect results
results = []
for job in jobs:
    try:
        result = job.result()
        results.append(result)
    except Exception as e:
        print(f"Job {job.job_id} failed: {e}")

# Process results
total_value = sum(r["result"] for r in results)
print(f"Total processed value across {len(results)} configs: {total_value:,.2f}")
