"""Example: Use pilot jobs to autotune resources."""

from paracore import autotune_from_pilot, map_cmds

# Sample of commands for pilot
all_configs = [f"inputs/config_{i:04d}.json" for i in range(100)]
sample_cmds = [
    f"python examples/process_data.py {cfg} results/pilot_{i:04d}.json"
    for i, cfg in enumerate(all_configs[:20])
]

# Run pilot to get resource recommendations
print("Running pilot jobs to determine optimal resources...")
suggestions = autotune_from_pilot(
    sample_cmds_or_items=sample_cmds,
    runner="cmds",
    sample_size=10,
    measurement="time_and_rss",
    partition="compute",
    cpus_per_task_guess=4,
    mem_gb_guess=8,
    time_min_guess=30,
)

print("Pilot recommendations:")
print(f"  Time: {suggestions['time_min']} minutes")
print(f"  Memory: {suggestions['mem_gb']} GB")
print(f"  CPUs: {suggestions['cpus_per_task']}")
print(f"  Array parallelism: {suggestions['array_parallelism']}")

# Run full batch with suggested resources
full_cmds = [
    f"python examples/process_data.py inputs/config_{i:04d}.json results/output_{i:04d}.json"
    for i in range(100)
]

print(f"\nSubmitting {len(full_cmds)} jobs with optimized resources...")
jobs = map_cmds(
    full_cmds,
    partition="compute",
    **suggestions,  # Use all pilot recommendations
    job_name="batch-optimized",
    retries=1,
)

print(f"Submitted {len(jobs)} jobs with autotuned resources")
