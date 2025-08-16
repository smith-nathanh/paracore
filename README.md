# Paracore

A thin, opinionated wrapper over Slurm via Submitit that standardizes job submission for compute workloads.

## Installation

```bash
uv pip install -e .
```

## Quick Start

### Python API

```python
from paracore import run_cmd, map_cmds

# Single job
job = run_cmd("python myscript.py", partition="compute", time_min=60)
result = job.result()

# Array job
cmds = [f"python process.py --id {i}" for i in range(100)]
jobs = map_cmds(cmds, partition="compute", array_parallelism=20)
```

### Command Line Interface

```bash
# Submit a single job
paracore run "python script.py" --time 60 --memory 16 --cpus 4

# Submit batch jobs from file
paracore batch commands.txt --array-parallelism 20 --wait

# Run pilot for autotuning
paracore autotune commands.txt --sample-size 10 --measure-memory

# Pipe commands
cat commands.txt | paracore batch - --partition gpu --time 120

# Save autotune recommendations
paracore autotune commands.txt -o resources.json --export-shell
```

## CLI Commands

- **`run`** - Submit a single job
- **`batch`** - Submit array job from file or stdin
- **`autotune`** - Run pilot jobs to determine optimal resources
- **`status`** - Check job status (placeholder)

## Examples

See `examples/` for complete Python examples and `examples/commands.txt` for CLI batch examples.