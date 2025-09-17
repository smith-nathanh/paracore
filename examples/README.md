# Examples Overview

The scripts in this directory form a miniature analytics pipeline that you can
use to try Paracore end-to-end. The rough workflow is:

1. Generate synthetic configs and datasets: `python examples/generate_test_data.py`
2. Submit jobs with the Python API helpers (`01_single_job.py`, `02_array_job.py`, `03_map_func.py`).
3. Explore CLI flows (`examples/commands.txt`, `paracore autotune`, `paracore status`).

All configs live in `examples/inputs/` and each points at a companion CSV in
`examples/datasets/`. The processing logic reads those files, computes summary
statistics, and writes JSON reports under `examples/results/`.

| Script | Demonstrates |
| --- | --- |
| `01_single_job.py` | Submitting a single workload and reading back the result file. |
| `02_array_job.py` | Throttled array submission plus aggregating outputs. |
| `03_map_func.py` | Function mapping with shared library code. |
| `04_autotune.py` | Using pilot jobs to tune resources before a batch run. |
| `05_environment.py` | Comparing environment handling strategies. |
| `commands.txt` | Ready-to-run CLI batch file to pair with `paracore batch`. |

Feel free to tweak the generated configs or dataset parameters in
`generate_test_data.py` to reflect your cluster’s workloads (row counts, metric
thresholds, etc.).
