"""Example: Submit a single job that processes a dataset."""

from __future__ import annotations

import json
from pathlib import Path

from paracore import run_cmd

EXAMPLES_ROOT = Path(__file__).resolve().parent
CONFIG = EXAMPLES_ROOT / "inputs" / "config_0001.json"
OUTPUT = EXAMPLES_ROOT / "results" / "output_0001.json"


def main() -> None:
    """Submit one job and display the processed metric."""

    if not CONFIG.exists():
        raise SystemExit(
            "Config not found. Run `python examples/generate_test_data.py` first."
        )

    command = (
        f"python examples/process_data.py {CONFIG.as_posix()} {OUTPUT.as_posix()} --mode heavy"
    )

    job = run_cmd(
        command,
        partition="compute",
        time_min=60,
        cpus_per_task=4,
        mem_gb=16,
        env={"APP_MODE": "production"},
        job_name="single-dataset-run",
        retries=1,
        retry_backoff_s=60,
    )

    print(f"Submitted job {job.job_id} ({job.job_name})")
    print(f"Stdout log: {job.stdout_path}")

    try:
        job.result(timeout=3600)
    except Exception as exc:  # noqa: BLE001 - surface the failure reason
        print(f"Job failed: {exc}")
        return

    print("Job completed successfully")
    with OUTPUT.open() as f:
        report = json.load(f)

    print(
        "Scaled metric",
        report["scaled_metric"],
        "from",
        report["rows_retained"],
        "filtered rows",
    )


if __name__ == "__main__":
    main()
