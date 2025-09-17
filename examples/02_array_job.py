"""Example: Submit an array job with throttling and aggregate the outputs."""

from __future__ import annotations

import json
from pathlib import Path

from paracore import map_cmds

EXAMPLES_ROOT = Path(__file__).resolve().parent
CONFIG_ROOT = EXAMPLES_ROOT / "inputs"
RESULT_ROOT = EXAMPLES_ROOT / "results" / "batch"


def main() -> None:
    """Submit a throttled batch and collect summary statistics."""

    if not CONFIG_ROOT.exists():
        raise SystemExit(
            "Config directory missing. Run `python examples/generate_test_data.py` first."
        )

    config_paths = sorted(CONFIG_ROOT.glob("config_*.json"))[:40]
    RESULT_ROOT.mkdir(parents=True, exist_ok=True)

    cmds = []
    for cfg in config_paths:
        output = RESULT_ROOT / f"{cfg.stem}_metrics.json"
        cmds.append(
            " ".join(
                [
                    "python",
                    "examples/process_data.py",
                    cfg.as_posix(),
                    output.as_posix(),
                    "--mode",
                    "simple",
                ]
            )
        )

    jobs = map_cmds(
        cmds,
        partition="compute",
        time_min=90,
        cpus_per_task=4,
        mem_gb=12,
        array_parallelism=15,
        job_name="batch-metrics",
        retries=1,
    )

    print(f"Submitted {len(jobs)} jobs (first job ID: {jobs[0].job_id})")

    successes = 0
    failures: list[tuple[int, str, str]] = []

    for idx, job in enumerate(jobs, start=1):
        try:
            job.result()
            successes += 1
            if idx % 10 == 0:
                print(f"Completed {idx}/{len(jobs)} jobs")
        except Exception as exc:  # noqa: BLE001 - collect for reporting
            failures.append((idx, job.job_id, str(exc)))

    print(f"Batch complete: {successes} succeeded, {len(failures)} failed")
    if failures:
        for idx, job_id, error in failures[:5]:
            print(f"  Job {idx} ({job_id}) failed: {error}")

    # Aggregate scaled metrics from the produced result files
    scaled_metrics = []
    for output_file in RESULT_ROOT.glob("*_metrics.json"):
        with output_file.open() as f:
            report = json.load(f)
        metric = report.get("scaled_metric")
        if metric is not None:
            scaled_metrics.append(metric)

    if scaled_metrics:
        average_metric = sum(scaled_metrics) / len(scaled_metrics)
        print(
            f"Average scaled metric across {len(scaled_metrics)} results: {average_metric:.3f}"
        )


if __name__ == "__main__":
    main()
