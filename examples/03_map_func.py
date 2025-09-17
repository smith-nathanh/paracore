"""Example: Map a Python function over config files using the API helper."""

from __future__ import annotations

import sys
from pathlib import Path

from paracore import map_func

EXAMPLES_ROOT = Path(__file__).resolve().parent
if str(EXAMPLES_ROOT) not in sys.path:
    sys.path.append(str(EXAMPLES_ROOT))

from process_data import process_config  # noqa: E402  (added after sys.path tweak)

OUTPUT_ROOT = EXAMPLES_ROOT / "results" / "map_func"


def execute_config(cfg_path: str) -> dict:
    """Worker entrypoint invoked on the cluster."""
    cfg_file = Path(cfg_path)
    output_file = OUTPUT_ROOT / f"{cfg_file.stem}_map.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    return process_config(cfg_path, output_file.as_posix(), mode="quick")


def main() -> None:
    input_dir = EXAMPLES_ROOT / "inputs"
    if not input_dir.exists():
        raise SystemExit(
            "Config directory missing. Run `python examples/generate_test_data.py` first."
        )

    configs = [str(path) for path in sorted(input_dir.glob("config_*.json"))[:20]]

    jobs = map_func(
        fn=execute_config,
        items=configs,
        partition="compute",
        cpus_per_task=2,
        mem_gb=8,
        time_min=30,
        job_name="map-func-example",
        retries=1,
    )

    print(f"Submitted {len(jobs)} processing jobs")

    total_scaled = 0.0
    completed = 0
    for job in jobs:
        try:
            report = job.result()
        except Exception as exc:  # noqa: BLE001 - surface worker failure
            print(f"Job {job.job_id} failed: {exc}")
            continue

        completed += 1
        scaled = report.get("scaled_metric")
        if scaled is not None:
            total_scaled += scaled

    if completed:
        print(
            f"Average scaled metric: {total_scaled / completed:.3f} across {completed} completed jobs"
        )


if __name__ == "__main__":
    main()
