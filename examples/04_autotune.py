"""Example: Run pilot jobs to autotune resources before a large batch."""

from __future__ import annotations

from pathlib import Path

from paracore import autotune_from_pilot, map_cmds

EXAMPLES_ROOT = Path(__file__).resolve().parent
CONFIG_ROOT = EXAMPLES_ROOT / "inputs"
RESULT_ROOT = EXAMPLES_ROOT / "results" / "autotune"


def main() -> None:
    if not CONFIG_ROOT.exists():
        raise SystemExit(
            "Config directory missing. Run `python examples/generate_test_data.py` first."
        )

    configs = sorted(CONFIG_ROOT.glob("config_*.json"))
    sample_configs = configs[:20]

    sample_cmds = [
        " ".join(
            [
                "python",
                "examples/process_data.py",
                cfg.as_posix(),
                (RESULT_ROOT / f"pilot_{cfg.stem}.json").as_posix(),
                "--mode",
                "quick",
            ]
        )
        for cfg in sample_configs
    ]

    RESULT_ROOT.mkdir(parents=True, exist_ok=True)

    print("Running pilot jobs to determine optimal resources...")
    suggestions = autotune_from_pilot(
        sample_cmds_or_items=sample_cmds,
        runner="cmds",
        sample_size=10,
        measurement="time_and_rss",
        partition="compute",
        cpus_per_task_guess=2,
        mem_gb_guess=8,
        time_min_guess=20,
    )

    print("Pilot recommendations:")
    for key in ("time_min", "mem_gb", "cpus_per_task", "array_parallelism"):
        if key in suggestions:
            print(f"  {key.replace('_', ' ').title()}: {suggestions[key]}")
    if "_info" in suggestions:
        print(f"  note: {suggestions['_info']}")

    batch_cmds = [
        " ".join(
            [
                "python",
                "examples/process_data.py",
                cfg.as_posix(),
                (RESULT_ROOT / f"batch_{cfg.stem}.json").as_posix(),
                "--mode",
                "simple",
            ]
        )
        for cfg in configs[:80]
    ]

    resource_args = {
        key: suggestions[key]
        for key in ("time_min", "mem_gb", "cpus_per_task", "array_parallelism")
        if key in suggestions and suggestions[key] is not None
    }

    print(f"\nSubmitting {len(batch_cmds)} jobs with tuned resources...")
    jobs = map_cmds(
        batch_cmds,
        partition="compute",
        job_name="autotuned-batch",
        retries=1,
        **resource_args,
    )

    print(f"Submitted {len(jobs)} jobs with autotuned resources")


if __name__ == "__main__":
    main()
