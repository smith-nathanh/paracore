"""Example: Demonstrate environment handling strategies."""

from __future__ import annotations

from pathlib import Path

from paracore import map_cmds, run_cmd

EXAMPLES_ROOT = Path(__file__).resolve().parent
CONFIG = (EXAMPLES_ROOT / "inputs" / "config_0000.json").as_posix()


def main() -> None:
    if not (EXAMPLES_ROOT / "inputs").exists():
        raise SystemExit(
            "Config directory missing. Run `python examples/generate_test_data.py` first."
        )

    job1 = run_cmd(
        "python examples/check_env.py --keys PATH APP_MODE",
        job_name="env-inherit",
        partition="compute",
    )

    job2 = run_cmd(
        "python examples/check_env.py --keys PATH APP_MODE LOG_LEVEL",
        env={"APP_MODE": "production", "LOG_LEVEL": "info"},
        env_merge="inherit",
        job_name="env-overlay",
        partition="compute",
    )

    job3 = run_cmd(
        "python examples/check_env.py --keys PATH APP_MODE",
        env={
            "PATH": "/opt/tools:/usr/bin",
            "APP_MODE": "minimal",
        },
        env_merge="replace",
        job_name="env-replace",
        partition="compute",
    )

    job4 = run_cmd(
        f"python examples/process_data.py {CONFIG} --mode quick",
        env_setup="source /opt/envs/prod/bin/activate",
        env={"ANALYSIS_TYPE": "full"},
        job_name="env-setup",
        partition="compute",
    )

    cmds = [
        " ".join(
            [
                "python",
                "examples/process_data.py",
                (EXAMPLES_ROOT / "inputs" / f"config_{i:04d}.json").as_posix(),
                "--mode",
                "simple",
            ]
        )
        for i in range(5)
    ]
    jobs = map_cmds(
        cmds,
        env={"PROCESSING_MODE": "parallel", "LOG_LEVEL": "warning"},
        partition="compute",
        array_parallelism=3,
        job_name="env-array",
    )

    print("Submitted environment test jobs:")
    print(f"  Inherit: {job1.job_id}")
    print(f"  Overlay: {job2.job_id}")
    print(f"  Replace: {job3.job_id}")
    print(f"  Setup: {job4.job_id}")
    print(f"  Array: {len(jobs)} jobs")


if __name__ == "__main__":
    main()
