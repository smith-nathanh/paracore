"""CLI entry point for paracore."""

import argparse
import json
import sys
from typing import List, Optional

from paracore import autotune_from_pilot, map_cmds, run_cmd


def parse_env_vars(env_list: Optional[List[str]]) -> dict:
    """Parse environment variables from KEY=VALUE format."""
    if not env_list:
        return {}

    env_dict = {}
    for item in env_list:
        if "=" not in item:
            print(f"Warning: Invalid env format '{item}', expected KEY=VALUE", file=sys.stderr)
            continue
        key, value = item.split("=", 1)
        env_dict[key] = value
    return env_dict


def cmd_run(args):
    """Execute the 'run' subcommand."""
    job = run_cmd(
        cmd=args.command,
        job_name=args.name,
        partition=args.partition,
        time_min=args.time,
        cpus_per_task=args.cpus,
        mem_gb=args.memory,
        env_setup=args.env_setup,
        env=parse_env_vars(args.env),
        env_merge=args.env_merge,
        account=args.account,
        qos=args.qos,
        retries=args.retries,
        retry_backoff_s=args.retry_backoff,
    )

    print(f"Submitted job {job.job_id}")
    if args.name:
        print(f"Job name: {job.job_name}")
    if job.stdout_path:
        print(f"Stdout: {job.stdout_path}")
    if job.stderr_path:
        print(f"Stderr: {job.stderr_path}")

    if args.wait:
        print("Waiting for job to complete...")
        try:
            result = job.result(timeout=args.wait_timeout)
            print("Job completed successfully")
            if result:
                print(f"Result: {result}")
        except Exception as e:
            print(f"Job failed: {e}", file=sys.stderr)
            sys.exit(1)


def cmd_batch(args):
    """Execute the 'batch' subcommand."""
    # Read commands from file or stdin
    if args.file == "-":
        cmds = [line.strip() for line in sys.stdin if line.strip()]
    else:
        with open(args.file) as f:
            cmds = [line.strip() for line in f if line.strip()]

    if not cmds:
        print("No commands to run", file=sys.stderr)
        sys.exit(1)

    print(f"Submitting {len(cmds)} jobs...")

    jobs = map_cmds(
        cmds=cmds,
        job_name=args.name,
        partition=args.partition,
        time_min=args.time,
        cpus_per_task=args.cpus,
        mem_gb=args.memory,
        env_setup=args.env_setup,
        env=parse_env_vars(args.env),
        env_merge=args.env_merge,
        array_parallelism=args.array_parallelism,
        account=args.account,
        qos=args.qos,
        retries=args.retries,
        retry_backoff_s=args.retry_backoff,
    )

    print(f"Submitted {len(jobs)} jobs")
    print(f"Job IDs: {jobs[0].job_id} - {jobs[-1].job_id}")

    if args.wait:
        print("Waiting for all jobs to complete...")
        results = []
        failed = []
        for i, job in enumerate(jobs):
            try:
                result = job.result(timeout=args.wait_timeout)
                results.append(result)
                if (i + 1) % 10 == 0:
                    print(f"Completed {i + 1}/{len(jobs)} jobs")
            except Exception as e:
                failed.append((i, job.job_id, str(e)))

        print(f"Completed {len(results)} jobs successfully")
        if failed:
            print(f"Failed {len(failed)} jobs", file=sys.stderr)
            for idx, job_id, error in failed[:5]:
                print(f"  Job {idx} ({job_id}): {error}", file=sys.stderr)
            sys.exit(1)


def cmd_autotune(args):
    """Execute the 'autotune' subcommand."""
    # Read commands from file
    if args.file == "-":
        cmds = [line.strip() for line in sys.stdin if line.strip()]
    else:
        with open(args.file) as f:
            cmds = [line.strip() for line in f if line.strip()]

    if not cmds:
        print("No commands for pilot", file=sys.stderr)
        sys.exit(1)

    print(f"Running pilot with {args.sample_size} jobs from {len(cmds)} total...")

    suggestions = autotune_from_pilot(
        sample_cmds_or_items=cmds,
        runner="cmds",
        sample_size=args.sample_size,
        measurement="time_and_rss" if args.measure_memory else "time_only",
        partition=args.partition,
        cpus_per_task_guess=args.cpus_guess,
        mem_gb_guess=args.memory_guess,
        time_min_guess=args.time_guess,
        env_setup=args.env_setup,
    )

    print("\nResource recommendations:")
    print(f"  Time:        {suggestions['time_min']} minutes")
    print(f"  Memory:      {suggestions['mem_gb']} GB")
    print(f"  CPUs:        {suggestions['cpus_per_task']}")
    print(f"  Parallelism: {suggestions['array_parallelism']}")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(suggestions, f, indent=2)
        print(f"\nRecommendations saved to {args.output}")

    if args.export_shell:
        print("\nExport as shell variables:")
        print(f"export PARACORE_TIME={suggestions['time_min']}")
        print(f"export PARACORE_MEMORY={suggestions['mem_gb']}")
        print(f"export PARACORE_CPUS={suggestions['cpus_per_task']}")
        print(f"export PARACORE_PARALLEL={suggestions['array_parallelism']}")


def cmd_status(args):
    """Check status of jobs (placeholder)."""
    print("Status command not yet implemented")
    print("Use 'squeue' or 'sacct' directly for now")
    sys.exit(0)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Paracore - Slurm job submission made easy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Submit a single job
  paracore run "python script.py" --time 60 --memory 16
  
  # Submit batch jobs from file
  paracore batch commands.txt --array-parallelism 20
  
  # Run pilot for autotuning
  paracore autotune commands.txt --sample-size 10
  
  # Pipe commands
  cat commands.txt | paracore batch - --wait
""",
    )

    # Global arguments
    parser.add_argument("--version", action="version", version="%(prog)s 1.0.0")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Submit a single job")
    run_parser.add_argument("command", help="Command to run")
    run_parser.add_argument("-n", "--name", help="Job name")
    run_parser.add_argument("-p", "--partition", help="Slurm partition")
    run_parser.add_argument("-t", "--time", type=int, help="Time limit (minutes)")
    run_parser.add_argument("-c", "--cpus", type=int, help="CPUs per task")
    run_parser.add_argument("-m", "--memory", type=int, help="Memory (GB)")
    run_parser.add_argument(
        "-e", "--env", action="append", help="Environment variables (KEY=VALUE)"
    )
    run_parser.add_argument("--env-setup", help="Environment setup command")
    run_parser.add_argument(
        "--env-merge",
        choices=["inherit", "replace"],
        default="inherit",
        help="Environment merge strategy",
    )
    run_parser.add_argument("-a", "--account", help="Slurm account")
    run_parser.add_argument("-q", "--qos", help="Quality of service")
    run_parser.add_argument("--retries", type=int, default=0, help="Number of retries")
    run_parser.add_argument(
        "--retry-backoff", type=float, default=30.0, help="Retry backoff (seconds)"
    )
    run_parser.add_argument("-w", "--wait", action="store_true", help="Wait for completion")
    run_parser.add_argument("--wait-timeout", type=float, help="Wait timeout (seconds)")
    run_parser.set_defaults(func=cmd_run)

    # Batch command
    batch_parser = subparsers.add_parser("batch", help="Submit array job from file")
    batch_parser.add_argument("file", help="File with commands (one per line, use - for stdin)")
    batch_parser.add_argument("-n", "--name", help="Job name")
    batch_parser.add_argument("-p", "--partition", help="Slurm partition")
    batch_parser.add_argument("-t", "--time", type=int, help="Time limit (minutes)")
    batch_parser.add_argument("-c", "--cpus", type=int, help="CPUs per task")
    batch_parser.add_argument("-m", "--memory", type=int, help="Memory (GB)")
    batch_parser.add_argument(
        "-e", "--env", action="append", help="Environment variables (KEY=VALUE)"
    )
    batch_parser.add_argument("--env-setup", help="Environment setup command")
    batch_parser.add_argument(
        "--env-merge",
        choices=["inherit", "replace"],
        default="inherit",
        help="Environment merge strategy",
    )
    batch_parser.add_argument("--array-parallelism", type=int, help="Max concurrent array jobs")
    batch_parser.add_argument("-a", "--account", help="Slurm account")
    batch_parser.add_argument("-q", "--qos", help="Quality of service")
    batch_parser.add_argument("--retries", type=int, default=0, help="Number of retries")
    batch_parser.add_argument(
        "--retry-backoff", type=float, default=30.0, help="Retry backoff (seconds)"
    )
    batch_parser.add_argument("-w", "--wait", action="store_true", help="Wait for completion")
    batch_parser.add_argument("--wait-timeout", type=float, help="Wait timeout per job (seconds)")
    batch_parser.set_defaults(func=cmd_batch)

    # Autotune command
    tune_parser = subparsers.add_parser(
        "autotune", help="Run pilot jobs to determine optimal resources"
    )
    tune_parser.add_argument("file", help="File with commands (one per line, use - for stdin)")
    tune_parser.add_argument(
        "-s", "--sample-size", type=int, default=10, help="Number of pilot jobs"
    )
    tune_parser.add_argument("-p", "--partition", help="Slurm partition")
    tune_parser.add_argument("--cpus-guess", type=int, default=4, help="Initial CPU guess")
    tune_parser.add_argument(
        "--memory-guess", type=int, default=8, help="Initial memory guess (GB)"
    )
    tune_parser.add_argument(
        "--time-guess", type=int, default=30, help="Initial time guess (minutes)"
    )
    tune_parser.add_argument("--env-setup", help="Environment setup command")
    tune_parser.add_argument("--measure-memory", action="store_true", help="Measure memory usage")
    tune_parser.add_argument("-o", "--output", help="Save recommendations to JSON file")
    tune_parser.add_argument(
        "--export-shell", action="store_true", help="Print shell export commands"
    )
    tune_parser.set_defaults(func=cmd_autotune)

    # Status command
    status_parser = subparsers.add_parser("status", help="Check job status (placeholder)")
    status_parser.add_argument("job_ids", nargs="*", help="Job IDs to check")
    status_parser.set_defaults(func=cmd_status)

    # Parse arguments
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Execute command
    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
