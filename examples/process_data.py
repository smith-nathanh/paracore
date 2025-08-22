#!/usr/bin/env python3
"""Generic data processing script for examples."""

import argparse
import json
import random
import sys
import time
from pathlib import Path


def process_config(config_path: str, output_path: str = None) -> dict:
    """Process a configuration file and optionally write results."""
    # Load config
    with open(config_path) as f:
        config = json.load(f)

    # Simulate some processing work
    process_time = config.get("process_time", random.uniform(0.5, 2.0))
    time.sleep(process_time)

    # Generate result
    result = {
        "id": config.get("id", "unknown"),
        "input": str(config_path),
        "status": "completed",
        "value": config.get("value", 0) * 2.5,  # Some transformation
        "metadata": {
            "processed_at": time.time(),
            "duration": process_time,
        },
    }

    # Write output if requested
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2)
        print(f"Processed {config_path} -> {output_path}")
    else:
        print(json.dumps(result))

    return result


def process_item(item: dict) -> dict:
    """Process a dictionary item (for map_func examples)."""
    # Simulate processing
    time.sleep(item.get("duration", 0.1))

    return {
        "id": item.get("id"),
        "result": item.get("value", 0) * 1.5,
        "processed": True,
    }


def main():
    """Main entry point for command-line usage."""
    parser = argparse.ArgumentParser(description="Process data files")
    parser.add_argument("input", help="Input configuration file")
    parser.add_argument("output", nargs="?", help="Output file (optional)")
    parser.add_argument("--mode", default="simple", choices=["simple", "heavy", "quick"])

    args = parser.parse_args()

    # Adjust processing based on mode
    if args.mode == "heavy":
        # Simulate heavy computation
        time.sleep(random.uniform(5, 10))
    elif args.mode == "quick":
        # Quick processing
        time.sleep(0.1)

    # Process the file
    try:
        result = process_config(args.input, args.output)
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
