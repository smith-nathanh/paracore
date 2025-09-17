#!/usr/bin/env python3
"""Utility used by the environment example to display key env variables."""

from __future__ import annotations

import argparse
import json
import os


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect environment variables inside a job")
    parser.add_argument(
        "--keys",
        nargs="*",
        default=["PATH", "APP_MODE", "LOG_LEVEL", "PROCESSING_MODE"],
        help="Environment variable names to print",
    )
    args = parser.parse_args()

    snapshot = {key: os.environ.get(key) for key in args.keys}
    print(json.dumps(snapshot, indent=2))


if __name__ == "__main__":
    main()
