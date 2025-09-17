#!/usr/bin/env python3
"""Synthetic yet realistic data processing script for Paracore examples."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Iterable


def _resolve_dataset(config_path: Path, dataset_field: str) -> Path:
    dataset_path = Path(dataset_field)
    if not dataset_path.is_absolute():
        dataset_path = (config_path.parent / dataset_path).resolve()
    return dataset_path


def _iter_values(dataset_path: Path, *, normalize: bool) -> Iterable[tuple[float, str]]:
    with dataset_path.open() as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            value = float(row["value"])
            if normalize:
                baseline = float(row.get("baseline", 1.0)) or 1.0
                value /= baseline
            yield value, row.get("category", "unknown")


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return float("nan")
    sorted_vals = sorted(values)
    idx = min(int(math.ceil(percentile * len(sorted_vals))) - 1, len(sorted_vals) - 1)
    return sorted_vals[max(idx, 0)]


def process_config(
    config_path: str,
    output_path: str | None = None,
    *,
    mode: str = "simple",
) -> dict:
    """Process a configuration file and optionally write results."""

    start_time = time.time()
    config_file = Path(config_path)
    with config_file.open() as f:
        config = json.load(f)

    analysis = config.get("analysis", {})
    dataset_field = analysis.get("dataset", config.get("dataset"))
    if dataset_field is None:
        raise ValueError("Config missing 'dataset' path")
    dataset_path = _resolve_dataset(config_file, dataset_field)

    normalize = bool(analysis.get("normalize"))
    min_value = float(analysis.get("min_value", float("-inf")))
    target_metric = analysis.get("metric", "mean")
    scale_factor = float(analysis.get("scale_factor", 1.0))

    raw_values: list[float] = []
    categories = Counter()

    for value, category in _iter_values(dataset_path, normalize=normalize):
        categories[category] += 1
        if value >= min_value:
            raw_values.append(value)

    filtered_rows = len(raw_values)
    total_rows = sum(categories.values())

    metrics = {
        "mean": statistics.mean(raw_values) if raw_values else float("nan"),
        "median": statistics.median(raw_values) if raw_values else float("nan"),
        "max": max(raw_values) if raw_values else float("nan"),
        "min": min(raw_values) if raw_values else float("nan"),
        "p95": _percentile(raw_values, 0.95) if raw_values else float("nan"),
        "count": filtered_rows,
    }
    metric_value = metrics.get(target_metric, float("nan"))
    scaled_value = metric_value * scale_factor if not math.isnan(metric_value) else float("nan")

    mode_delay = {
        "simple": 0.25,
        "quick": 0.05,
        "heavy": 1.0,
    }.get(mode, 0.25)
    time.sleep(mode_delay)

    result = {
        "id": config.get("id"),
        "dataset": str(dataset_path),
        "rows_total": total_rows,
        "rows_retained": filtered_rows,
        "category_breakdown": dict(categories),
        "metrics": {
            k: round(v, 4) if isinstance(v, float) and not math.isnan(v) else v for k, v in metrics.items()
        },
        "target_metric": target_metric,
        "scaled_metric": round(scaled_value, 4) if not math.isnan(scaled_value) else None,
        "mode": mode,
        "analysis": analysis,
        "processing_seconds": round(time.time() - start_time, 3),
    }

    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open("w") as f:
            json.dump(result, f, indent=2)
        print(f"Processed {config_path} -> {output_path}")
    else:
        print(json.dumps(result, indent=2))

    return result


def main() -> None:
    """Main entry point for command-line usage."""

    parser = argparse.ArgumentParser(description="Process example datasets based on configs")
    parser.add_argument("input", help="Input configuration file")
    parser.add_argument("output", nargs="?", help="Output file (optional)")
    parser.add_argument(
        "--mode",
        default="simple",
        choices=["simple", "quick", "heavy"],
        help="Adjusts simulated compute intensity",
    )

    args = parser.parse_args()

    try:
        process_config(args.input, args.output, mode=args.mode)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
