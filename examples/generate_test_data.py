#!/usr/bin/env python3
"""Generate synthetic configs + datasets for the examples pipeline."""

from __future__ import annotations

import csv
import json
import math
import random
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_COUNT = 120
ROWS_PER_DATASET = 240
METRICS = ("mean", "max", "p95", "median")


def _generate_dataset(path: Path, *, seed: int, rows: int = ROWS_PER_DATASET) -> dict[str, float]:
    """Write a CSV dataset with synthetic metrics and return quick stats."""
    rng = random.Random(seed)
    base_value = rng.uniform(80, 160)
    jitter = rng.uniform(0.2, 0.6)

    path.parent.mkdir(parents=True, exist_ok=True)

    total = 0.0
    total_sq = 0.0
    min_value = math.inf
    max_value = -math.inf

    with path.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "value", "baseline", "category"])
        for idx in range(rows):
            timestamp = idx
            category = rng.choice(["train", "validate", "test"])
            noise = rng.gauss(0, jitter)
            value = base_value * (1 + noise)
            baseline = base_value * rng.uniform(0.85, 1.15)

            writer.writerow([timestamp, f"{value:.4f}", f"{baseline:.4f}", category])

            total += value
            total_sq += value * value
            min_value = min(min_value, value)
            max_value = max(max_value, value)

    mean = total / rows
    variance = max(total_sq / rows - mean**2, 0.0)
    stddev = math.sqrt(variance)

    return {
        "mean": mean,
        "stddev": stddev,
        "min": min_value,
        "max": max_value,
        "rows": rows,
    }


def _analysis_plan(rng: random.Random) -> dict[str, float | str | bool]:
    metric = rng.choice(METRICS)
    return {
        "metric": metric,
        "min_value": round(rng.uniform(70, 120), 2),
        "normalize": rng.random() < 0.5,
        "scale_factor": round(rng.uniform(0.8, 1.3), 3),
    }


def generate_test_configs(
    *,
    config_dir: str | Path = BASE_DIR / "inputs",
    dataset_dir: str | Path = BASE_DIR / "datasets",
    count: int = DEFAULT_COUNT,
    rows_per_dataset: int = ROWS_PER_DATASET,
) -> None:
    """Generate configuration JSON files and matching CSV datasets."""
    config_root = Path(config_dir)
    dataset_root = Path(dataset_dir)
    config_root.mkdir(parents=True, exist_ok=True)
    dataset_root.mkdir(parents=True, exist_ok=True)

    rng = random.Random(13)

    for i in range(count):
        dataset_path = dataset_root / f"dataset_{i:04d}.csv"
        stats = _generate_dataset(dataset_path, seed=1_000 + i, rows=rows_per_dataset + (i % 50))

        dataset_relative = Path("..") / dataset_path.relative_to(BASE_DIR)

        config = {
            "id": f"job_{i:04d}",
            "dataset": str(dataset_relative),
            "analysis": _analysis_plan(rng),
            "resources": {
                "estimated_rows": stats["rows"],
                "estimated_mean": round(stats["mean"], 2),
                "estimated_stddev": round(stats["stddev"], 2),
            },
            "metadata": {
                "owner": rng.choice(["analytics", "ml", "science"]),
                "priority": rng.choice(["standard", "high", "bulk"]),
            },
        }

        config_path = config_root / f"config_{i:04d}.json"
        with config_path.open("w") as f:
            json.dump(config, f, indent=2)

    print(
        f"Generated {count} configs in {config_root}/ and datasets in {dataset_root}/."
    )


if __name__ == "__main__":
    generate_test_configs()
