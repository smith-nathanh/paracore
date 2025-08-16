#!/usr/bin/env python3
"""Generate test data for examples."""

import json
import os
from pathlib import Path


def generate_test_configs(output_dir="inputs", count=100):
    """Generate test configuration files."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    for i in range(count):
        config = {
            "id": f"task_{i:04d}",
            "value": 100 + i * 10,
            "process_time": 0.5 + (i % 10) * 0.1,  # Vary processing time
            "parameters": {
                "iterations": 1000 + i * 100,
                "threshold": 0.5,
            }
        }
        
        config_path = output_path / f"config_{i:04d}.json"
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)
    
    print(f"Generated {count} config files in {output_dir}/")


if __name__ == "__main__":
    generate_test_configs()