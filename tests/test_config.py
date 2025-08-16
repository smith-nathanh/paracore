"""Tests for configuration system."""

import pytest
import tempfile
from pathlib import Path
import yaml

from paracore.config import Config


def test_default_config():
    """Test that default configuration is loaded correctly."""
    config = Config()
    
    assert config.get_active_cluster() == "default"
    cluster_config = config.get_cluster_config()
    
    assert cluster_config["slurm"]["partition"] == "compute"
    assert cluster_config["slurm"]["cpus_per_task"] == 4
    assert cluster_config["slurm"]["mem_gb"] == 16
    assert cluster_config["slurm"]["time_min"] == 60


def test_config_layering(tmp_path):
    """Test configuration layering with user and project configs."""
    # Create user config
    user_config = {
        "active_cluster": "hpc1",
        "clusters": {
            "hpc1": {
                "slurm": {
                    "partition": "gpu",
                    "mem_gb": 32,
                }
            }
        }
    }
    
    user_config_path = tmp_path / ".paracore.yaml"
    with open(user_config_path, "w") as f:
        yaml.dump(user_config, f)
    
    # Create project config
    project_config = {
        "clusters": {
            "hpc1": {
                "slurm": {
                    "time_min": 120,
                }
            }
        }
    }
    
    project_config_path = tmp_path / "paracore.yaml"
    with open(project_config_path, "w") as f:
        yaml.dump(project_config, f)
    
    # Mock home and cwd
    import os
    original_home = os.environ.get("HOME")
    original_cwd = os.getcwd()
    
    try:
        os.environ["HOME"] = str(tmp_path)
        os.chdir(tmp_path)
        
        config = Config()
        
        assert config.get_active_cluster() == "hpc1"
        cluster_config = config.get_cluster_config("hpc1")
        
        # Check layered values
        assert cluster_config["slurm"]["partition"] == "gpu"  # from user config
        assert cluster_config["slurm"]["mem_gb"] == 32  # from user config
        assert cluster_config["slurm"]["time_min"] == 120  # from project config
        assert cluster_config["slurm"]["cpus_per_task"] == 4  # from defaults
        
    finally:
        if original_home:
            os.environ["HOME"] = original_home
        os.chdir(original_cwd)


def test_resolve_with_overrides():
    """Test configuration resolution with overrides."""
    config = Config()
    
    resolved = config.resolve(
        partition="custom",
        mem_gb=64,
        extra={"constraint": "haswell"},
    )
    
    assert resolved["partition"] == "custom"
    assert resolved["mem_gb"] == 64
    assert resolved["cpus_per_task"] == 4  # from defaults
    assert resolved["extra"]["constraint"] == "haswell"


def test_job_name_formatting():
    """Test job name formatting and sanitization."""
    config = Config()
    
    # Test default template
    name = config.format_job_name(None, project="myproj", cluster="hpc1", partition="gpu")
    assert "myproj" in name
    assert "hpc1" in name
    assert "gpu" in name
    
    # Test custom name
    name = config.format_job_name("custom-job")
    assert name == "custom-job"
    
    # Test sanitization
    name = config.format_job_name("job with spaces & special@chars!")
    assert name == "job-with-spaces-special-chars"
    
    # Test truncation
    long_name = "a" * 100
    name = config.format_job_name(long_name)
    assert len(name) <= 80
    assert name.endswith("-")  # hash suffix


def test_unknown_cluster():
    """Test error handling for unknown cluster."""
    config = Config()
    
    with pytest.raises(ValueError, match="Unknown cluster"):
        config.get_cluster_config("nonexistent")