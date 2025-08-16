"""Configuration management for paracore."""

from __future__ import annotations

import os
import re
import hashlib
from pathlib import Path
from typing import Any, Dict, Optional
import yaml


class Config:
    """Layered configuration system."""
    
    def __init__(self):
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from multiple sources with proper layering."""
        # Start with built-in defaults
        config = self._get_defaults()
        
        # Layer user config
        user_config_path = Path.home() / ".paracore.yaml"
        if user_config_path.exists():
            with open(user_config_path) as f:
                user_config = yaml.safe_load(f) or {}
                config = self._merge_configs(config, user_config)
        
        # Layer project config
        project_config_path = Path("./paracore.yaml")
        if project_config_path.exists():
            with open(project_config_path) as f:
                project_config = yaml.safe_load(f) or {}
                config = self._merge_configs(config, project_config)
        
        return config
    
    def _get_defaults(self) -> Dict[str, Any]:
        """Get built-in safe defaults."""
        return {
            "active_cluster": "default",
            "naming": {
                "default_job_name": "{project}-{cluster}-{partition}",
                "max_len": 80,
            },
            "clusters": {
                "default": {
                    "default_env": None,
                    "io_scratch": "$TMPDIR",
                    "slurm": {
                        "partition": "compute",
                        "account": None,
                        "qos": None,
                        "cpus_per_task": 4,
                        "mem_gb": 16,
                        "time_min": 60,
                        "max_array_parallelism": 100,
                        "start_jitter_s": 0,
                        "extra": {},
                    },
                },
            },
        }
    
    def _merge_configs(self, base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge overlay config into base config."""
        result = base.copy()
        
        for key, value in overlay.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def get_active_cluster(self) -> str:
        """Get the currently active cluster name."""
        return self._config.get("active_cluster", "default")
    
    def get_cluster_config(self, cluster: Optional[str] = None) -> Dict[str, Any]:
        """Get configuration for a specific cluster."""
        if cluster is None:
            cluster = self.get_active_cluster()
        
        clusters = self._config.get("clusters", {})
        if cluster not in clusters:
            raise ValueError(f"Unknown cluster: {cluster}")
        
        return clusters[cluster]
    
    def resolve(self, cluster: Optional[str] = None, **overrides) -> Dict[str, Any]:
        """Resolve final configuration with overrides."""
        cluster_config = self.get_cluster_config(cluster)
        slurm_config = cluster_config.get("slurm", {})
        
        # Build resolved config
        resolved = {
            "cluster": cluster or self.get_active_cluster(),
            "default_env": cluster_config.get("default_env"),
            "io_scratch": cluster_config.get("io_scratch"),
            "partition": overrides.get("partition", slurm_config.get("partition")),
            "account": overrides.get("account", slurm_config.get("account")),
            "qos": overrides.get("qos", slurm_config.get("qos")),
            "cpus_per_task": overrides.get("cpus_per_task", slurm_config.get("cpus_per_task")),
            "mem_gb": overrides.get("mem_gb", slurm_config.get("mem_gb")),
            "time_min": overrides.get("time_min", slurm_config.get("time_min")),
            "max_array_parallelism": overrides.get("array_parallelism", slurm_config.get("max_array_parallelism")),
            "start_jitter_s": overrides.get("jitter_s", slurm_config.get("start_jitter_s", 0)),
            "extra": {**slurm_config.get("extra", {}), **overrides.get("extra", {})},
        }
        
        # Add any additional overrides not in standard fields
        for key, value in overrides.items():
            if key not in resolved and value is not None:
                resolved[key] = value
        
        return resolved
    
    def format_job_name(self, job_name: Optional[str], **context) -> str:
        """Format and sanitize job name."""
        if job_name is None:
            # Use default template
            naming = self._config.get("naming", {})
            template = naming.get("default_job_name", "paracore-job")
            job_name = template
        
        # Format with available context
        try:
            cluster = context.get("cluster", self.get_active_cluster())
            cluster_config = self.get_cluster_config(cluster)
            
            format_context = {
                "project": context.get("project", "paracore"),
                "cluster": cluster,
                "partition": context.get("partition", cluster_config.get("slurm", {}).get("partition", "unknown")),
                "env": context.get("env", cluster_config.get("default_env", "default")),
                **context,
            }
            
            job_name = job_name.format(**format_context)
        except (KeyError, ValueError):
            # If formatting fails, use as-is
            pass
        
        # Sanitize
        job_name = re.sub(r'[^a-zA-Z0-9\-_]', '-', job_name)
        job_name = re.sub(r'-+', '-', job_name).strip('-')
        
        # Truncate if needed
        max_len = self._config.get("naming", {}).get("max_len", 80)
        if len(job_name) > max_len:
            # Add short hash of original name
            hash_suffix = hashlib.md5(job_name.encode()).hexdigest()[:6]
            job_name = f"{job_name[:max_len-7]}-{hash_suffix}"
        
        return job_name or "paracore-job"


_config_instance: Optional[Config] = None


def get_config() -> Config:
    """Get the global config instance."""
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance