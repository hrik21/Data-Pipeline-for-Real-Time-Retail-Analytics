"""
Configuration management system with environment-specific settings.
"""
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path
import yaml
import json


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int
    database: str
    username: str
    password: str
    schema: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DatabaseConfig':
        return cls(**data)


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration."""
    account: str
    username: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SnowflakeConfig':
        return cls(**data)


@dataclass
class AirflowConfig:
    """Airflow configuration."""
    dags_folder: str
    base_url: str
    username: Optional[str] = None
    password: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AirflowConfig':
        return cls(**data)


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "json"
    file_path: Optional[str] = None
    max_file_size: str = "10MB"
    backup_count: int = 5
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LoggingConfig':
        return cls(**data)


@dataclass
class PipelineSettings:
    """Main pipeline configuration."""
    environment: str
    debug: bool = False
    snowflake: Optional[SnowflakeConfig] = None
    airflow: Optional[AirflowConfig] = None
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    databases: Dict[str, DatabaseConfig] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PipelineSettings':
        """Create settings from dictionary."""
        # Handle nested configurations
        if 'snowflake' in data and data['snowflake']:
            data['snowflake'] = SnowflakeConfig.from_dict(data['snowflake'])
        
        if 'airflow' in data and data['airflow']:
            data['airflow'] = AirflowConfig.from_dict(data['airflow'])
        
        if 'logging' in data and data['logging']:
            data['logging'] = LoggingConfig.from_dict(data['logging'])
        
        if 'databases' in data and data['databases']:
            databases = {}
            for name, db_config in data['databases'].items():
                databases[name] = DatabaseConfig.from_dict(db_config)
            data['databases'] = databases
        
        return cls(**data)


class ConfigManager:
    """Manages configuration loading and environment-specific settings."""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self._settings: Optional[PipelineSettings] = None
        self._environment = os.getenv("PIPELINE_ENV", "development")
    
    def load_config(self, environment: Optional[str] = None) -> PipelineSettings:
        """Load configuration for specified environment."""
        env = environment or self._environment
        
        # Load base configuration
        base_config = self._load_config_file("base.yaml")
        
        # Load environment-specific configuration
        env_config = self._load_config_file(f"{env}.yaml")
        
        # Merge configurations (environment overrides base)
        merged_config = self._merge_configs(base_config, env_config)
        
        # Override with environment variables
        merged_config = self._apply_env_overrides(merged_config)
        
        # Set environment in config
        merged_config['environment'] = env
        
        self._settings = PipelineSettings.from_dict(merged_config)
        return self._settings
    
    def get_settings(self) -> PipelineSettings:
        """Get current settings, loading if necessary."""
        if self._settings is None:
            return self.load_config()
        return self._settings
    
    def _load_config_file(self, filename: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        config_path = self.config_dir / filename
        
        if not config_path.exists():
            return {}
        
        with open(config_path, 'r') as f:
            return yaml.safe_load(f) or {}
    
    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge configuration dictionaries."""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply environment variable overrides."""
        # Define environment variable mappings
        env_mappings = {
            'SNOWFLAKE_ACCOUNT': ['snowflake', 'account'],
            'SNOWFLAKE_USERNAME': ['snowflake', 'username'],
            'SNOWFLAKE_PASSWORD': ['snowflake', 'password'],
            'SNOWFLAKE_WAREHOUSE': ['snowflake', 'warehouse'],
            'SNOWFLAKE_DATABASE': ['snowflake', 'database'],
            'SNOWFLAKE_SCHEMA': ['snowflake', 'schema'],
            'AIRFLOW_BASE_URL': ['airflow', 'base_url'],
            'AIRFLOW_USERNAME': ['airflow', 'username'],
            'AIRFLOW_PASSWORD': ['airflow', 'password'],
            'LOG_LEVEL': ['logging', 'level'],
            'DEBUG': ['debug'],
        }
        
        for env_var, config_path in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                # Navigate to the nested dictionary
                current = config
                for key in config_path[:-1]:
                    if key not in current:
                        current[key] = {}
                    current = current[key]
                
                # Set the value, converting boolean strings
                final_key = config_path[-1]
                if value.lower() in ('true', 'false'):
                    current[final_key] = value.lower() == 'true'
                else:
                    current[final_key] = value
        
        return config


# Global configuration manager instance
config_manager = ConfigManager()


def get_settings() -> PipelineSettings:
    """Get current pipeline settings."""
    return config_manager.get_settings()