"""
Unit tests for configuration management.
"""
import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.config.settings import ConfigManager, PipelineSettings, SnowflakeConfig


class TestConfigManager:
    """Test configuration management functionality."""
    
    def test_load_base_config_only(self):
        """Test loading base configuration when no environment config exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Create base config
            base_config = config_dir / "base.yaml"
            base_config.write_text("""
debug: false
logging:
  level: INFO
""")
            
            manager = ConfigManager(str(config_dir))
            settings = manager.load_config("test")
            
            assert settings.environment == "test"
            assert settings.debug is False
            assert settings.logging.level == "INFO"
    
    def test_environment_override(self):
        """Test environment-specific configuration overrides."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Create base config
            base_config = config_dir / "base.yaml"
            base_config.write_text("""
debug: false
logging:
  level: INFO
""")
            
            # Create environment config
            env_config = config_dir / "development.yaml"
            env_config.write_text("""
debug: true
logging:
  level: DEBUG
""")
            
            manager = ConfigManager(str(config_dir))
            settings = manager.load_config("development")
            
            assert settings.debug is True
            assert settings.logging.level == "DEBUG"
    
    @patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'test-account', 'DEBUG': 'true'})
    def test_environment_variable_override(self):
        """Test environment variable overrides."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir)
            
            # Create base config with snowflake section
            base_config = config_dir / "base.yaml"
            base_config.write_text("""
debug: false
snowflake:
  account: base-account
  username: base-user
""")
            
            manager = ConfigManager(str(config_dir))
            settings = manager.load_config("test")
            
            assert settings.debug is True  # From env var
            assert settings.snowflake.account == "test-account"  # From env var
            assert settings.snowflake.username == "base-user"  # From config
    
    def test_snowflake_config_creation(self):
        """Test SnowflakeConfig creation from dictionary."""
        config_data = {
            'account': 'test-account',
            'username': 'test-user',
            'password': 'test-pass',
            'warehouse': 'TEST_WH',
            'database': 'TEST_DB',
            'schema': 'TEST_SCHEMA'
        }
        
        snowflake_config = SnowflakeConfig.from_dict(config_data)
        
        assert snowflake_config.account == 'test-account'
        assert snowflake_config.username == 'test-user'
        assert snowflake_config.warehouse == 'TEST_WH'