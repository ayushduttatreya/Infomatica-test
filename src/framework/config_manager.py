"""
Configuration Management Module
Handles loading and accessing configuration from YAML files with environment variable support.
"""
import os
import yaml
from typing import Any, Dict, Optional
from pathlib import Path


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass


class ConfigManager:
    """
    Centralized configuration manager with support for multiple environments
    and environment variable interpolation.
    """
    
    _instance = None
    _config: Dict[str, Any] = {}
    _environment: str = "development"
    
    def __new__(cls):
        """Singleton pattern to ensure single configuration instance."""
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize configuration manager."""
        if not self._config:
            self._environment = os.getenv('NIFI_ENV', 'development')
    
    def load_config(self, config_path: str) -> None:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to configuration YAML file
            
        Raises:
            ConfigurationError: If config file cannot be loaded
        """
        try:
            config_file = Path(config_path)
            if not config_file.exists():
                raise ConfigurationError(f"Configuration file not found: {config_path}")
            
            with open(config_file, 'r') as f:
                raw_config = yaml.safe_load(f)
            
            if not raw_config:
                raise ConfigurationError(f"Empty configuration file: {config_path}")
            
            # Interpolate environment variables
            self._config = self._interpolate_env_vars(raw_config)
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in configuration file: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {e}")
    
    def _interpolate_env_vars(self, config: Any) -> Any:
        """
        Recursively interpolate environment variables in configuration.
        Supports ${VAR_NAME} and ${VAR_NAME:default_value} syntax.
        
        Args:
            config: Configuration object (dict, list, or primitive)
            
        Returns:
            Configuration with interpolated values
        """
        if isinstance(config, dict):
            return {k: self._interpolate_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._interpolate_env_vars(item) for item in config]
        elif isinstance(config, str):
            return self._replace_env_var(config)
        else:
            return config
    
    def _replace_env_var(self, value: str) -> str:
        """
        Replace environment variable placeholders in string.
        
        Args:
            value: String potentially containing ${VAR} placeholders
            
        Returns:
            String with environment variables replaced
        """
        import re
        
        pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'
        
        def replacer(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) is not None else ''
            return os.getenv(var_name, default_value)
        
        return re.sub(pattern, replacer, value)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to configuration value (e.g., 'nifi.host')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        value = self._config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def get_required(self, key_path: str) -> Any:
        """
        Get required configuration value, raise error if not found.
        
        Args:
            key_path: Dot-separated path to configuration value
            
        Returns:
            Configuration value
            
        Raises:
            ConfigurationError: If required key not found
        """
        value = self.get(key_path)
        if value is None:
            raise ConfigurationError(f"Required configuration key not found: {key_path}")
        return value
    
    def get_environment(self) -> str:
        """Get current environment name."""
        return self._environment
    
    def get_all(self) -> Dict[str, Any]:
        """Get entire configuration dictionary."""
        return self._config.copy()
    
    def reload(self, config_path: str) -> None:
        """
        Reload configuration from file.
        
        Args:
            config_path: Path to configuration YAML file
        """
        self._config = {}
        self.load_config(config_path)


# Global configuration instance
config = ConfigManager()