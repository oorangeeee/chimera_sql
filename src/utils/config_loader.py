"""Configuration loader with Singleton pattern."""

from pathlib import Path

import yaml


class ConfigLoader:
    """Singleton configuration loader that reads config/config.yaml."""

    _instance = None
    _config = None

    # Project root: two levels up from this file (src/utils/ -> project root)
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance

    def _load(self):
        config_path = self._PROJECT_ROOT / "config" / "config.yaml"
        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}\n"
                "Please copy config/config.template.yaml to config/config.yaml"
            )
        with open(config_path, "r", encoding="utf-8") as f:
            self._config = yaml.safe_load(f)

    def get(self, key: str, default=None):
        """Get a configuration value using dot-notation.

        Example:
            config.get("oracle.host")  ->  config["oracle"]["host"]
        """
        keys = key.split(".")
        value = self._config
        for k in keys:
            if not isinstance(value, dict) or k not in value:
                return default
            value = value[k]
        return value

    @property
    def all(self) -> dict:
        """Return the entire configuration dictionary."""
        return self._config
