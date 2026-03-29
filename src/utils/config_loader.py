"""采用单例模式的配置加载器。"""

from pathlib import Path
from typing import Any, Dict

import yaml

from src.utils.constants import PROJECT_ROOT


class ConfigLoader:
    """单例配置加载器，读取 config/config.yaml。"""

    _instance: "ConfigLoader" = None  # type: ignore[assignment]
    _config: Dict[str, Any] = {}

    # 项目根目录
    _PROJECT_ROOT: Path = PROJECT_ROOT

    def __new__(cls) -> "ConfigLoader":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance

    def _load(self) -> None:
        config_path = PROJECT_ROOT / "config" / "config.yaml"
        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}\n"
                "Please copy config/config.template.yaml to config/config.yaml"
            )
        with open(config_path, "r", encoding="utf-8") as f:
            loaded_config = yaml.safe_load(f)
            # 确保配置是一个字典，如果为None则使用空字典
            self._config = loaded_config if isinstance(loaded_config, dict) else {}

    def get(self, key: str, default: Any = None) -> Any:
        """使用点号表示法获取配置值。
        示例：
        config.get("oracle.host") -> config["oracle"]["host"]
        """
        if not self._config:
            return default

        keys = key.split(".")
        value = self._config
        for k in keys:
            if not isinstance(value, dict) or k not in value:
                return default
            value = value[k]
        return value

    def get_or_raise(self, key: str) -> Any:
        """获取配置值，如果不存在则抛出异常。"""
        value = self.get(key)
        if value is None:
            raise ValueError(f"Configuration key '{key}' is not set")
        return value

    @property
    def all(self) -> Dict[str, Any]:
        """返回完整的配置字典。"""
        return self._config
