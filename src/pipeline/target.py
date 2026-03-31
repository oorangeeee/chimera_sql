"""已注册数据库定义与加载。

从 config.yaml 的 databases 节读取项目已对接的数据库类型，
并提供按方言名称查找的便捷方法。
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

from src.utils.config_loader import ConfigLoader


@dataclass(frozen=True)
class DatabaseEntry:
    """项目已对接的数据库类型。

    Attributes:
        name: 配置中的 key，如 "oracle"。
        db_type: ConnectorFactory.create() 参数，如 "oracle" / "sqlite"。
        sqlglot_dialect: SQLGlot 方言名，用于转译和变异能力画像。
    """

    name: str
    db_type: str
    sqlglot_dialect: str


def load_databases() -> Dict[str, DatabaseEntry]:
    """从 config.yaml databases 节加载全部已注册数据库。

    Returns:
        以 name 为 key 的 DatabaseEntry 字典。

    Raises:
        ValueError: databases 节不存在或为空。
    """
    config = ConfigLoader()
    raw = config.get("databases")

    if not raw or not isinstance(raw, dict):
        raise ValueError(
            "config.yaml 中未找到有效的 databases 配置节。"
            "请参考 config.template.yaml 添加 databases 定义。"
        )

    if not raw:
        raise ValueError("databases 列表为空，请指定至少一个已对接的数据库。")

    result: Dict[str, DatabaseEntry] = {}
    for name, entry in raw.items():
        if not isinstance(entry, dict):
            raise ValueError(f"databases.{name} 配置格式错误，应为字典。")
        result[name] = DatabaseEntry(
            name=name,
            db_type=entry.get("db_type", name),
            sqlglot_dialect=entry.get("sqlglot_dialect", name),
        )

    return result


def resolve_database(dialect: str) -> DatabaseEntry:
    """根据方言名称查找已注册的数据库。

    Args:
        dialect: 方言名称（如 "oracle"、"sqlite"）。

    Returns:
        匹配的 DatabaseEntry。

    Raises:
        ValueError: 未找到匹配的已注册数据库。
    """
    databases = load_databases()
    for db in databases.values():
        if db.name.lower() == dialect.lower():
            return db

    available = [db.name for db in databases.values()]
    raise ValueError(
        f"项目未对接方言为 '{dialect}' 的数据库。"
        f"已对接的数据库: {available}"
    )
