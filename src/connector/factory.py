"""连接器工厂 — 根据类型实例化对应的 DBConnector。"""

from src.connector.base import DBConnector
from src.connector.oracle_connector import OracleConnector
from src.connector.sqlite_connector import SQLiteConnector

_REGISTRY = {
    "oracle": OracleConnector,
    "sqlite": SQLiteConnector,
}


class ConnectorFactory:
    @staticmethod
    def create(db_type: str) -> DBConnector:
        """根据给定的数据库类型创建连接器实例。

        Args:
            db_type: "oracle" 或 "sqlite"。
        """
        cls = _REGISTRY.get(db_type.lower())
        if cls is None:
            raise ValueError(
                f"Unknown db_type '{db_type}'. Supported: {list(_REGISTRY)}"
            )
        return cls()
