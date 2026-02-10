"""基于内置 sqlite3 的 SQLite 数据库连接器。"""

import sqlite3
from pathlib import Path
from typing import Any, List, Optional, Tuple

from src.connector.base import DBConnector
from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)

# 项目根目录，用于解析相对数据库路径
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class SQLiteConnector(DBConnector):
    def __init__(self) -> None:
        config = ConfigLoader()
        db_path_str = str(config.get_or_raise("sqlite.db_path"))

        # 将字符串转换为 Path 对象
        db_path = Path(db_path_str)
        # 使用绝对路径
        self._db_path = (_PROJECT_ROOT / db_path).resolve()
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        if self._conn is not None:
            return

        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Connecting to SQLite at %s", self._db_path)
        self._conn = sqlite3.connect(str(self._db_path))
        logger.info("SQLite connection established")

    def _ensure_connection(self) -> None:
        """确保数据库连接已建立"""
        if self._conn is None:
            self.connect()

    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        self._ensure_connection()

        # 使用 with 语句确保游标正确关闭
        with self._conn as conn:  # type: ignore
            logger.debug("SQLite execute: %s", sql)
            conn.execute(sql, params or ())

    def execute_query(
        self, sql: str, params: Optional[tuple] = None
    ) -> List[Tuple[Any, ...]]:
        self._ensure_connection()

        cursor = self._conn.cursor()  # type: ignore
        try:
            logger.debug("SQLite query: %s", sql)
            cursor.execute(sql, params or ())
            return cursor.fetchall()
        finally:
            cursor.close()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("SQLite connection closed")
