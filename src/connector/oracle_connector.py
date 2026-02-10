"""基于 oracledb（thin 模式）的 Oracle 数据库连接器。"""

from typing import Any, List, Optional, Tuple

import oracledb

from src.connector.base import DBConnector
from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)


class OracleConnector(DBConnector):
    def __init__(self) -> None:
        config = ConfigLoader()

        # 从配置中获取所有必需参数，确保不为None
        self._host = str(config.get_or_raise("oracle.host"))
        self._port = str(config.get_or_raise("oracle.port"))
        self._service = str(config.get_or_raise("oracle.service_name"))
        self._user = str(config.get_or_raise("oracle.user"))
        self._password = str(config.get_or_raise("oracle.password"))

        self._conn: Optional[oracledb.Connection] = None

    def connect(self) -> None:
        if self._conn is not None:
            return

        dsn = f"{self._host}:{self._port}/{self._service}"
        logger.info("Connecting to Oracle at %s as %s", dsn, self._user)

        try:
            self._conn = oracledb.connect(
                user=self._user,
                password=self._password,
                dsn=dsn,
            )
            logger.info("Oracle connection established")
        except Exception as e:
            logger.error("Failed to connect to Oracle: %s", e)
            raise

    def _ensure_connection(self) -> oracledb.Connection:
        """确保数据库连接已建立并返回连接对象"""
        if self._conn is None:
            self.connect()

        # 使用断言告诉类型检查器 self._conn 不为 None
        assert self._conn is not None, "Connection should be established by now"
        return self._conn

    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        conn = self._ensure_connection()
        cursor = conn.cursor()

        try:
            logger.debug("Oracle execute: %s", sql)
            cursor.execute(sql, params or ())
            conn.commit()
        except Exception as e:
            logger.error("Failed to execute SQL: %s", e)
            # 可以选择回滚或重新抛出异常
            conn.rollback()
            raise
        finally:
            cursor.close()

    def execute_query(
        self, sql: str, params: Optional[tuple] = None
    ) -> List[Tuple[Any, ...]]:
        conn = self._ensure_connection()
        cursor = conn.cursor()

        try:
            logger.debug("Oracle query: %s", sql)
            cursor.execute(sql, params or ())
            return cursor.fetchall()
        except Exception as e:
            logger.error("Failed to execute query: %s", e)
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Oracle connection closed")
