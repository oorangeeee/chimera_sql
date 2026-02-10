"""数据库连接器抽象基类（DB-API 2.0 风格）。"""

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple


class DBConnector(ABC):
    """参照 JDBC / Python DB-API 2.0 的统一数据库接口。"""

    @abstractmethod
    def connect(self) -> None:
        """建立数据库连接。"""

    @abstractmethod
    def execute(self, sql: str, params: Optional[tuple] = None) -> None:
        """执行语句（DDL / DML），不返回行。"""

    @abstractmethod
    def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Tuple]:
        """执行查询并以元组列表形式返回结果集。"""

    @abstractmethod
    def close(self) -> None:
        """释放连接及相关资源。"""
