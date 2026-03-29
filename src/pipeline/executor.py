"""单目标 SQL 执行器。

负责连接目标数据库，执行 SQL 并记录结果（状态、行数、耗时、错误信息等）。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.connector.factory import ConnectorFactory
from src.connector.base import DBConnector
from src.utils.json_utils import rows_to_jsonable
from src.utils.logger import get_logger

from .target import TargetDatabase

logger = get_logger("pipeline.executor")


@dataclass
class SQLExecutionResult:
    """单条 SQL 的执行结果。

    Attributes:
        file: 输出文件相对路径。
        seed_file: 原始种子文件名。
        executed_sql: 实际执行的 SQL。
        status: 执行状态，"ok" 或 "error"。
        row_count: 结果行数。
        rows: 结果行（已做 JSON 序列化安全转换）。
        error: 错误信息（成功时为 None）。
        elapsed_ms: 执行耗时（毫秒）。
        mutation_strategies: 应用的变异策略 ID 列表。
        transpile_rules: 应用的转译规则名称列表。
        transpile_warnings: 转译警告列表。
    """

    file: str
    seed_file: str
    executed_sql: str
    status: str
    row_count: int
    rows: List[List[Any]] = field(default_factory=list)
    error: Optional[str] = None
    elapsed_ms: float = 0.0
    mutation_strategies: List[str] = field(default_factory=list)
    transpile_rules: List[str] = field(default_factory=list)
    transpile_warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """转换为可 JSON 序列化的字典。"""
        return {
            "file": self.file,
            "seed_file": self.seed_file,
            "executed_sql": self.executed_sql,
            "status": self.status,
            "row_count": self.row_count,
            "rows": self.rows,
            "error": self.error,
            "elapsed_ms": self.elapsed_ms,
            "mutation_strategies": self.mutation_strategies,
            "transpile_rules": self.transpile_rules,
            "transpile_warnings": self.transpile_warnings,
        }


class TargetExecutor:
    """连接目标数据库，执行 SQL 列表并记录结果。"""

    def __init__(self, target: TargetDatabase) -> None:
        """初始化执行器。

        Args:
            target: 目标数据库定义。
        """
        self._target = target
        self._connector: Optional[DBConnector] = None

    @property
    def target(self) -> TargetDatabase:
        """获取目标数据库定义。"""
        return self._target

    def connect(self) -> None:
        """通过 ConnectorFactory 创建连接器并建立连接。

        Raises:
            Exception: 连接失败时传播底层异常。
        """
        self._connector = ConnectorFactory.create(self._target.db_type)
        self._connector.connect()
        logger.info("已连接目标数据库: %s (%s)", self._target.name, self._target.db_type)

    def execute_one(self, sql: str, metadata: Dict[str, Any]) -> SQLExecutionResult:
        """执行单条 SQL 并返回执行结果。

        Args:
            sql: 要执行的 SQL 字符串。
            metadata: 元数据字典，包含以下可选键：
                - file: 输出文件相对路径
                - seed_file: 原始种子文件
                - mutation_strategies: 变异策略列表
                - transpile_rules: 转译规则列表
                - transpile_warnings: 转译警告列表

        Returns:
            SQLExecutionResult 执行结果。
        """
        assert self._connector is not None, "请先调用 connect() 建立连接"

        file_path = metadata.get("file", "")
        seed_file = metadata.get("seed_file", "")
        mutation_strategies = metadata.get("mutation_strategies", [])
        transpile_rules = metadata.get("transpile_rules", [])
        transpile_warnings = metadata.get("transpile_warnings", [])

        t0 = time.perf_counter()
        try:
            rows = self._connector.execute_query(sql)
            elapsed = (time.perf_counter() - t0) * 1000
            return SQLExecutionResult(
                file=file_path,
                seed_file=seed_file,
                executed_sql=sql,
                status="ok",
                row_count=len(rows),
                rows=rows_to_jsonable(rows),
                error=None,
                elapsed_ms=round(elapsed, 2),
                mutation_strategies=mutation_strategies,
                transpile_rules=transpile_rules,
                transpile_warnings=transpile_warnings,
            )
        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000
            logger.debug(
                "%s 执行失败 [%s]: %s | SQL: %s",
                file_path, self._target.name, e, sql[:200],
            )
            return SQLExecutionResult(
                file=file_path,
                seed_file=seed_file,
                executed_sql=sql,
                status="error",
                row_count=0,
                rows=[],
                error=str(e),
                elapsed_ms=round(elapsed, 2),
                mutation_strategies=mutation_strategies,
                transpile_rules=transpile_rules,
                transpile_warnings=transpile_warnings,
            )

    def close(self) -> None:
        """关闭数据库连接。"""
        if self._connector is not None:
            try:
                self._connector.close()
                logger.info("已关闭连接: %s", self._target.name)
            except Exception as e:
                logger.warning("关闭连接失败 [%s]: %s", self._target.name, e)
            self._connector = None
