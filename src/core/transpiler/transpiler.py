"""SQL 方言转译器编排器。

负责编排完整的转译流程：解析 → 规则链变换 → 目标方言生成。
"""

import logging
from typing import List, Optional

import sqlglot
from sqlglot.errors import ErrorLevel

from .dialect import Dialect
from .rule_base import TranspileResult
from .rule_registry import RuleRegistry, create_default_registry
from .rules.set_op_rules import ExceptToMinusRule

logger = logging.getLogger(__name__)


class SQLTranspiler:
    """SQL 方言转译器，将 SQL 从源方言转换为目标方言。

    采用两阶段转译管线：
    1. SQLGlot 解析源 SQL 为 AST
    2. 按规则链依次变换 AST（补充 SQLGlot 未覆盖的方言差异）
    3. SQLGlot 将变换后的 AST 生成目标方言 SQL

    Args:
        registry: 规则注册表，默认使用 create_default_registry() 创建。
    """

    def __init__(self, registry: Optional[RuleRegistry] = None) -> None:
        self._registry = registry or create_default_registry()

    @property
    def registry(self) -> RuleRegistry:
        """获取当前使用的规则注册表。"""
        return self._registry

    def transpile(self, sql: str, source: Dialect, target: Dialect) -> TranspileResult:
        """将单条 SQL 从源方言转译为目标方言。

        Args:
            sql: 源 SQL 字符串。
            source: 源方言。
            target: 目标方言。

        Returns:
            TranspileResult 包含转译后的 SQL、应用的规则列表和警告。

        Raises:
            sqlglot.errors.ParseError: SQL 解析失败时抛出。
        """
        sql = sql.strip()
        warnings: List[str] = []
        rules_applied: List[str] = []

        # 源和目标相同时直接返回
        if source == target:
            return TranspileResult(
                sql=sql,
                source_dialect=source,
                target_dialect=target,
            )

        # 阶段1：解析源 SQL 为 AST
        # - sql: 原始 SQL 字符串（已 strip）
        # - read=source.value: 指定“源方言”，确保按对应语法解析
        # - error_level=ErrorLevel.RAISE: 解析失败直接抛异常，便于上层感知非法 SQL
        tree = sqlglot.parse_one(sql, read=source.value, error_level=ErrorLevel.RAISE)

        # 阶段2：按规则链依次变换 AST
        rules = self._registry.get_rules(source, target)
        for rule in rules:
            try:
                tree = rule.apply(tree)
                rules_applied.append(rule.name)
            except Exception as e:
                # 单条规则异常不中断转译（模糊测试场景容错）
                msg = f"规则 {rule.name} 执行异常: {e}"
                warnings.append(msg)
                logger.warning(msg)

        # 阶段3：生成目标方言 SQL
        result_sql = tree.sql(dialect=target.value)

        # 后处理：ExceptToMinus 文本替换（若 meta 中有标记）
        if tree.meta.get("except_to_minus"):
            result_sql = ExceptToMinusRule.post_process(result_sql)

        return TranspileResult(
            sql=result_sql,
            source_dialect=source,
            target_dialect=target,
            rules_applied=rules_applied,
            warnings=warnings,
        )

    def transpile_batch(
        self, sqls: List[str], source: Dialect, target: Dialect
    ) -> List[TranspileResult]:
        """批量转译 SQL 列表。

        单条转译失败时不中断批次，返回原 SQL 并附带警告信息。

        Args:
            sqls: 源 SQL 字符串列表。
            source: 源方言。
            target: 目标方言。

        Returns:
            TranspileResult 列表，与输入 SQL 一一对应。
        """
        results: List[TranspileResult] = []
        for sql in sqls:
            try:
                result = self.transpile(sql, source, target)
            except Exception as e:
                # 单条失败返回原 SQL + 警告
                msg = f"转译失败: {e}"
                logger.warning("SQL 转译失败: %s | SQL: %s", e, sql[:100])
                result = TranspileResult(
                    sql=sql.strip(),
                    source_dialect=source,
                    target_dialect=target,
                    warnings=[msg],
                )
            results.append(result)
        return results
