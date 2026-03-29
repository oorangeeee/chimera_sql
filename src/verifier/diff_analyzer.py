"""验证失败模式分析器。

将验证失败的 SQL 按归一化模式分组，识别系统性转译缺口，
并按失败数量排序，指导转译规则优先级。
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

from .comparator import ComparisonDetail, ComparisonVerdict


@dataclass
class RuleSuggestion:
    """基于失败分析的建议转译规则。"""

    category: str
    failure_count: int
    example_sql: str
    example_diff_type: str
    suggested_rule_name: str


class DiffAnalyzer:
    """分析验证失败，识别系统性转译缺口。

    输出按失败数量降序排列的规则建议列表。
    """

    # SQL 字面量匹配模式（用于归一化）
    _STRING_LITERAL = re.compile(r"'[^']*'")
    _NUMERIC_LITERAL = re.compile(r"\b\d+\.?\d*\b")
    _DATE_LITERAL = re.compile(r"DATE\s+'[^']*'")

    def analyze(self, details: List[ComparisonDetail]) -> List[RuleSuggestion]:
        """分析验证失败并生成规则建议。

        Args:
            details: VerifyRunner 生成的比较详情列表。

        Returns:
            按失败数量降序排列的 RuleSuggestion 列表。
        """
        failures = [
            d for d in details
            if d.verdict
            in (ComparisonVerdict.MISMATCH, ComparisonVerdict.TARGET_ERROR)
        ]

        if not failures:
            return []

        # 按归一化 SQL 模式分组
        pattern_groups: Dict[str, List[ComparisonDetail]] = defaultdict(list)
        for d in failures:
            pattern = self._extract_pattern(d.source_sql)
            pattern_groups[pattern].append(d)

        # 按失败数量排序并生成建议
        suggestions: List[RuleSuggestion] = []
        for pattern, group in sorted(
            pattern_groups.items(), key=lambda x: len(x[1]), reverse=True
        ):
            example = group[0]
            category = self._categorize_failure(example)
            suggestions.append(
                RuleSuggestion(
                    category=category,
                    failure_count=len(group),
                    example_sql=example.source_sql[:200],
                    example_diff_type=example.diff_type,
                    suggested_rule_name=self._suggest_rule_name(category),
                )
            )

        return suggestions

    def _extract_pattern(self, sql: str) -> str:
        """将 SQL 归一化为模式（替换字面量为占位符）。

        例如: "WHERE age > 25" → "WHERE age > ?"
        """
        normalized = sql.strip()
        # 替换 DATE 字面量
        normalized = self._DATE_LITERAL.sub("DATE ?", normalized)
        # 替换字符串字面量
        normalized = self._STRING_LITERAL.sub("?", normalized)
        # 替换数字字面量（但不替换列名中的数字如 t_users）
        # 只替换单独出现的数字
        normalized = self._NUMERIC_LITERAL.sub("?", normalized)
        # 合并多个连续占位符
        normalized = re.sub(r"(\?)(\s*\?)+", r"\1", normalized)
        return normalized

    def _categorize_failure(self, detail: ComparisonDetail) -> str:
        """根据比较详情推断失败类别。"""
        sql = detail.source_sql.upper()
        diff = detail.diff_type

        # 转译阶段就失败
        if diff == "transpile_error":
            return self._categorize_by_sql(sql)

        # 执行阶段失败
        if detail.target_error:
            error_upper = detail.target_error.upper()
            if "ORA-00904" in error_upper or "NO SUCH COLUMN" in error_upper:
                return "column_not_found"
            if "ORA-00942" in error_upper or "NO SUCH TABLE" in error_upper:
                return "table_not_found"
            if "ORA-00933" in error_upper or "SYNTAX ERROR" in error_upper:
                return "syntax_error"
            if "ORA-00900" in error_upper:
                return "invalid_sql"
            if "ORA-32040" in error_upper:
                return "recursive_cte"
            if "ORA-22818" in error_upper:
                return "subquery_in_group_by"
            return "execution_error"

        # 语义不匹配
        if diff == "row_count_mismatch":
            return "row_count_mismatch"
        if diff == "order_difference":
            return "order_difference"
        if diff == "value_mismatch":
            return self._categorize_by_sql(sql)

        return "unknown"

    def _categorize_by_sql(self, sql: str) -> str:
        """根据 SQL 内容推断涉及的特性类别。"""
        if "GROUP_CONCAT" in sql or "LISTAGG" in sql:
            return "string_aggregation"
        if "LEAD(" in sql or "LAG(" in sql:
            return "lead_lag_function"
        if "NTILE(" in sql:
            return "ntile_function"
        if "FIRST_VALUE(" in sql or "LAST_VALUE(" in sql:
            return "first_last_value"
        if "PERCENT_RANK" in sql:
            return "percent_rank"
        if "ROWS BETWEEN" in sql or "RANGE BETWEEN" in sql:
            return "window_frame"
        if "ROUND(" in sql or "TRUNC(" in sql:
            return "numeric_function"
        if "POWER(" in sql or "SQRT(" in sql:
            return "math_function"
        if "MOD(" in sql:
            return "mod_function"
        if "ABS(" in sql:
            return "abs_function"
        if "INSTR(" in sql:
            return "string_function"
        if "REPLACE(" in sql:
            return "string_function"
        if "SUBSTR(" in sql:
            return "string_function"
        if "DATE " in sql or "CAST" in sql:
            return "date_type"
        if "CROSS JOIN" in sql:
            return "cross_join"
        if "WITH RECURSIVE" in sql:
            return "recursive_cte"
        if "JSON_EXTRACT" in sql or "JSON_VALUE" in sql:
            return "json_function"
        return "general"

    @staticmethod
    def _suggest_rule_name(category: str) -> str:
        """根据失败类别建议转译规则名称。"""
        mapping = {
            "string_aggregation": "GroupConcatToListaggRule",
            "lead_lag_function": "LeadLagDefaultRule",
            "ntile_function": "NtileCompatibilityRule",
            "first_last_value": "FirstLastValueRule",
            "percent_rank": "PercentRankRule",
            "window_frame": "WindowFrameRule",
            "numeric_function": "NumericFunctionRule",
            "math_function": "MathFunctionRule",
            "mod_function": "ModFunctionRule",
            "abs_function": "AbsFunctionRule",
            "string_function": "StringFunctionRule",
            "date_type": "DateTypeRule",
            "cross_join": "CrossJoinRule",
            "recursive_cte": "RecursiveCTERule",
            "json_function": "JsonFunctionRule",
            "column_not_found": "ColumnResolutionRule",
            "syntax_error": "SyntaxCompatibilityRule",
            "execution_error": "ExecutionErrorRule",
            "row_count_mismatch": "RowCountRule",
            "order_difference": "OrderStabilityRule",
        }
        return mapping.get(category, f"Fix{category.title().replace('_', '')}Rule")
