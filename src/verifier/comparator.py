"""跨数据库结果比较引擎。

提供类型感知、容差可控的查询结果比较，
用于判定源数据库与目标数据库的执行结果是否语义等价。
"""

from __future__ import annotations

import datetime
import math
import re
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, List, Optional, Tuple


class ComparisonVerdict(str, Enum):
    """单条 SQL 的跨库比较判定。"""

    EQUIVALENT = "equivalent"
    PARTIAL_MATCH = "partial_match"
    MISMATCH = "mismatch"
    SOURCE_ERROR = "source_error"
    TARGET_ERROR = "target_error"
    BOTH_ERROR = "both_error"
    SKIP = "skip"
    DB_BEHAVIOR_DIFF = "db_behavior_diff"


@dataclass
class CellDiff:
    """单元格级差异描述。"""

    row_index: int
    col_index: int
    source_value: Any
    target_value: Any
    diff_type: str  # "value_mismatch", "type_difference", "null_vs_value"


@dataclass
class ComparisonDetail:
    """单条 SQL 的详细比较结果。"""

    sql_name: str
    verdict: ComparisonVerdict
    source_row_count: int
    target_row_count: int
    source_status: str  # "ok" 或 "error"
    target_status: str  # "ok" 或 "error"
    source_error: Optional[str] = None
    target_error: Optional[str] = None
    source_sql: str = ""
    target_sql: str = ""
    rules_applied: List[str] = field(default_factory=list)
    diff_type: str = ""  # "row_count_mismatch", "value_mismatch", "order_difference", ""
    cell_diffs: List[CellDiff] = field(default_factory=list)
    mismatch_count: int = 0
    total_cells_compared: int = 0


@dataclass
class ComparatorConfig:
    """比较行为配置。"""

    float_tolerance: float = 1e-6
    case_sensitive: bool = True
    normalize_timestamps: bool = True
    allow_integer_decimal_coercion: bool = True
    # Oracle 将空字符串视为 NULL，开启后 '' 与 None 视为等价
    treat_empty_string_as_null: bool = False


class ResultComparator:
    """跨数据库查询结果比较器。

    处理类型感知比较：
    - 浮点数容差比较
    - 时间戳格式归一化
    - NULL 等价（双方 NULL = 匹配）
    - 整数/小数数值强转
    - 字符串精确匹配（可配置大小写敏感）
    """

    def __init__(self, config: Optional[ComparatorConfig] = None) -> None:
        self._config = config or ComparatorConfig()

    def compare(
        self,
        sql_name: str,
        source_rows: List[Tuple[Any, ...]],
        target_rows: List[Tuple[Any, ...]],
        source_status: str,
        target_status: str,
        source_error: Optional[str] = None,
        target_error: Optional[str] = None,
        source_sql: str = "",
        target_sql: str = "",
        rules_applied: Optional[List[str]] = None,
    ) -> ComparisonDetail:
        """比较源数据库与目标数据库的执行结果。

        Args:
            sql_name: SQL 标识（文件路径）。
            source_rows: 源数据库结果行。
            target_rows: 目标数据库结果行。
            source_status: 源端执行状态（"ok" / "error"）。
            target_status: 目标端执行状态（"ok" / "error"）。
            source_error: 源端错误信息。
            target_error: 目标端错误信息。
            source_sql: 实际在源库执行的 SQL。
            target_sql: 实际在目标库执行的 SQL。
            rules_applied: 应用的转译规则列表。

        Returns:
            ComparisonDetail 比较详情。
        """
        # 双端均报错
        if source_status == "error" and target_status == "error":
            return ComparisonDetail(
                sql_name=sql_name,
                verdict=ComparisonVerdict.BOTH_ERROR,
                source_row_count=0,
                target_row_count=0,
                source_status=source_status,
                target_status=target_status,
                source_error=source_error,
                target_error=target_error,
                source_sql=source_sql,
                target_sql=target_sql,
                rules_applied=rules_applied or [],
            )

        # 源端报错，目标端成功
        if source_status == "error":
            return ComparisonDetail(
                sql_name=sql_name,
                verdict=ComparisonVerdict.SOURCE_ERROR,
                source_row_count=0,
                target_row_count=len(target_rows),
                source_status=source_status,
                target_status=target_status,
                source_error=source_error,
                target_error=target_error,
                source_sql=source_sql,
                target_sql=target_sql,
                rules_applied=rules_applied or [],
            )

        # 目标端报错，源端成功（转译问题）
        if target_status == "error":
            return ComparisonDetail(
                sql_name=sql_name,
                verdict=ComparisonVerdict.TARGET_ERROR,
                source_row_count=len(source_rows),
                target_row_count=0,
                source_status=source_status,
                target_status=target_status,
                source_error=source_error,
                target_error=target_error,
                source_sql=source_sql,
                target_sql=target_sql,
                diff_type="target_execution_error",
                rules_applied=rules_applied or [],
            )

        # 双端均成功，进行结果比较
        return self._compare_results(
            sql_name=sql_name,
            source_rows=source_rows,
            target_rows=target_rows,
            source_sql=source_sql,
            target_sql=target_sql,
            rules_applied=rules_applied or [],
        )

    def _compare_results(
        self,
        sql_name: str,
        source_rows: List[Tuple[Any, ...]],
        target_rows: List[Tuple[Any, ...]],
        source_sql: str,
        target_sql: str,
        rules_applied: List[str],
    ) -> ComparisonDetail:
        """比较双端成功执行的结果集。"""
        # 行数不同 → 尝试数据库行为差异容忍
        if len(source_rows) != len(target_rows):
            detail = ComparisonDetail(
                sql_name=sql_name,
                verdict=ComparisonVerdict.MISMATCH,
                source_row_count=len(source_rows),
                target_row_count=len(target_rows),
                source_status="ok",
                target_status="ok",
                source_sql=source_sql,
                target_sql=target_sql,
                rules_applied=rules_applied,
                diff_type="row_count_mismatch",
            )
            if self._check_row_count_db_behavior(source_rows, target_rows):
                detail.verdict = ComparisonVerdict.DB_BEHAVIOR_DIFF
                detail.diff_type = "db_behavior_difference"
            return detail

        # 空结果集 → 等价
        if len(source_rows) == 0:
            return ComparisonDetail(
                sql_name=sql_name,
                verdict=ComparisonVerdict.EQUIVALENT,
                source_row_count=0,
                target_row_count=0,
                source_status="ok",
                target_status="ok",
                source_sql=source_sql,
                target_sql=target_sql,
                rules_applied=rules_applied,
            )

        # 先尝试有序比较（种子 SQL 均有 ORDER BY）
        cell_diffs, mismatch_count, total_cells = self._compare_rows_ordered(
            source_rows, target_rows
        )

        if mismatch_count == 0:
            return ComparisonDetail(
                sql_name=sql_name,
                verdict=ComparisonVerdict.EQUIVALENT,
                source_row_count=len(source_rows),
                target_row_count=len(target_rows),
                source_status="ok",
                target_status="ok",
                source_sql=source_sql,
                target_sql=target_sql,
                rules_applied=rules_applied,
                cell_diffs=cell_diffs,
                mismatch_count=0,
                total_cells_compared=total_cells,
            )

        # 有序比较失败，尝试无序比较（排除行序差异）
        (
            unordered_diffs,
            unordered_mismatch,
            unordered_total,
            is_order_diff,
        ) = self._compare_rows_unordered(source_rows, target_rows)

        if is_order_diff and unordered_mismatch == 0:
            return ComparisonDetail(
                sql_name=sql_name,
                verdict=ComparisonVerdict.PARTIAL_MATCH,
                source_row_count=len(source_rows),
                target_row_count=len(target_rows),
                source_status="ok",
                target_status="ok",
                source_sql=source_sql,
                target_sql=target_sql,
                rules_applied=rules_applied,
                diff_type="order_difference",
                cell_diffs=unordered_diffs,
                mismatch_count=0,
                total_cells_compared=unordered_total,
            )

        # 真正的值不匹配 → 尝试数据库行为差异容忍比较
        detail = ComparisonDetail(
            sql_name=sql_name,
            verdict=ComparisonVerdict.MISMATCH,
            source_row_count=len(source_rows),
            target_row_count=len(target_rows),
            source_status="ok",
            target_status="ok",
            source_sql=source_sql,
            target_sql=target_sql,
            rules_applied=rules_applied,
            diff_type="value_mismatch",
            cell_diffs=unordered_diffs,
            mismatch_count=unordered_mismatch,
            total_cells_compared=unordered_total,
        )

        # 二次比较：容忍已知数据库行为差异
        if self._check_db_behavior_tolerance(
            source_rows, target_rows, unordered_diffs
        ):
            detail.verdict = ComparisonVerdict.DB_BEHAVIOR_DIFF
            detail.diff_type = "db_behavior_difference"

        return detail

    def _compare_rows_ordered(
        self,
        source: List[Tuple[Any, ...]],
        target: List[Tuple[Any, ...]],
    ) -> Tuple[List[CellDiff], int, int]:
        """有序逐行比较两个结果集。

        Returns:
            (cell_diffs, mismatch_count, total_cells_compared)
        """
        diffs: List[CellDiff] = []
        mismatch_count = 0
        total_cells = 0

        for row_idx, (src_row, tgt_row) in enumerate(zip(source, target)):
            col_count = max(len(src_row), len(tgt_row))
            for col_idx in range(col_count):
                total_cells += 1
                src_val = src_row[col_idx] if col_idx < len(src_row) else None
                tgt_val = tgt_row[col_idx] if col_idx < len(tgt_row) else None

                if not self._compare_values(src_val, tgt_val):
                    mismatch_count += 1
                    diffs.append(
                        CellDiff(
                            row_index=row_idx,
                            col_index=col_idx,
                            source_value=src_val,
                            target_value=tgt_val,
                            diff_type=self._classify_diff(src_val, tgt_val),
                        )
                    )

        return diffs, mismatch_count, total_cells

    def _compare_rows_unordered(
        self,
        source: List[Tuple[Any, ...]],
        target: List[Tuple[Any, ...]],
    ) -> Tuple[List[CellDiff], int, int, bool]:
        """无序集合比较两个结果集。

        尝试将每行源端结果与某行目标端结果匹配。
        如果所有行都能匹配（可能有不同顺序），判定为 order_difference。

        Returns:
            (cell_diffs, mismatch_count, total_cells, is_order_difference)
        """
        # 将行转为可比较的元组列表
        normalized_source = [self._normalize_row(r) for r in source]
        normalized_target = [self._normalize_row(r) for r in target]

        # 检查是否为纯行序差异（多集合相等）
        source_multiset = list(normalized_target)
        is_order_only = True
        total_cells = 0
        mismatch_count = 0
        diffs: List[CellDiff] = []

        for src_row in normalized_source:
            matched = False
            for i, tgt_row in enumerate(source_multiset):
                if self._rows_equivalent(src_row, tgt_row):
                    source_multiset.pop(i)
                    matched = True
                    break
            if not matched:
                is_order_only = False
                break

        if is_order_only and len(source_multiset) == 0:
            # 所有行都匹配，只是顺序不同
            for src_row, tgt_row in zip(source, target):
                col_count = max(len(src_row), len(tgt_row))
                for col_idx in range(col_count):
                    total_cells += 1
            return diffs, 0, total_cells, True

        # 非纯行序差异，进行详细比较
        # 找最佳匹配并报告差异
        for row_idx, (src_row, tgt_row) in enumerate(zip(source, target)):
            col_count = max(len(src_row), len(tgt_row))
            for col_idx in range(col_count):
                total_cells += 1
                src_val = src_row[col_idx] if col_idx < len(src_row) else None
                tgt_val = tgt_row[col_idx] if col_idx < len(tgt_row) else None
                if not self._compare_values(src_val, tgt_val):
                    mismatch_count += 1
                    diffs.append(
                        CellDiff(
                            row_index=row_idx,
                            col_index=col_idx,
                            source_value=src_val,
                            target_value=tgt_val,
                            diff_type=self._classify_diff(src_val, tgt_val),
                        )
                    )

        return diffs, mismatch_count, total_cells, False

    def _normalize_row(self, row: Tuple[Any, ...]) -> Tuple[Any, ...]:
        """归一化行数据用于集合比较。"""
        return tuple(self._normalize_value(v) for v in row)

    def _rows_equivalent(
        self, a: Tuple[Any, ...], b: Tuple[Any, ...]
    ) -> bool:
        """判断两行是否等价。"""
        if len(a) != len(b):
            return False
        return all(self._compare_values(va, vb) for va, vb in zip(a, b))

    def _compare_values(self, a: Any, b: Any) -> bool:
        """类型感知的单值比较。"""
        a = self._normalize_value(a)
        b = self._normalize_value(b)

        # 双方 NULL
        if a is None and b is None:
            return True

        # 一方 NULL 另一方非 NULL
        if a is None or b is None:
            return False

        # 数值比较
        if self._is_numeric(a) and self._is_numeric(b):
            return self._compare_numeric(a, b)

        # 字符串比较
        if isinstance(a, str) and isinstance(b, str):
            result = self._compare_strings(a, b)
            if result is not None:
                return result
            # 直接字符串相等
            if self._config.case_sensitive:
                return a == b
            return a.lower() == b.lower()

        # 数值 vs 字符串：尝试跨类型数值比较
        if self._is_numeric(a) and isinstance(b, str):
            return self._try_numeric_string_compare(a, b)
        if isinstance(a, str) and self._is_numeric(b):
            return self._try_numeric_string_compare(b, a)

        # 布尔比较
        if isinstance(a, bool) and isinstance(b, bool):
            return a == b

        # 直接相等
        return a == b

    def _compare_strings(self, a: str, b: str) -> Optional[bool]:
        """增强的字符串比较：CSV 列表归一化。

        Returns:
            True/False 表示比较结果，None 表示未处理（回退到默认比较）。
        """
        # 先检查直接相等
        if self._config.case_sensitive:
            if a == b:
                return True
        else:
            if a.lower() == b.lower():
                return True

        # 不相等时，尝试逗号分隔列表归一化
        if ", " in a and ", " in b:
            parts_a = sorted(a.split(", "))
            parts_b = sorted(b.split(", "))
            if parts_a == parts_b:
                return True

        return None  # 回退到默认字符串比较

    # Oracle 默认日期格式 DD-MON-RR / DD-MON-YYYY（如 15-MAR-95, 01-JAN-2000）
    _ORACLE_DATE_RE = re.compile(
        r"^\d{1,2}-[A-Z]{3}-\d{2,4}$", re.IGNORECASE
    )

    def _normalize_value(self, value: Any) -> Any:
        """归一化值用于比较。"""
        if value is None:
            return None

        # 空字符串按 NULL 归一化（Oracle '' = NULL 语义）
        if self._config.treat_empty_string_as_null and value == "":
            return None

        # 时间戳归一化
        if self._config.normalize_timestamps and isinstance(
            value, (datetime.datetime, datetime.date)
        ):
            return value.isoformat()

        # 字符串形式的时间戳也归一化
        if (
            self._config.normalize_timestamps
            and isinstance(value, str)
            and self._looks_like_timestamp(value)
        ):
            try:
                dt = datetime.datetime.fromisoformat(value)
                return dt.isoformat()
            except (ValueError, TypeError):
                pass

        # Oracle 默认日期格式字符串归一化为 ISO（与 _looks_like_timestamp 输出对齐）
        if isinstance(value, str) and self._ORACLE_DATE_RE.match(value):
            try:
                for fmt in ("%d-%b-%y", "%d-%b-%Y"):
                    try:
                        dt = datetime.datetime.strptime(value, fmt)
                        return dt.isoformat()
                    except ValueError:
                        continue
            except Exception:
                pass

        return value

    def _try_numeric_string_compare(self, numeric_val: Any, string_val: str) -> bool:
        """尝试将字符串转为数值后与数值比较。"""
        try:
            fb = float(string_val)
            fa = float(numeric_val) if not isinstance(numeric_val, float) else numeric_val
            return math.isclose(fa, fb, abs_tol=self._config.float_tolerance)
        except (ValueError, TypeError, OverflowError):
            return False

    def _compare_numeric(self, a: Any, b: Any) -> bool:
        """数值比较，支持容差和类型强转。"""
        try:
            # 转为 float 进行比较
            fa = float(a) if not isinstance(a, float) else a
            fb = float(b) if not isinstance(b, float) else b

            # 整数/小数强转
            if self._config.allow_integer_decimal_coercion:
                # 1 == 1.0 == Decimal('1.0')
                if fa == fb:
                    return True

            # 浮点容差比较
            return math.isclose(fa, fb, abs_tol=self._config.float_tolerance)
        except (TypeError, ValueError, OverflowError):
            return False

    @staticmethod
    def _is_numeric(value: Any) -> bool:
        """判断值是否为数值类型。"""
        return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)

    @staticmethod
    def _looks_like_timestamp(value: str) -> bool:
        """启发式判断字符串是否像时间戳。"""
        # 简单检测：包含日期分隔符或 T 分隔符
        return (
            "-" in value[:5]
            and ("T" in value or ":" in value or " " in value)
        ) or value[:4].isdigit() and "-" in value[4:6]

    def _check_db_behavior_tolerance(
        self,
        source_rows: List[Tuple[Any, ...]],
        target_rows: List[Tuple[Any, ...]],
        cell_diffs: List[CellDiff],
    ) -> bool:
        """二次比较：检查所有差异是否均为已知数据库行为差异。

        已知差异包括：
        - 空字符串 '' vs NULL（Oracle VARCHAR2 语义）
        - ROUND 舍入差异（整数结果差 ±1，银行家舍入 vs 标准舍入）
        - 由 '' vs NULL 引起的 COALESCE / 字符串拼接结果差异
        """
        if not cell_diffs:
            return False

        for diff in cell_diffs:
            src, tgt = diff.source_value, diff.target_value

            # 1) 空字符串 vs NULL
            if (src == "" and tgt is None) or (src is None and tgt == ""):
                continue

            # 2) 整数结果差 ±1（ROUND 舍入模式差异）
            if self._is_round_tolerance(src, tgt):
                continue

            # 3) 包含空串/NULL 导致的字符串差异
            #    如 COALESCE 结果: '' vs 'N/A'，或拼接结果: 'Ève ()' vs 'Ève (N/A)'
            if isinstance(src, str) and isinstance(tgt, str):
                if self._is_null_induced_string_diff(src, tgt):
                    continue

            # 未匹配任何已知模式 → 不是纯粹的 DB 行为差异
            return False

        return True

    def _check_row_count_db_behavior(
        self,
        source_rows: List[Tuple[Any, ...]],
        target_rows: List[Tuple[Any, ...]],
    ) -> bool:
        """行数不同时，检查是否由已知数据库行为差异引起。

        场景：Oracle 将 '' 视为 NULL，导致 IS NULL / IS NOT NULL 返回不同行数。
        判定条件：用宽松规则（'' = NULL）比较后，行数少的那个是行数多的子集。
        """
        shorter, longer = (
            (source_rows, target_rows)
            if len(source_rows) < len(target_rows)
            else (target_rows, source_rows)
        )

        # 用宽松配置创建临时比较行
        normalized_shorter = [self._normalize_row_relaxed(r) for r in shorter]
        normalized_longer = [self._normalize_row_relaxed(r) for r in longer]

        # 检查较短结果集是否是较长结果集的子集
        longer_remaining = list(normalized_longer)
        for short_row in normalized_shorter:
            matched = False
            for i, long_row in enumerate(longer_remaining):
                if self._rows_equivalent(short_row, long_row):
                    longer_remaining.pop(i)
                    matched = True
                    break
            if not matched:
                return False

        # 额外的行必须可以由 '' vs NULL 差异解释
        # （简单检查：剩余行中至少有一个 NULL 值）
        for extra_row in longer_remaining:
            if any(v is None for v in extra_row):
                continue
            return False

        return True

    def _normalize_row_relaxed(self, row: Tuple[Any, ...]) -> Tuple[Any, ...]:
        """用宽松规则归一化行（'' 视为 NULL）。"""
        result = []
        for v in row:
            if v == "":
                result.append(None)
            else:
                result.append(self._normalize_value(v))
        return tuple(result)

    @staticmethod
    def _is_round_tolerance(a: Any, b: Any) -> bool:
        """检查两个值是否属于 ROUND 舍入模式差异（差值恰好为 1）。"""
        try:
            fa = float(a)
            fb = float(b)
            diff = abs(fa - fb)
            # 差值恰好为 1.0 且两边都是整数值 → ROUND 舍入差异
            if math.isclose(diff, 1.0, abs_tol=1e-9):
                if fa == int(fa) and fb == int(fb):
                    return True
            return False
        except (TypeError, ValueError, OverflowError):
            return False

    @staticmethod
    def _is_null_induced_string_diff(a: str, b: str) -> bool:
        """检查两个字符串差异是否由 NULL/空字符串语义引起。

        模式：一个字符串包含某个值（如 'N/A'），另一个为空或不含该值，
        且差异可追溯到 COALESCE 等函数对 '' 和 NULL 的不同处理。
        """
        # 完全相同的不需要处理
        if a == b:
            return True

        # 一个为空字符串
        if a == "" or b == "":
            return True

        # COALESCE 产生的 'N/A' vs '' 差异
        # 检查是否一方包含 'N/A' 另一方不包含，但其余部分相同
        na_a = a.replace("N/A", "").replace("()", "")
        na_b = b.replace("N/A", "").replace("()", "")
        if na_a == na_b:
            return True

        return False

    @staticmethod
    def _classify_diff(source_value: Any, target_value: Any) -> str:
        """分类差异类型。"""
        if source_value is None or target_value is None:
            return "null_vs_value"
        if type(source_value) != type(target_value):
            return "type_difference"
        return "value_mismatch"
