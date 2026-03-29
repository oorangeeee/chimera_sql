"""模糊测试结果分析器。

接收 SQLExecutionResult 列表，执行多维统计分析并产出 AnalysisResult。
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from src.utils.logger import get_logger

if TYPE_CHECKING:
    from src.pipeline.executor import SQLExecutionResult

from .result import (
    AnalysisResult,
    ErrorCategory,
    PerformanceEntry,
    SeedCoverage,
    StrategyStats,
    TranspileStats,
)

logger = get_logger("analyzer")

# 错误分类的正则模式（按优先级排列）
_ERROR_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("表不存在", re.compile(r"table.*does not exist|no such table|ORA-00942", re.I)),
    ("列不存在", re.compile(r"column.*does not exist|no such column|ORA-00904", re.I)),
    ("语法错误", re.compile(r"syntax error|ORA-00900|ORA-00933", re.I)),
    ("类型不匹配", re.compile(r"type mismatch|datatype mismatch|ORA-00932", re.I)),
    ("唯一约束冲突", re.compile(
        r"unique constraint|duplicate key|ORA-00001|UNIQUE constraint failed", re.I
    )),
    ("连接错误", re.compile(r"connection|ORA-12154|ORA-12514", re.I)),
    ("权限不足", re.compile(r"permission|insufficient privileges|ORA-01031", re.I)),
    ("非空约束冲突", re.compile(
        r"cannot be null|NOT NULL constraint|ORA-01400", re.I
    )),
    ("函数不存在", re.compile(r"no such function|ORA-00904|undefined function", re.I)),
    ("其他错误", re.compile(r".*")),
]


def _classify_error(error_msg: str) -> str:
    """根据错误信息分类。"""
    for category, pattern in _ERROR_PATTERNS:
        if pattern.search(error_msg):
            return category
    return "其他错误"


class FuzzAnalyzer:
    """模糊测试结果分析器。"""

    def analyze(self, results: List[SQLExecutionResult]) -> AnalysisResult:
        """对执行结果列表进行多维度分析。

        Args:
            results: SQLExecutionResult 列表。

        Returns:
            AnalysisResult 分析结果。
        """
        if not results:
            return AnalysisResult()

        # 总览
        total = len(results)
        success_count = sum(1 for r in results if r.status == "ok")
        error_count = total - success_count
        total_elapsed = sum(r.elapsed_ms for r in results)
        avg_elapsed = total_elapsed / total

        # 错误分类
        error_categories = self._analyze_errors(results)

        # 变异策略效果
        strategy_stats = self._analyze_strategies(results)

        # 转译效果
        transpile_stats = self._analyze_transpile(results)

        # 性能分析
        slowest = self._analyze_performance(results)

        # 种子覆盖
        seed_coverage = self._analyze_seed_coverage(results)

        return AnalysisResult(
            total_executed=total,
            success_count=success_count,
            error_count=error_count,
            error_categories=error_categories,
            strategy_stats=strategy_stats,
            transpile_stats=transpile_stats,
            total_elapsed_ms=round(total_elapsed, 1),
            avg_elapsed_ms=round(avg_elapsed, 2),
            slowest_entries=slowest,
            seed_coverage=seed_coverage,
        )

    @staticmethod
    def _analyze_errors(results: List[SQLExecutionResult]) -> List[ErrorCategory]:
        """按错误类型分类统计。"""
        error_results = [r for r in results if r.status == "error" and r.error]
        if not error_results:
            return []

        # 分类计数，保留每种分类的第一个示例
        category_examples: Dict[str, ErrorCategory] = {}
        for r in error_results:
            cat = _classify_error(r.error)
            if cat in category_examples:
                category_examples[cat].count += 1
            else:
                category_examples[cat] = ErrorCategory(
                    category=cat,
                    count=1,
                    example_message=r.error[:200],
                    example_file=r.file,
                )

        return sorted(category_examples.values(), key=lambda e: e.count, reverse=True)

    @staticmethod
    def _analyze_strategies(results: List[SQLExecutionResult]) -> List[StrategyStats]:
        """统计各变异策略的成功/失败情况。"""
        strategy_map: Dict[str, StrategyStats] = {}

        for r in results:
            for sid in r.mutation_strategies:
                if sid not in strategy_map:
                    strategy_map[sid] = StrategyStats(strategy_id=sid)
                strategy_map[sid].total += 1
                if r.status == "ok":
                    strategy_map[sid].success += 1
                else:
                    strategy_map[sid].error += 1

        return sorted(strategy_map.values(), key=lambda s: s.total, reverse=True)

    @staticmethod
    def _analyze_transpile(results: List[SQLExecutionResult]) -> TranspileStats:
        """统计转译效果。"""
        stats = TranspileStats()
        rules_counter: Counter = Counter()

        for r in results:
            if r.transpile_rules:
                stats.total_transpiled += 1
                for rule_name in r.transpile_rules:
                    rules_counter[rule_name] += 1
                if r.status == "ok":
                    stats.success_after_transpile += 1
                else:
                    stats.error_after_transpile += 1

            stats.transpile_warnings_count += len(r.transpile_warnings)

        stats.transpile_applied_rules = dict(rules_counter.most_common())
        return stats

    @staticmethod
    def _analyze_performance(results: List[SQLExecutionResult], top_n: int = 5) -> List[PerformanceEntry]:
        """找出耗时最慢的 N 条 SQL。"""
        sorted_results = sorted(results, key=lambda r: r.elapsed_ms, reverse=True)
        return [
            PerformanceEntry(
                file=r.file,
                elapsed_ms=r.elapsed_ms,
                status=r.status,
            )
            for r in sorted_results[:top_n]
        ]

    @staticmethod
    def _analyze_seed_coverage(results: List[SQLExecutionResult]) -> List[SeedCoverage]:
        """统计每条种子的变异执行覆盖。"""
        seed_map: Dict[str, SeedCoverage] = {}

        for r in results:
            sf = r.seed_file
            if sf not in seed_map:
                seed_map[sf] = SeedCoverage(seed_file=sf)
            seed_map[sf].total += 1
            if r.status == "ok":
                seed_map[sf].success += 1
            else:
                seed_map[sf].error += 1

        return sorted(seed_map.values(), key=lambda s: s.seed_file)
