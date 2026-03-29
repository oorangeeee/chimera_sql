"""模糊测试分析结果数据类。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ErrorCategory:
    """错误分类统计。"""

    category: str
    count: int
    example_message: str
    example_file: str


@dataclass
class StrategyStats:
    """单个变异策略的执行统计。"""

    strategy_id: str
    total: int = 0
    success: int = 0
    error: int = 0

    @property
    def success_rate(self) -> float:
        return self.success / self.total if self.total else 0.0


@dataclass
class TranspileStats:
    """转译效果统计。"""

    total_transpiled: int = 0
    transpile_applied_rules: Dict[str, int] = field(default_factory=dict)
    transpile_warnings_count: int = 0
    success_after_transpile: int = 0
    error_after_transpile: int = 0


@dataclass
class PerformanceEntry:
    """性能分析条目。"""

    file: str
    elapsed_ms: float
    status: str


@dataclass
class SeedCoverage:
    """单条种子的执行覆盖统计。"""

    seed_file: str
    total: int = 0
    success: int = 0
    error: int = 0

    @property
    def success_rate(self) -> float:
        return self.success / self.total if self.total else 0.0


@dataclass
class AnalysisResult:
    """模糊测试分析的完整结果。"""

    # 总览
    total_executed: int = 0
    success_count: int = 0
    error_count: int = 0

    # 错误分析
    error_categories: List[ErrorCategory] = field(default_factory=list)

    # 变异策略效果
    strategy_stats: List[StrategyStats] = field(default_factory=list)

    # 转译效果
    transpile_stats: TranspileStats = field(default_factory=TranspileStats)

    # 性能分析
    total_elapsed_ms: float = 0.0
    avg_elapsed_ms: float = 0.0
    slowest_entries: List[PerformanceEntry] = field(default_factory=list)

    # 种子覆盖
    seed_coverage: List[SeedCoverage] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        return self.success_count / self.total_executed if self.total_executed else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "overview": {
                "total_executed": self.total_executed,
                "success": self.success_count,
                "error": self.error_count,
                "success_rate": round(self.success_rate, 4),
                "total_elapsed_ms": round(self.total_elapsed_ms, 1),
                "avg_elapsed_ms": round(self.avg_elapsed_ms, 2),
            },
            "error_categories": [
                {
                    "category": ec.category,
                    "count": ec.count,
                    "example_message": ec.example_message,
                    "example_file": ec.example_file,
                }
                for ec in self.error_categories
            ],
            "strategy_stats": [
                {
                    "strategy_id": ss.strategy_id,
                    "total": ss.total,
                    "success": ss.success,
                    "error": ss.error,
                    "success_rate": round(ss.success_rate, 4),
                }
                for ss in self.strategy_stats
            ],
            "transpile_stats": {
                "total_transpiled": self.transpile_stats.total_transpiled,
                "rules_applied": self.transpile_stats.transpile_applied_rules,
                "warnings_count": self.transpile_stats.transpile_warnings_count,
                "success_after_transpile": self.transpile_stats.success_after_transpile,
                "error_after_transpile": self.transpile_stats.error_after_transpile,
            },
            "performance": {
                "slowest": [
                    {
                        "file": pe.file,
                        "elapsed_ms": round(pe.elapsed_ms, 2),
                        "status": pe.status,
                    }
                    for pe in self.slowest_entries
                ],
            },
            "seed_coverage": [
                {
                    "seed_file": sc.seed_file,
                    "total": sc.total,
                    "success": sc.success,
                    "error": sc.error,
                    "success_rate": round(sc.success_rate, 4),
                }
                for sc in self.seed_coverage
            ],
        }
