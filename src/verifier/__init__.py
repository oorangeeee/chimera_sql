"""转译正确率验证模块。

提供跨数据库结果比较、验证运行器、差异分析等能力，
用于度量转译引擎的正确率（执行通过率 + 语义等价率）。
"""

from .comparator import (
    CellDiff,
    ComparatorConfig,
    ComparisonDetail,
    ComparisonVerdict,
    ResultComparator,
)
from .runner import VerifyMetrics, VerifyReport, VerifyRunner

__all__ = [
    "CellDiff",
    "ComparatorConfig",
    "ComparisonDetail",
    "ComparisonVerdict",
    "ResultComparator",
    "VerifyMetrics",
    "VerifyReport",
    "VerifyRunner",
]
