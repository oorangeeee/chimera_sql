"""JSON 序列化辅助工具。

提供将数据库返回值转换为 JSON 安全类型的通用函数。
"""

import base64
from datetime import date, timedelta
from typing import Any, List
from decimal import Decimal


def to_jsonable(value: Any) -> Any:
    """将单个值转换为可 JSON 序列化的形式。

    None/基本类型原样返回，常见数据库类型做有损但语义合理的转换，
    其他未知类型降级为 str()。
    """
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, timedelta):
        total = value.total_seconds()
        return f"P{int(total // 86400)}DT{int(total % 86400 // 3600)}H{int(total % 3600 // 60)}M{total % 60:.6f}S"
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def rows_to_jsonable(rows: List[tuple]) -> List[List[Any]]:
    """将结果行列表转换为 JSON 安全的二维列表。"""
    return [[to_jsonable(v) for v in row] for row in rows]
