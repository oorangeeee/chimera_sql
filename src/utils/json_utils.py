"""JSON 序列化辅助工具。

提供将数据库返回值转换为 JSON 安全类型的通用函数。
"""

from typing import Any, List


def to_jsonable(value: Any) -> Any:
    """将单个值转换为可 JSON 序列化的形式。

    None/基本类型原样返回，datetime 对象转为 ISO 格式字符串，
    其他类型降级为 str()。
    """
    if value is None:
        return None
    if isinstance(value, (int, float, str, bool)):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def rows_to_jsonable(rows: List[tuple]) -> List[List[Any]]:
    """将结果行列表转换为 JSON 安全的二维列表。"""
    return [[to_jsonable(v) for v in row] for row in rows]
