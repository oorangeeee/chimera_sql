"""SQL 方言兼容性检测器。

通过检测 SQL 中是否包含与目标方言不兼容的其他方言特有语法，
判断给定 SQL 是否与指定数据库方言兼容。

检测策略为「反向排除」：不判断 SQL 属于哪种方言，而是检查 SQL 中
是否存在目标方言不支持的语法特征。无方言特征的通用 SQL 视为兼容所有方言。
检测前会剥离注释和字符串字面量，避免其中的关键字导致误判。

扩展新数据库：仅需在 _INCOMPATIBLE 字典中添加一行映射，无需改动任何逻辑代码。
"""

import re
from typing import Dict, List, Pattern


# ── 方言特有语法签名 ──
# 每个列表包含该方言的独有语法特征。
# 当目标方言为 X 时，若 SQL 中存在 Y 方言的签名，则判定为不兼容。

_SQLITE_SIGNATURES: List[Pattern] = [
    # SQLite 要求 RECURSIVE 关键字，Oracle 不使用
    re.compile(r"\bWITH\s+RECURSIVE\b", re.IGNORECASE),
    # SQLite JSON 函数族（Oracle 使用 JSON_VALUE / JSON_TABLE 等）
    re.compile(r"\bjson_extract\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_array\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_object\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_each\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_tree\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_type\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_patch\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_remove\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_set\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_insert\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_replace\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_quote\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_group_array\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_group_object\s*\(", re.IGNORECASE),
    re.compile(r"\bjson_valid\s*\(", re.IGNORECASE),
    # SQLite 空值处理函数（Oracle 使用 NVL）
    re.compile(r"\bIFNULL\s*\(", re.IGNORECASE),
    # SQLite 列约束（Oracle 使用 GENERATED ALWAYS AS IDENTITY）
    re.compile(r"\bAUTOINCREMENT\b", re.IGNORECASE),
    # SQLite 行限制语法（Oracle 使用 OFFSET ... ROWS FETCH NEXT ... ROWS ONLY）
    re.compile(r"\bLIMIT\s+\d+", re.IGNORECASE),
    # SQLite UPSERT 语法
    re.compile(r"\bON\s+CONFLICT\b", re.IGNORECASE),
    # SQLite RETURNING 子句（Oracle 23c 之前不支持）
    re.compile(r"\bRETURNING\b", re.IGNORECASE),
]

_ORACLE_SIGNATURES: List[Pattern] = [
    # Oracle JSON 函数族（SQLite 使用 json_extract 等）
    re.compile(r"\bJSON_VALUE\s*\(", re.IGNORECASE),
    re.compile(r"\bJSON_QUERY\s*\(", re.IGNORECASE),
    re.compile(r"\bJSON_TABLE\s*\(", re.IGNORECASE),
    re.compile(r"\bJSON_EXISTS\s*\(", re.IGNORECASE),
    # Oracle 空值处理函数
    re.compile(r"\bNVL\s*\(", re.IGNORECASE),
    re.compile(r"\bNVL2\s*\(", re.IGNORECASE),
    # Oracle 虚拟表
    re.compile(r"\bFROM\s+DUAL\b", re.IGNORECASE),
    # Oracle 伪列
    re.compile(r"\bROWNUM\b", re.IGNORECASE),
    re.compile(r"\bROWID\b", re.IGNORECASE),
    # Oracle 行限制语法
    re.compile(r"\bFETCH\s+(?:NEXT|FIRST)\b", re.IGNORECASE),
    re.compile(r"\bOFFSET\s+\d+\s+ROWS\b", re.IGNORECASE),
    # Oracle 日期函数
    re.compile(r"\bSYSDATE\b", re.IGNORECASE),
    re.compile(r"\bSYSTIMESTAMP\b", re.IGNORECASE),
    re.compile(r"\bTO_DATE\s*\(", re.IGNORECASE),
    re.compile(r"\bTO_CHAR\s*\(", re.IGNORECASE),
    re.compile(r"\bTO_TIMESTAMP\s*\(", re.IGNORECASE),
    re.compile(r"\bADD_MONTHS\s*\(", re.IGNORECASE),
    re.compile(r"\bMONTHS_BETWEEN\s*\(", re.IGNORECASE),
    # Oracle 条件函数
    re.compile(r"\bDECODE\s*\(", re.IGNORECASE),
    # Oracle 层次查询
    re.compile(r"\bCONNECT\s+BY\b", re.IGNORECASE),
    re.compile(r"\bPRIOR\b", re.IGNORECASE),
    # Oracle MERGE 语法
    re.compile(r"\bMERGE\s+INTO\b", re.IGNORECASE),
    # Oracle 序列伪列
    re.compile(r"\bNEXTVAL\b", re.IGNORECASE),
    re.compile(r"\bCURRVAL\b", re.IGNORECASE),
]

# ── 方言 → 与之不兼容的签名集合 ──
# 例如：检测 Oracle 兼容性时，若 SQL 中存在 SQLite 特征则不兼容。
# 扩展新数据库：在此字典中添加一行即可，例如：
#   "mysql": _ORACLE_SIGNATURES + _SQLITE_SIGNATURES,
_INCOMPATIBLE: Dict[str, List[Pattern]] = {
    "oracle": _SQLITE_SIGNATURES,
    "sqlite": _ORACLE_SIGNATURES,
}

# 预编译：剥离非代码内容的正则
_RE_COMMENT_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)
_RE_COMMENT_LINE = re.compile(r"--[^\n]*")
_RE_STRING_LITERAL = re.compile(r"'(?:''|[^'])*'")


class DialectDetector:
    """SQL 方言兼容性检测器。

    检查给定 SQL 是否与目标数据库方言兼容。
    采用反向排除策略：检测 SQL 中是否包含目标方言不支持的语法特征。
    """

    @staticmethod
    def is_compatible(sql: str, dialect: str) -> bool:
        """检查单条 SQL 是否与指定方言兼容。

        Args:
            sql: SQL 语句字符串。
            dialect: 目标方言名称（如 "oracle", "sqlite"）。

        Returns:
            True 表示兼容（SQL 中未检测到不兼容的方言特征）。
            False 表示不兼容（SQL 中包含其他方言的特有语法）。
            未知方言视为兼容（返回 True）。
        """
        patterns = _INCOMPATIBLE.get(dialect.lower())
        if patterns is None:
            return True

        cleaned = _strip_non_code(sql)
        for pattern in patterns:
            if pattern.search(cleaned):
                return False
        return True

    @staticmethod
    def detect_incompatible(
        sql_map: Dict[str, str],
        dialect: str,
    ) -> List[Dict[str, str]]:
        """批量检测 SQL 文件，返回不兼容的文件列表。

        Args:
            sql_map: {文件路径: SQL 内容} 字典。
            dialect: 目标方言名称。

        Returns:
            不兼容文件列表，每项包含 file 和 reason 字段。
            若全部兼容或方言未知，返回空列表。
        """
        patterns = _INCOMPATIBLE.get(dialect.lower())
        if patterns is None:
            return []

        results: List[Dict[str, str]] = []
        for file_path, sql in sql_map.items():
            cleaned = _strip_non_code(sql)
            for p in patterns:
                match = p.search(cleaned)
                if match:
                    results.append({
                        "file": file_path,
                        "reason": f"检测到不兼容的语法: {match.group()}",
                    })
                    break  # 每个文件只报告第一个不兼容特征

        return results


def _strip_non_code(sql: str) -> str:
    """去除 SQL 中的注释和字符串字面量，只保留代码部分。

    剥离顺序：
    1. 多行注释  /* ... */
    2. 单行注释  -- ...
    3. 字符串字面量  '...'（含 '' 转义引号处理）
    """
    text = _RE_COMMENT_BLOCK.sub(" ", sql)
    text = _RE_COMMENT_LINE.sub(" ", text)
    text = _RE_STRING_LITERAL.sub("''", text)
    return text
