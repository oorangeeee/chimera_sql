"""种子 SQL 模板引擎基类。"""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ColumnMeta:
    """列元数据。"""

    name: str
    type: str  # "INTEGER", "TEXT", "REAL", "DATE", "TIMESTAMP", "VARCHAR(50)", ...
    nullable: bool
    table: str


@dataclass
class SchemaMetadata:
    """通过反射 SQLite 连接获得的表结构元数据。"""

    tables: Dict[str, List[ColumnMeta]]
    foreign_keys: List[Tuple[str, str, str, str]]  # (table, column, ref_table, ref_column)

    def columns_of_type(self, type_pattern: str) -> List[ColumnMeta]:
        """返回类型名包含 type_pattern 的所有列。"""
        pat = type_pattern.upper()
        return [
            col
            for cols in self.tables.values()
            for col in cols
            if pat in col.type.upper()
        ]

    def tables_with_column_type(self, type_pattern: str) -> List[str]:
        """返回拥有指定类型列的表名列表。"""
        pat = type_pattern.upper()
        return list({
            col.table
            for cols in self.tables.values()
            for col in cols
            if pat in col.type.upper()
        })

    def nullable_columns(self, table: Optional[str] = None) -> List[ColumnMeta]:
        """返回 nullable 列。"""
        if table:
            return [c for c in self.tables.get(table, []) if c.nullable]
        return [
            c for cols in self.tables.values() for c in cols if c.nullable
        ]

    def numeric_columns(self) -> List[ColumnMeta]:
        """返回数值类型列。"""
        return self.columns_of_type("INT") + self.columns_of_type("REAL") + self.columns_of_type("DECIMAL")

    def string_columns(self) -> List[ColumnMeta]:
        """返回字符串类型列。"""
        return self.columns_of_type("TEXT") + self.columns_of_type("VARCHAR")

    def date_columns(self) -> List[ColumnMeta]:
        """返回日期类型列。"""
        return self.columns_of_type("DATE")

    def json_columns(self) -> List[ColumnMeta]:
        """返回 JSON 列（VARCHAR/TEXT 列中名为 profile/metadata/payload 的列）。"""
        json_names = {"profile", "metadata", "payload", "metadata_json"}
        return [
            col
            for cols in self.tables.values()
            for col in cols
            if col.name in json_names
        ]

    def all_columns(self, table: str) -> List[ColumnMeta]:
        """返回指定表的所有列。"""
        return list(self.tables.get(table, []))

    def table_names(self) -> List[str]:
        """返回所有表名。"""
        return list(self.tables.keys())

    @classmethod
    def reflect(cls, conn: sqlite3.Connection) -> SchemaMetadata:
        """从 SQLite 连接反射表结构。"""
        tables: Dict[str, List[ColumnMeta]] = {}
        foreign_keys: List[Tuple[str, str, str, str]] = []

        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        table_names = [row[0] for row in cur.fetchall()]

        for tbl in table_names:
            cur.execute(f"PRAGMA table_info({tbl})")
            cols = []
            for row in cur.fetchall():
                # row: (cid, name, type, notnull, dflt_value, pk)
                col_name = row[1]
                col_type = row[2] or "TEXT"
                nullable = row[3] == 0
                cols.append(ColumnMeta(name=col_name, type=col_type, nullable=nullable, table=tbl))
            tables[tbl] = cols

            cur.execute(f"PRAGMA foreign_key_list({tbl})")
            for row in cur.fetchall():
                # row: (id, seq, table, from, to, on_update, on_delete, match)
                foreign_keys.append((tbl, row[3], row[2], row[4]))

        logger.info("SchemaMetadata 反射完成: %d 表, %d 外键", len(tables), len(foreign_keys))
        return cls(tables=tables, foreign_keys=foreign_keys)


@dataclass
class SeedSQL:
    """单条种子 SQL。"""

    sql: str
    category: str  # "dialect/cast_types" or "standard/basic_select"
    tags: List[str] = field(default_factory=list)
    description: str = ""


class SeedTemplate(ABC):
    """种子 SQL 模板基类。

    子类需实现 domain, description, generate 三个成员。
    generate() 接收 SchemaMetadata，返回 SeedSQL 列表。
    """

    @property
    @abstractmethod
    def domain(self) -> str:
        """模板域标识，如 'cast_types'。"""

    @property
    @abstractmethod
    def description(self) -> str:
        """模板中文描述。"""

    @property
    def category_prefix(self) -> str:
        """类别前缀，子类可覆盖。默认根据模块路径自动判断。"""
        return "dialect"

    @abstractmethod
    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        """生成种子 SQL 列表。"""

    @property
    def category(self) -> str:
        return f"{self.category_prefix}/{self.domain}"

    def _seed(self, sql: str, tags: Optional[List[str]] = None, desc: str = "") -> SeedSQL:
        """快捷构造 SeedSQL。"""
        return SeedSQL(sql=sql, category=self.category, tags=tags or [], description=desc)
