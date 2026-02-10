"""测试数据库模式初始化器 — 在目标数据库中创建统一的测试表结构。

采用 dataclass 定义通用 schema，通过类型映射字典生成各方言 DDL，
避免使用 SQLGlot 转译 DDL（DDL 转译存在已知兼容性问题）。
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from src.connector.base import DBConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ────────────────────────────────────────────────────────
# 通用类型 → 方言类型映射
# ────────────────────────────────────────────────────────
_TYPE_MAP: Dict[str, Dict[str, str]] = {
    "oracle": {
        "INTEGER": "NUMBER(10)",
        "VARCHAR": "VARCHAR2",
        "DECIMAL": "NUMBER",
        "TIMESTAMP": "TIMESTAMP",
    },
    "sqlite": {
        "INTEGER": "INTEGER",
        "VARCHAR": "TEXT",
        "DECIMAL": "REAL",
        "TIMESTAMP": "TEXT",
    },
}


# ────────────────────────────────────────────────────────
# 列 / 表 元数据定义
# ────────────────────────────────────────────────────────
@dataclass
class ColumnDef:
    """单列定义。"""

    name: str
    # 通用类型字符串，如 "INTEGER", "VARCHAR(50)", "DECIMAL(10,2)"
    generic_type: str
    nullable: bool = True
    default: Optional[str] = None
    primary_key: bool = False


@dataclass
class ForeignKeyDef:
    """外键约束定义。"""

    column: str
    ref_table: str
    ref_column: str


@dataclass
class IndexDef:
    """索引定义。"""

    name: str
    columns: List[str]


@dataclass
class TableDef:
    """单张表的完整定义。"""

    name: str
    columns: List[ColumnDef]
    foreign_keys: List[ForeignKeyDef] = field(default_factory=list)
    indexes: List[IndexDef] = field(default_factory=list)


# ────────────────────────────────────────────────────────
# 五张测试表定义
# ────────────────────────────────────────────────────────
TABLES: List[TableDef] = [
    # ---------- t_users ----------
    TableDef(
        name="t_users",
        columns=[
            ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
            ColumnDef("username", "VARCHAR(50)", nullable=False),
            ColumnDef("email", "VARCHAR(100)"),
            ColumnDef("age", "INTEGER"),
            ColumnDef("score", "DECIMAL(10,2)"),
            ColumnDef("active", "INTEGER", nullable=False, default="1"),
            ColumnDef("created_at", "TIMESTAMP"),
        ],
    ),
    # ---------- t_products ----------
    TableDef(
        name="t_products",
        columns=[
            ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
            ColumnDef("name", "VARCHAR(100)", nullable=False),
            ColumnDef("category", "VARCHAR(50)"),
            ColumnDef("price", "DECIMAL(10,2)", nullable=False),
            ColumnDef("stock", "INTEGER"),
            ColumnDef("discontinued", "INTEGER", nullable=False, default="0"),
        ],
    ),
    # ---------- t_orders ----------
    TableDef(
        name="t_orders",
        columns=[
            ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
            ColumnDef("user_id", "INTEGER", nullable=False),
            ColumnDef("product_id", "INTEGER", nullable=False),
            ColumnDef("quantity", "INTEGER", nullable=False),
            ColumnDef("total_price", "DECIMAL(10,2)", nullable=False),
            ColumnDef("order_date", "TIMESTAMP"),
            ColumnDef("status", "VARCHAR(20)"),
        ],
        foreign_keys=[
            ForeignKeyDef("user_id", "t_users", "id"),
            ForeignKeyDef("product_id", "t_products", "id"),
        ],
        indexes=[
            IndexDef("idx_orders_user", ["user_id"]),
            IndexDef("idx_orders_product", ["product_id"]),
            IndexDef("idx_orders_status", ["status"]),
        ],
    ),
    # ---------- t_metrics ----------
    TableDef(
        name="t_metrics",
        columns=[
            ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
            ColumnDef("user_id", "INTEGER", nullable=False),
            ColumnDef("metric_name", "VARCHAR(50)", nullable=False),
            ColumnDef("metric_value", "DECIMAL(15,5)"),
            ColumnDef("recorded_at", "TIMESTAMP"),
        ],
        foreign_keys=[
            ForeignKeyDef("user_id", "t_users", "id"),
        ],
    ),
    # ---------- t_tags ----------
    TableDef(
        name="t_tags",
        columns=[
            ColumnDef("id", "INTEGER", nullable=False, primary_key=True),
            ColumnDef("entity_type", "VARCHAR(20)", nullable=False),
            ColumnDef("entity_id", "INTEGER", nullable=False),
            ColumnDef("tag", "VARCHAR(50)", nullable=False),
        ],
        indexes=[
            IndexDef("idx_tags_entity", ["entity_type", "entity_id"]),
        ],
    ),
]


# ────────────────────────────────────────────────────────
# SchemaInitializer 核心类
# ────────────────────────────────────────────────────────
class SchemaInitializer:
    """在目标数据库中创建统一的测试表结构。

    根据 db_type 选择对应方言的 DDL 语句，
    执行 DROP → CREATE → INDEX 三步初始化。
    """

    def __init__(self, connector: DBConnector, db_type: str) -> None:
        self._connector = connector
        self._db_type = db_type.lower()
        if self._db_type not in _TYPE_MAP:
            raise ValueError(f"不支持的数据库类型: {db_type}")

    # ── 公开入口 ────────────────────────────────
    def initialize(self) -> None:
        """执行完整的模式初始化（DROP → CREATE → INDEX）。"""
        logger.info("[%s] 开始模式初始化 ...", self._db_type)

        # SQLite 需要先开启外键约束
        if self._db_type == "sqlite":
            self._connector.execute("PRAGMA foreign_keys = ON")

        self._drop_tables()
        self._create_tables()
        self._create_indexes()

        logger.info("[%s] 模式初始化完成（共 %d 张表）", self._db_type, len(TABLES))

    # ── DROP 阶段 ───────────────────────────────
    def _drop_tables(self) -> None:
        """按反序删除已有表（满足外键约束）。"""
        for table in reversed(TABLES):
            ddl = self._gen_drop(table.name)
            try:
                self._connector.execute(ddl)
                logger.debug("[%s] 删除表 %s", self._db_type, table.name)
            except Exception:
                # Oracle PL/SQL 块内部已处理 -942，此处兜底
                logger.debug("[%s] 表 %s 不存在，跳过", self._db_type, table.name)

    def _gen_drop(self, table_name: str) -> str:
        """生成方言专用的 DROP TABLE 语句。"""
        if self._db_type == "oracle":
            # Oracle 不支持 IF EXISTS，用 PL/SQL 匿名块
            return (
                f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table_name} CASCADE CONSTRAINTS'; "
                f"EXCEPTION WHEN OTHERS THEN "
                f"IF SQLCODE != -942 THEN RAISE; END IF; END;"
            )
        # SQLite
        return f"DROP TABLE IF EXISTS {table_name}"

    # ── CREATE 阶段 ──────────────────────────────
    def _create_tables(self) -> None:
        """按正序创建所有表。"""
        for table in TABLES:
            ddl = self._gen_create(table)
            self._connector.execute(ddl)
            logger.info("[%s] 创建表 %s", self._db_type, table.name)

    def _gen_create(self, table: TableDef) -> str:
        """生成方言专用的 CREATE TABLE 语句。"""
        col_defs = []
        for col in table.columns:
            col_defs.append(self._col_to_ddl(col))

        # 外键约束
        for fk in table.foreign_keys:
            col_defs.append(
                f"FOREIGN KEY ({fk.column}) REFERENCES {fk.ref_table}({fk.ref_column})"
            )

        body = ", ".join(col_defs)
        return f"CREATE TABLE {table.name} ({body})"

    def _col_to_ddl(self, col: ColumnDef) -> str:
        """将通用列定义转换为方言 DDL 片段。"""
        type_map = _TYPE_MAP[self._db_type]
        sql_type = self._resolve_type(col.generic_type, type_map)

        parts = [col.name, sql_type]

        # Oracle 要求 DEFAULT 在 NOT NULL 之前
        if col.default is not None:
            parts.append(f"DEFAULT {col.default}")
        if col.primary_key:
            parts.append("PRIMARY KEY")
        if not col.nullable and not col.primary_key:
            parts.append("NOT NULL")

        return " ".join(parts)

    @staticmethod
    def _resolve_type(generic_type: str, type_map: Dict[str, str]) -> str:
        """解析通用类型到具体方言类型。

        处理带参数的类型，如 VARCHAR(50) → VARCHAR2(50) (Oracle)
                               DECIMAL(10,2) → NUMBER(10,2) (Oracle)
        """
        # 拆分基础类型和参数
        paren_idx = generic_type.find("(")
        if paren_idx == -1:
            base = generic_type
            params = ""
        else:
            base = generic_type[:paren_idx]
            params = generic_type[paren_idx:]  # 包含括号

        mapped = type_map.get(base, base)

        # 如果映射后的类型本身不带参数，则附加原始参数
        if params and "(" not in mapped:
            return mapped + params
        return mapped

    # ── INDEX 阶段 ────────────────────────────────
    def _create_indexes(self) -> None:
        """创建所有索引。"""
        for table in TABLES:
            for idx in table.indexes:
                cols = ", ".join(idx.columns)
                ddl = f"CREATE INDEX {idx.name} ON {table.name} ({cols})"
                self._connector.execute(ddl)
                logger.debug("[%s] 创建索引 %s", self._db_type, idx.name)
