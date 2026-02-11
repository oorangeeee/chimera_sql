"""测试数据填充器 — 向测试表中插入覆盖各种边界条件的数据。

数据设计原则（参考 SQLancer 低行数策略）：
- 每表 15–20 行，避免笛卡尔积超时
- 覆盖：正常值、NULL、边界值（0, -1, MAX）、空字符串、负数、特殊字符
- 时间戳使用 datetime 对象，oracledb 和 sqlite3 均可正确绑定
"""

from datetime import datetime
from typing import Any, Callable, Dict, List, Tuple

from src.connector.base import DBConnector
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ────────────────────────────────────────────────────────
# 占位符生成器（Oracle 用 :1/:2，SQLite 用 ?）
# ────────────────────────────────────────────────────────
_PLACEHOLDER: Dict[str, Callable[[int], str]] = {
    "oracle": lambda n: ", ".join(f":{i + 1}" for i in range(n)),
    "sqlite": lambda n: ", ".join("?" for _ in range(n)),
}


def _ts(s: str) -> datetime:
    """将 ISO 格式字符串解析为 datetime 对象。

    oracledb 绑定 TIMESTAMP 列要求 datetime 对象，不接受纯字符串；
    sqlite3 绑定 TEXT 列时会自动调用 datetime.__str__() 存为字符串。
    """
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


# ────────────────────────────────────────────────────────
# 测试数据定义（每表一个 (列名列表, 行数据列表) 元组）
# ────────────────────────────────────────────────────────

# ── t_users ──────────────────────────────────────────
_USERS_COLS = ("id", "username", "email", "age", "score", "active", "created_at", "manager_id", "profile")
_USERS_ROWS: List[Tuple[Any, ...]] = [
    # alice (1) ← 根节点
    (1, "alice", "alice@example.com", 25, 88.50, 1, _ts("2024-01-15 10:30:00"),
     None, '{"theme": "dark", "lang": "en"}'),
    # bob (2) → 上级 alice
    (2, "bob", "bob@example.com", 30, 92.00, 1, _ts("2024-02-20 14:00:00"),
     1, '{"theme": "light", "lang": "en", "notifications": true}'),
    # Charlie (3) → 上级 alice（大小写 Unicode）
    (3, "Charlie", None, 0, 75.25, 1, _ts("2024-03-10 08:15:00"),
     1, None),
    # diana (4) → 上级 bob
    (4, "diana", "diana@example.com", -1, None, 0, _ts("2024-04-05 16:45:00"),
     2, '{}'),
    # Ève (5) → 上级 bob（拉丁重音 Unicode）
    (5, "Ève", "", 999, 100.00, 1, None,
     2, '{"theme": "dark"}'),
    # frank (6) → 上级 charlie
    (6, "frank", "frank@example.com", 18, 0.00, 1, _ts("2024-06-01 12:00:00"),
     3, '{"lang": "fr", "timezone": "UTC+1"}'),
    # grace (7) ← 根节点
    (7, "grace", None, None, -10.50, 0, _ts("2024-07-22 09:30:00"),
     None, '{"theme": "light"}'),
    # heidi (8) → 上级 grace
    (8, "heidi", "heidi@example.com", 45, 55.55, 1, _ts("2024-08-15 18:20:00"),
     7, None),
    # ivan (9) ← 根节点（叶子）
    (9, "ivan", "ivan@example.com", 22, 99.99, 1, _ts("2024-09-01 07:00:00"),
     None, '{"theme": "dark", "lang": "ru"}'),
    # 小明 (10) → 上级 heidi（CJK Unicode）
    (10, "小明", "", 35, 60.00, 0, _ts("2024-10-10 20:10:00"),
     8, '{"lang": "zh", "notifications": false}'),
    # kevin (11) → 上级 charlie
    (11, "kevin", "kevin@example.com", 28, None, 1, _ts("2024-11-05 11:45:00"),
     3, '{}'),
    # linda (12) ← 根节点（叶子）
    (12, "linda", None, 0, 33.33, 1, None,
     None, None),
    # mike (13) → 上级 grace
    (13, "mike", "mike@example.com", 50, 77.77, 0, _ts("2024-12-25 00:00:00"),
     7, '{"theme": "system"}'),
    # O'Brien (14) ← 根节点（叶子，撇号 Unicode）
    (14, "O'Brien", "nancy@example.com", -1, 0.01, 1, _ts("2025-01-01 23:59:59"),
     None, '{"lang": "en", "theme": "dark"}'),
    # José (15) → 上级 diana（拉丁重音 Unicode）
    (15, "José", "", None, None, 1, None,
     4, None),
]

# ── t_products ───────────────────────────────────────
_PRODUCTS_COLS = ("id", "name", "category", "price", "stock", "discontinued", "metadata")
_PRODUCTS_ROWS: List[Tuple[Any, ...]] = [
    (1, "Widget A", "electronics", 29.99, 100, 0,
     '{"color": "red", "weight": 0.5}'),
    (2, "Widget B", "electronics", 49.99, 0, 0,
     '{"color": "blue", "weight": 0.8}'),
    (3, "Gadget X", "gadgets", 9999.99, 5, 0,
     '{"tags": ["new", "sale"], "warranty": 24}'),
    (4, "Gadget Y", None, 0.01, None, 1,
     None),
    (5, "Book Alpha", "books", 15.00, 200, 0,
     '{"author": "Smith", "pages": 350}'),
    (6, "Book Beta", "books", 25.50, 50, 0,
     '{"author": "Jones", "pages": 200}'),
    (7, "Tool Pro", "tools", 199.99, 10, 0,
     '{"material": "steel", "warranty": 12}'),
    (8, "Tool Basic", "tools", 79.99, None, 1,
     '{}'),
    (9, "配件Z", None, 5.00, 0, 0,
     '{"origin": "CN"}'),
    (10, "Premium Item", "electronics", 599.99, 3, 0,
     '{"color": "gold", "weight": 1.2}'),
    (11, "Clearance Item", "clearance", 1.00, 999, 1,
     None),
    (12, "Luxury Good", "luxury", 4999.99, 1, 0,
     '{"material": "platinum", "limited": true}'),
    (13, "Free Sample", "samples", 0.01, 500, 0,
     '{}'),
    (14, "Paquet Économique", None, 120.00, 0, 0,
     '{"units": 12, "discount": 0.15}'),
    (15, "Rare Find", "collectibles", 750.00, None, 0,
     None),
]

# ── t_orders ─────────────────────────────────────────
_ORDERS_COLS = (
    "id",
    "user_id",
    "product_id",
    "quantity",
    "total_price",
    "order_date",
    "status",
)
_ORDERS_ROWS: List[Tuple[Any, ...]] = [
    (1, 1, 1, 2, 59.98, _ts("2024-03-01 10:00:00"), "delivered"),
    (2, 1, 5, 1, 15.00, _ts("2024-03-15 11:30:00"), "delivered"),
    (3, 2, 3, 1, 9999.99, _ts("2024-04-01 09:00:00"), "shipped"),
    (4, 2, 6, 3, 76.50, _ts("2024-04-10 14:20:00"), "pending"),
    (5, 3, 2, 1, 49.99, _ts("2024-05-01 16:00:00"), "cancelled"),
    (6, 4, 7, 1, 199.99, None, "pending"),
    (7, 5, 10, 1, 599.99, _ts("2024-06-15 08:45:00"), None),
    (8, 6, 1, 5, 149.95, _ts("2024-07-01 12:00:00"), "delivered"),
    (9, 7, 12, 1, 4999.99, _ts("2024-07-20 10:30:00"), "shipped"),
    (10, 8, 4, 10, 0.10, _ts("2024-08-01 15:00:00"), "delivered"),
    (11, 9, 9, 3, 15.00, None, "pending"),
    (12, 10, 11, 100, 100.00, _ts("2024-09-01 09:00:00"), "delivered"),
    (13, 1, 13, 2, 0.02, _ts("2024-10-01 11:00:00"), None),
    (14, 3, 8, 1, 79.99, _ts("2024-10-15 13:30:00"), "cancelled"),
    (15, 5, 14, 1, 120.00, _ts("2024-11-01 17:00:00"), "shipped"),
    (16, 11, 5, 2, 30.00, _ts("2024-11-20 10:00:00"), "pending"),
    (17, 12, 15, 1, 750.00, None, "shipped"),
    (18, 13, 2, 1, 49.99, _ts("2024-12-01 14:00:00"), "delivered"),
]

# ── t_metrics ────────────────────────────────────────
_METRICS_COLS = ("id", "user_id", "metric_name", "metric_value", "recorded_at")
_METRICS_ROWS: List[Tuple[Any, ...]] = [
    (1, 1, "login_count", 150.00000, _ts("2024-06-01 00:00:00")),
    (2, 1, "page_views", 1200.50000, _ts("2024-06-01 00:00:00")),
    (3, 2, "login_count", 80.00000, _ts("2024-06-01 00:00:00")),
    (4, 2, "page_views", None, _ts("2024-06-01 00:00:00")),
    (5, 3, "login_count", 0.00001, _ts("2024-06-15 12:00:00")),
    (6, 3, "page_views", 99999.99999, _ts("2024-06-15 12:00:00")),
    (7, 4, "login_count", None, None),
    (8, 5, "login_count", 45.12345, _ts("2024-07-01 00:00:00")),
    (9, 5, "page_views", 0.00000, _ts("2024-07-01 00:00:00")),
    (10, 6, "login_count", 200.00000, _ts("2024-07-15 00:00:00")),
    (11, 6, "score", -5.55555, _ts("2024-07-15 00:00:00")),
    (12, 7, "login_count", 10.00000, _ts("2024-08-01 00:00:00")),
    (13, 8, "page_views", 500.00000, None),
    (14, 9, "login_count", 300.00000, _ts("2024-08-15 00:00:00")),
    (15, 9, "score", None, _ts("2024-08-15 00:00:00")),
    (16, 10, "login_count", 55.00000, _ts("2024-09-01 00:00:00")),
]

# ── t_tags ───────────────────────────────────────────
_TAGS_COLS = ("id", "entity_type", "entity_id", "tag")
_TAGS_ROWS: List[Tuple[Any, ...]] = [
    # 用户标签
    (1, "user", 1, "vip"),
    (2, "user", 1, "active"),
    (3, "user", 2, "active"),
    (4, "user", 3, "new"),
    (5, "user", 5, "vip"),
    (6, "user", 7, "inactive"),
    (7, "user", 9, "active"),
    # 产品标签（与用户标签有重叠，用于 INTERSECT 测试）
    (8, "product", 1, "popular"),
    (9, "product", 1, "vip"),
    (10, "product", 3, "premium"),
    (11, "product", 5, "popular"),
    (12, "product", 10, "premium"),
    (13, "product", 12, "vip"),
    (14, "product", 7, "active"),
    (15, "product", 11, "clearance"),
    # 额外标签
    (16, "user", 10, "inactive"),
    (17, "product", 4, "discontinued"),
    (18, "user", 13, "new"),
    # Unicode 标签（扩展维度）
    (19, "user", 2, "重要"),
    (20, "product", 3, "重要"),       # 与 user 重叠，INTERSECT 可命中
    (21, "user", 4, "café"),          # 拉丁重音标签
]

# 所有表的数据集合
_ALL_TABLE_DATA: List[Tuple[str, Tuple[str, ...], List[Tuple[Any, ...]]]] = [
    ("t_users", _USERS_COLS, _USERS_ROWS),
    ("t_products", _PRODUCTS_COLS, _PRODUCTS_ROWS),
    ("t_orders", _ORDERS_COLS, _ORDERS_ROWS),
    ("t_metrics", _METRICS_COLS, _METRICS_ROWS),
    ("t_tags", _TAGS_COLS, _TAGS_ROWS),
]


# ────────────────────────────────────────────────────────
# DataPopulator 核心类
# ────────────────────────────────────────────────────────
class DataPopulator:
    """向目标数据库的测试表中填充测试数据。"""

    def __init__(self, connector: DBConnector, db_type: str) -> None:
        self._connector = connector
        self._db_type = db_type.lower()
        if self._db_type not in _PLACEHOLDER:
            raise ValueError(f"不支持的数据库类型: {db_type}")

    def populate_all(self) -> None:
        """填充所有测试表。"""
        logger.info("[%s] 开始填充测试数据 ...", self._db_type)

        for table_name, columns, rows in _ALL_TABLE_DATA:
            self._populate_table(table_name, columns, rows)

        total = sum(len(rows) for _, _, rows in _ALL_TABLE_DATA)
        logger.info("[%s] 数据填充完成（共 %d 行）", self._db_type, total)

    def _populate_table(
        self,
        table_name: str,
        columns: Tuple[str, ...],
        rows: List[Tuple[Any, ...]],
    ) -> None:
        """向单张表插入所有行。"""
        col_list = ", ".join(columns)
        placeholders = _PLACEHOLDER[self._db_type](len(columns))
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

        for row in rows:
            # Oracle 需要将时间戳字符串通过 TO_TIMESTAMP 处理，
            # 但使用参数绑定时 oracledb 可以直接接受字符串，无需转换。
            self._connector.execute(sql, row)

        logger.info("[%s] 表 %s 填充 %d 行", self._db_type, table_name, len(rows))
