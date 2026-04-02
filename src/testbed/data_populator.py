"""测试数据填充器 — 向测试表中插入覆盖各种边界条件的数据。

数据设计原则（参考 SQLancer 低行数策略）：
- 每表 15–20 行，避免笛卡尔积超时
- 覆盖：正常值、NULL、边界值（0, -1, MAX）、空字符串、负数、特殊字符
- 时间戳使用 datetime 对象，oracledb 和 sqlite3 均可正确绑定
"""

from datetime import date, datetime
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


def _d(s: str) -> date:
    """将 ISO 格式字符串解析为 date 对象。

    oracledb 绑定 DATE 列要求 date/datetime 对象；
    sqlite3 绑定 TEXT 列时会自动存为字符串。
    """
    return datetime.strptime(s, "%Y-%m-%d").date()


# ────────────────────────────────────────────────────────
# 测试数据定义（每表一个 (列名列表, 行数据列表) 元组）
# ────────────────────────────────────────────────────────

# ── t_users ──────────────────────────────────────────
_USERS_COLS = ("id", "username", "email", "age", "score", "active", "created_at", "manager_id", "profile",
               "birth_date", "height", "initials")
_USERS_ROWS: List[Tuple[Any, ...]] = [
    # alice (1) ← 根节点
    (1, "alice", "alice@example.com", 25, 88.50, 1, _ts("2024-01-15 10:30:00"),
     None, '{"theme": "dark", "lang": "en"}',
     _d("1995-03-15"), 175.5, "ALS"),
    # bob (2) → 上级 alice
    (2, "bob", "bob@example.com", 30, 92.00, 1, _ts("2024-02-20 14:00:00"),
     1, '{"theme": "light", "lang": "en", "notifications": true}',
     _d("1990-06-20"), 180.0, "BOB"),
    # Charlie (3) → 上级 alice（大小写 Unicode）
    (3, "Charlie", None, 0, 75.25, 1, _ts("2024-03-10 08:15:00"),
     1, None,
     _d("2000-01-01"), 160.25, "CHL"),
    # diana (4) → 上级 bob
    (4, "diana", "diana@example.com", -1, None, 0, _ts("2024-04-05 16:45:00"),
     2, '{}',
     None, -1.0, None),
    # Ève (5) → 上级 bob（拉丁重音 Unicode）
    (5, "Ève", "", 999, 100.00, 1, None,
     2, '{"theme": "dark"}',
     _d("2099-12-31"), 165.0, "EVE"),
    # frank (6) → 上级 charlie
    (6, "frank", "frank@example.com", 18, 0.00, 1, _ts("2024-06-01 12:00:00"),
     3, '{"lang": "fr", "timezone": "UTC+1"}',
     _d("2002-11-05"), 0.0, "FRK"),
    # grace (7) ← 根节点
    (7, "grace", None, None, -10.50, 0, _ts("2024-07-22 09:30:00"),
     None, '{"theme": "light"}',
     _d("1970-01-01"), 999.999, None),
    # heidi (8) → 上级 grace
    (8, "heidi", "heidi@example.com", 45, 55.55, 1, _ts("2024-08-15 18:20:00"),
     7, None,
     _d("1985-04-10"), 168.75, "HDI"),
    # ivan (9) ← 根节点（叶子）
    (9, "ivan", "ivan@example.com", 22, 99.99, 1, _ts("2024-09-01 07:00:00"),
     None, '{"theme": "dark", "lang": "ru"}',
     _d("1998-09-01"), 178.0, "IVN"),
    # 小明 (10) → 上级 heidi（CJK Unicode）
    (10, "小明", "", 35, 60.00, 0, _ts("2024-10-10 20:10:00"),
     8, '{"lang": "zh", "notifications": false}',
     None, 0.001, None),
    # kevin (11) → 上级 charlie
    (11, "kevin", "kevin@example.com", 28, None, 1, _ts("2024-11-05 11:45:00"),
     3, '{}',
     _d("1993-07-22"), 185.5, "KEV"),
    # linda (12) ← 根节点（叶子）
    (12, "linda", None, 0, 33.33, 1, None,
     None, None,
     _d("2005-12-25"), 155.0, "LND"),
    # mike (13) → 上级 grace
    (13, "mike", "mike@example.com", 50, 77.77, 0, _ts("2024-12-25 00:00:00"),
     7, '{"theme": "system"}',
     _d("1980-02-14"), 172.25, "MIK"),
    # O'Brien (14) ← 根节点（叶子，撇号 Unicode）
    (14, "O'Brien", "nancy@example.com", -1, 0.01, 1, _ts("2025-01-01 23:59:59"),
     None, '{"lang": "en", "theme": "dark"}',
     _d("1996-08-30"), 190.0, "OB1"),
    # José (15) → 上级 diana（拉丁重音 Unicode）
    (15, "José", "", None, None, 1, None,
     4, None,
     None, None, None),
]

# ── t_products ───────────────────────────────────────
_PRODUCTS_COLS = ("id", "name", "category", "price", "stock", "discontinued", "metadata",
                  "release_date", "weight_kg")
_PRODUCTS_ROWS: List[Tuple[Any, ...]] = [
    (1, "Widget A", "electronics", 29.99, 100, 0,
     '{"color": "red", "weight": 0.5}',
     _d("2023-01-15"), 0.5),
    (2, "Widget B", "electronics", 49.99, 0, 0,
     '{"color": "blue", "weight": 0.8}',
     _d("2023-03-01"), 0.8),
    (3, "Gadget X", "gadgets", 9999.99, 5, 0,
     '{"tags": ["new", "sale"], "warranty": 24}',
     _d("2024-06-01"), 2.5),
    (4, "Gadget Y", None, 0.01, None, 1,
     None,
     None, -1.0),
    (5, "Book Alpha", "books", 15.00, 200, 0,
     '{"author": "Smith", "pages": 350}',
     _d("2020-01-01"), 0.3),
    (6, "Book Beta", "books", 25.50, 50, 0,
     '{"author": "Jones", "pages": 200}',
     _d("2021-07-15"), 0.35),
    (7, "Tool Pro", "tools", 199.99, 10, 0,
     '{"material": "steel", "warranty": 12}',
     _d("2022-11-20"), 5.0),
    (8, "Tool Basic", "tools", 79.99, None, 1,
     '{}',
     _d("2022-05-10"), 1.2),
    (9, "配件Z", None, 5.00, 0, 0,
     '{"origin": "CN"}',
     None, 0.001),
    (10, "Premium Item", "electronics", 599.99, 3, 0,
     '{"color": "gold", "weight": 1.2}',
     _d("2024-01-01"), 1.2),
    (11, "Clearance Item", "clearance", 1.00, 999, 1,
     None,
     _d("2019-12-31"), 999.999),
    (12, "Luxury Good", "luxury", 4999.99, 1, 0,
     '{"material": "platinum", "limited": true}',
     _d("2024-09-01"), 0.01),
    (13, "Free Sample", "samples", 0.01, 500, 0,
     '{}',
     _d("2023-06-15"), 0.0),
    (14, "Paquet Économique", None, 120.00, 0, 0,
     '{"units": 12, "discount": 0.15}',
     None, 0.75),
    (15, "Rare Find", "collectibles", 750.00, None, 0,
     None,
     _d("2099-12-31"), 3.14),
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
_METRICS_COLS = ("id", "user_id", "metric_name", "metric_value", "recorded_at", "measurement_date")
_METRICS_ROWS: List[Tuple[Any, ...]] = [
    (1, 1, "login_count", 150.00000, _ts("2024-06-01 00:00:00"), _d("2024-06-01")),
    (2, 1, "page_views", 1200.50000, _ts("2024-06-01 00:00:00"), _d("2024-06-01")),
    (3, 2, "login_count", 80.00000, _ts("2024-06-01 00:00:00"), _d("2024-06-01")),
    (4, 2, "page_views", None, _ts("2024-06-01 00:00:00"), _d("2024-06-01")),
    (5, 3, "login_count", 0.00001, _ts("2024-06-15 12:00:00"), _d("2024-06-15")),
    (6, 3, "page_views", 99999.99999, _ts("2024-06-15 12:00:00"), _d("2024-06-15")),
    (7, 4, "login_count", None, None, None),
    (8, 5, "login_count", 45.12345, _ts("2024-07-01 00:00:00"), _d("2024-07-01")),
    (9, 5, "page_views", 0.00000, _ts("2024-07-01 00:00:00"), _d("2024-07-01")),
    (10, 6, "login_count", 200.00000, _ts("2024-07-15 00:00:00"), _d("2024-07-15")),
    (11, 6, "score", -5.55555, _ts("2024-07-15 00:00:00"), _d("2024-07-15")),
    (12, 7, "login_count", 10.00000, _ts("2024-08-01 00:00:00"), _d("2024-08-01")),
    (13, 8, "page_views", 500.00000, None, None),
    (14, 9, "login_count", 300.00000, _ts("2024-08-15 00:00:00"), _d("2024-08-15")),
    (15, 9, "score", None, _ts("2024-08-15 00:00:00"), _d("2024-08-15")),
    (16, 10, "login_count", 55.00000, _ts("2024-09-01 00:00:00"), _d("2024-09-01")),
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

# ── t_departments ────────────────────────────────────
_DEPARTMENTS_COLS = ("id", "name", "parent_id", "budget", "location")
_DEPARTMENTS_ROWS: List[Tuple[Any, ...]] = [
    (1, "Engineering", None, 500000.00, "Building A"),
    (2, "Sales", None, 300000.00, "Building B"),
    (3, "HR", None, 200000.00, "Building A"),
    (4, "Frontend", 1, 150000.00, "Floor 2"),
    (5, "Backend", 1, 180000.00, "Floor 3"),
    (6, "Domestic", 2, 100000.00, None),
    (7, "International", 2, None, "Building C"),
    (8, "Recruiting", 3, 80000.00, "Floor 1"),
    (9, "QA", 1, 0.00, "Floor 2"),
    (10, "Other", None, -1.00, None),  # 边界预算
]

# ── t_employees ──────────────────────────────────────
_EMPLOYEES_COLS = ("id", "name", "dept_id", "salary", "hire_date", "manager_id", "status", "bio")
_EMPLOYEES_ROWS: List[Tuple[Any, ...]] = [
    (1, "张三", 4, 120000.00, _d("2020-03-15"), None, "active", "Senior frontend developer"),
    (2, "李四", 5, 95000.50, _d("2021-07-01"), None, "active", None),
    (3, "Alice", 4, 110000.00, _d("2019-01-10"), 1, "active", '{"skills": ["React", "TypeScript"]}'),
    (4, "Bob", 5, 130000.00, _d("2018-06-20"), 2, "active", ""),
    (5, "王五", 6, 70000.00, _d("2022-11-01"), None, "active", "Domestic sales lead"),
    (6, "赵六", 7, 85000.75, _d("2023-02-14"), None, "on_leave", None),
    (7, "Carol", 8, 65000.00, None, None, "active", "HR specialist"),
    (8, "David", 9, 90000.00, _d("2020-09-01"), None, "active", "QA engineer"),
    (9, "陈七", 4, 0.00, _d("2024-01-01"), 1, "probation", "New grad"),
    (10, "刘八", 5, -1.00, _d("2024-03-15"), 2, None, None),  # 负数工资边界
    (11, "Eve", 1, 150000.00, _d("2017-05-01"), None, "active", "Engineering director"),
    (12, "Frank", 3, 99000.99, _d("2019-12-25"), None, "active", "HR manager"),
    (13, "黄九", 4, 105000.00, _d("2021-01-15"), 1, "active", ""),
    (14, "林十", 6, 60000.00, None, 5, "active", "Sales rep"),
    (15, "Grace", 5, 115000.00, _d("2020-07-01"), 2, "active", None),
    (16, "Hank", 9, 88000.00, _d("2021-03-10"), 8, "active", "Senior QA"),
    (17, "周十一", 7, 78000.50, _d("2022-06-01"), 6, "active", "Intl sales"),
    (18, "吴十二", 4, 99999.99, _d("2023-01-01"), 1, "probation", None),
    (19, "Ivy", 8, 55000.00, _d("2023-09-01"), 7, "active", "Junior recruiter"),
    (20, "Jack", None, None, None, None, "inactive", None),  # 无部门
    (21, "孙十三", 1, 200000.00, _d("2015-01-01"), 11, "active", "CTO"),
    (22, "钱十四", 9, 82000.00, _d("2022-04-15"), 8, "active", "QA analyst"),
    (23, "Karl", 3, 105000.00, _d("2020-02-01"), 12, "on_leave", ""),
    (24, "Leo", 6, 72000.00, _d("2021-08-01"), 5, "active", "Sales associate"),
    (25, "郑十五", 5, 100000.00, _d("2019-10-15"), 2, "active", None),
    (26, "Mia", 4, 98000.00, _d("2022-01-01"), 1, "active", "Frontend dev"),
    (27, "Nancy", 7, 90000.00, _d("2020-11-01"), 6, "inactive", "Former intl sales"),
    (28, "冯十六", 8, 62000.00, _d("2023-05-01"), 7, "active", None),
    (29, "Oscar", 1, 135000.00, _d("2018-03-01"), 11, "active", "Principal engineer"),
    (30, "褚十七", None, 0.01, _d("2024-06-01"), None, "probation", "Intern"),
]

# ── t_events ─────────────────────────────────────────
_EVENTS_COLS = ("id", "event_type", "event_time", "event_date", "payload", "user_id")
_EVENTS_ROWS: List[Tuple[Any, ...]] = [
    (1, "login", _ts("2024-06-01 08:00:00"), _d("2024-06-01"), '{"ip": "192.168.1.1"}', 1),
    (2, "purchase", _ts("2024-06-01 10:30:00"), _d("2024-06-01"), '{"product_id": 1, "qty": 2}', 1),
    (3, "login", _ts("2024-06-02 09:15:00"), _d("2024-06-02"), '{"ip": "10.0.0.5"}', 3),
    (4, "logout", _ts("2024-06-02 18:00:00"), _d("2024-06-02"), None, 3),
    (5, "click", _ts("2024-06-03 14:20:00"), _d("2024-06-03"), '{"page": "/products", "x": 100, "y": 200}', 5),
    (6, "error", _ts("2024-06-04 03:00:00"), _d("2024-06-04"), '{"code": 500, "msg": "Internal error"}', None),
    (7, "signup", _ts("2024-06-05 11:00:00"), _d("2024-06-05"), '{}', 9),
    (8, "login", None, None, '{"ip": "172.16.0.1"}', 7),
    (9, "purchase", _ts("2024-06-06 16:45:00"), _d("2024-06-06"), None, 2),
    (10, "click", _ts("2024-06-07 08:30:00"), _d("2024-06-07"), '{"page": "/home"}', 6),
    (11, "system", _ts("2024-06-08 00:00:00"), _d("2024-06-08"), "", 8),  # system event
    (12, "api_call", _ts("2024-06-08 12:00:00"), _d("2024-06-08"), '{"endpoint": "/api/v1/users", "method": "GET"}', 1),
    (13, "login", _ts("2024-06-09 07:00:00"), _d("2024-06-09"), '{"ip": "192.168.1.50"}', 10),
    (14, "purchase", _ts("2024-06-10 20:00:00"), _d("2024-06-10"), '{"product_id": 5, "qty": 1}', 13),
    (15, "error", _ts("2024-06-11 01:00:00"), _d("2024-06-11"), '{"code": 404}', None),
    (16, "login", _ts("2024-06-12 09:00:00"), _d("2024-06-12"), None, 15),
    (17, "click", _ts("2024-06-13 15:30:00"), _d("2024-06-13"), '{"page": "/cart", "items": [1, 3, 5]}', 1),
    (18, "api_call", None, _d("2024-06-14"), '{"endpoint": "/api/v1/orders"}', 5),
    (19, "purchase", _ts("2024-06-15 10:00:00"), None, '{"product_id": 10}', 8),
    (20, "signup", _ts("2024-06-16 14:00:00"), _d("2024-06-16"), '{}', 14),
    (21, "login", _ts("1970-01-01 00:00:00"), _d("1970-01-01"), '{"ip": "0.0.0.0"}', 11),  # epoch
    (22, "error", _ts("2099-12-31 23:59:59"), _d("2099-12-31"), '{"code": 999}', 12),  # 远未来
    (23, "click", _ts("2024-06-17 08:00:00"), _d("2024-06-17"), None, 4),
    (24, "api_call", _ts("2024-06-18 11:30:00"), _d("2024-06-18"), '{"endpoint": "/health"}', None),
    (25, "login", _ts("2024-06-19 06:00:00"), _d("2024-06-19"), '{"ip": "10.10.10.10"}', 6),
    (26, "logout", _ts("2024-06-19 22:00:00"), _d("2024-06-19"), None, 6),
    (27, "purchase", _ts("2024-06-20 09:15:00"), _d("2024-06-20"), '{"product_id": 3, "qty": 1}', 3),
    (28, "click", _ts("2024-06-21 13:00:00"), _d("2024-06-21"), '{"page": "/profile"}', 9),
    (29, "error", None, None, None, None),  # 全 NULL 事件
    (30, "api_call", _ts("2024-06-22 10:00:00"), _d("2024-06-22"), '{"nested": {"key": "val"}, "arr": [1,2,3]}', 2),
]

# ── t_transactions ───────────────────────────────────
_TRANSACTIONS_COLS = ("id", "from_user", "to_user", "amount", "tx_type", "created_at", "status", "metadata_json")
_TRANSACTIONS_ROWS: List[Tuple[Any, ...]] = [
    (1, 1, 2, 100.00, "transfer", _ts("2024-06-01 10:00:00"), "completed", '{"note": "rent"}'),
    (2, 2, 3, 250.50, "transfer", _ts("2024-06-02 14:30:00"), "completed", None),
    (3, 3, 1, 0.01, "transfer", _ts("2024-06-03 09:00:00"), "completed", '{"note": "test"}'),
    (4, 5, 1, -1.00, "refund", _ts("2024-06-04 11:00:00"), "completed", ""),  # 负数金额
    (5, None, 7, 500.00, "deposit", _ts("2024-06-05 08:00:00"), "completed", '{"source": "bank"}'),
    (6, 8, None, 200.00, "withdrawal", _ts("2024-06-06 16:00:00"), "pending", None),
    (7, 1, 5, 99999.99, "transfer", _ts("2024-06-07 12:00:00"), "failed", '{"reason": "insufficient funds"}'),
    (8, 6, 9, 75.25, "payment", _ts("2024-06-08 10:30:00"), "completed", '{"item": "Widget A"}'),
    (9, 10, 1, 1000.00, "transfer", None, "pending", None),
    (10, 2, 6, 0.00, "transfer", _ts("2024-06-10 09:00:00"), "completed", '{"note": ""}'),
    (11, 13, 3, 350.00, "payment", _ts("2024-06-11 14:00:00"), "completed", '{"items": [1, 2]}'),
    (12, None, 4, 10000.00, "deposit", _ts("2024-06-12 08:00:00"), "completed", '{"source": "wire"}'),
    (13, 7, 8, 88.88, "transfer", _ts("2024-06-13 11:00:00"), None, None),
    (14, 1, 9, 555.55, "transfer", _ts("2024-06-14 15:00:00"), "completed", ""),
    (15, 11, 2, 420.00, "payment", _ts("2024-06-15 09:30:00"), "completed", '{"item": "Gadget X"}'),
    (16, 4, 1, 0.50, "refund", _ts("2024-06-16 10:00:00"), "completed", None),
    (17, None, None, 0.00, "transfer", None, "failed", None),  # 无双方
    (18, 3, 5, 2000.00, "transfer", _ts("2024-06-18 13:00:00"), "completed", '{"urgent": true}'),
    (19, 9, 13, 120.00, "payment", _ts("2024-06-19 16:30:00"), "pending", '{"item": "Book Alpha"}'),
    (20, 14, 1, 50.00, "transfer", _ts("2024-06-20 08:00:00"), "completed", None),
    (21, 5, 10, 333.33, "transfer", _ts("2024-06-21 10:00:00"), "completed", '{"note": "split bill"}'),
    (22, 6, 3, -999.99, "refund", _ts("2024-06-22 14:00:00"), "completed", '{"reason": "return"}'),
    (23, None, 11, 5000.00, "deposit", _ts("2024-06-23 09:00:00"), "completed", '{"source": "crypto"}'),
    (24, 12, 7, 250.00, "transfer", _ts("2024-06-24 11:00:00"), None, None),
    (25, 8, 15, 180.00, "payment", _ts("2024-06-25 15:30:00"), "completed", '{"items": [5, 6]}'),
    (26, 1, 6, 999.00, "transfer", _ts("2024-06-26 08:30:00"), "failed", ""),
    (27, 2, 11, 45.50, "payment", _ts("2024-06-27 12:00:00"), "completed", '{"item": "配件Z"}'),
    (28, None, 5, 100.00, "deposit", _ts("2024-06-28 09:00:00"), "pending", None),
    (29, 3, 8, 777.77, "transfer", _ts("2024-06-29 10:00:00"), "completed", '{"note": "bonus"}'),
    (30, 7, 2, 0.01, "refund", _ts("2024-06-30 16:00:00"), "completed", None),
]

# 所有表的数据集合
_ALL_TABLE_DATA: List[Tuple[str, Tuple[str, ...], List[Tuple[Any, ...]]]] = [
    ("t_users", _USERS_COLS, _USERS_ROWS),
    ("t_products", _PRODUCTS_COLS, _PRODUCTS_ROWS),
    ("t_orders", _ORDERS_COLS, _ORDERS_ROWS),
    ("t_metrics", _METRICS_COLS, _METRICS_ROWS),
    ("t_tags", _TAGS_COLS, _TAGS_ROWS),
    ("t_departments", _DEPARTMENTS_COLS, _DEPARTMENTS_ROWS),
    ("t_employees", _EMPLOYEES_COLS, _EMPLOYEES_ROWS),
    ("t_events", _EVENTS_COLS, _EVENTS_ROWS),
    ("t_transactions", _TRANSACTIONS_COLS, _TRANSACTIONS_ROWS),
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
