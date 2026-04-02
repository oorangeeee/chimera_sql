"""CAST 类型转换方言差异模板。

测试 SQLite 与 Oracle 之间 CAST 函数的行为差异：
- INT 截断 vs 四舍五入
- VARCHAR vs VARCHAR2
- DATE 格式差异
- REAL vs BINARY_DOUBLE
- 混合类型隐式转换
"""

from __future__ import annotations

from typing import List, Optional

from ..base import ColumnMeta, SchemaMetadata, SeedSQL, SeedTemplate


# 用于 CAST AS INTEGER 测试的数值列（不含 id）
_INT_CAST_COLUMNS = [
    ("t_users", "score", "score"),
    ("t_users", "age", "age"),
    ("t_users", "height", "height"),
    ("t_products", "price", "price"),
    ("t_products", "stock", "stock"),
    ("t_products", "weight_kg", "weight_kg"),
    ("t_orders", "quantity", "qty"),
    ("t_orders", "total_price", "total_price"),
    ("t_employees", "salary", "salary"),
    ("t_transactions", "amount", "amount"),
]

# 字符串列
_VARCHAR_CAST_COLUMNS = [
    ("t_users", "score", "score"),
    ("t_users", "age", "age"),
    ("t_products", "price", "price"),
    ("t_products", "stock", "stock"),
    ("t_orders", "total_price", "total_price"),
    ("t_employees", "salary", "salary"),
    ("t_events", "event_type", "event_type"),
    ("t_tags", "tag", "tag"),
]

# 用于 CAST AS REAL 的整数列
_REAL_CAST_COLUMNS = [
    ("t_users", "age", "age"),
    ("t_users", "score", "score"),
    ("t_products", "stock", "stock"),
    ("t_orders", "quantity", "qty"),
    ("t_employees", "salary", "salary"),
]


class CastTypesTemplate(SeedTemplate):
    """CAST 类型转换方言差异模板。"""

    @property
    def category_prefix(self) -> str:
        return "dialect"

    @property
    def domain(self) -> str:
        return "cast_types"

    @property
    def description(self) -> str:
        return "CAST类型转换方言差异测试"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._cast_to_integer(schema))
        seeds.extend(self._cast_to_varchar(schema))
        seeds.extend(self._cast_to_real(schema))
        seeds.extend(self._cast_to_decimal(schema))
        seeds.extend(self._cast_in_expressions(schema))
        return seeds

    def _cast_to_integer(self, schema: SchemaMetadata) -> List[SeedSQL]:
        """CAST AS INTEGER 差异：SQLite 截断 vs Oracle 四舍五入。"""
        seeds: List[SeedSQL] = []

        # 基本 CAST AS INTEGER
        for table, col, alias in _INT_CAST_COLUMNS:
            seeds.append(self._seed(
                f"SELECT id, {col}, CAST({col} AS INTEGER) AS {alias}_int FROM {table} WHERE {col} IS NOT NULL ORDER BY id",
                tags=["cast", "integer", table],
                desc=f"CAST({col} AS INTEGER) from {table}",
            ))

        # CAST AS INTEGER + WHERE
        for table, col, alias in _INT_CAST_COLUMNS[:6]:
            seeds.append(self._seed(
                f"SELECT id, {col}, CAST({col} AS INTEGER) AS {alias}_int FROM {table} WHERE CAST({col} AS INTEGER) > 0 ORDER BY id",
                tags=["cast", "integer", "where", table],
                desc=f"CAST({col} AS INTEGER) in WHERE from {table}",
            ))

        # CAST AS INTEGER + 算术
        for table, col, alias in _INT_CAST_COLUMNS[:6]:
            seeds.append(self._seed(
                f"SELECT id, {col}, CAST({col} AS INTEGER) + 10 AS {alias}_plus10 FROM {table} WHERE {col} IS NOT NULL ORDER BY id",
                tags=["cast", "integer", "arithmetic", table],
                desc=f"CAST({col} AS INTEGER) + 10 from {table}",
            ))

        # CAST AS INTEGER 对小数（最关键的差异测试）
        decimal_pairs = [
            ("t_products", "price"),
            ("t_orders", "total_price"),
            ("t_users", "score"),
            ("t_employees", "salary"),
            ("t_transactions", "amount"),
        ]
        for table, col in decimal_pairs:
            seeds.append(self._seed(
                f"SELECT id, {col}, CAST({col} AS INTEGER) AS truncated, {col} - CAST({col} AS INTEGER) AS frac FROM {table} WHERE {col} IS NOT NULL ORDER BY id",
                tags=["cast", "integer", "truncation", table],
                desc=f"CAST truncation test: {col} - CAST({col} AS INTEGER) from {table}",
            ))

        # 嵌套 CAST: CAST(CAST(col AS INTEGER) AS TEXT)
        for table, col, alias in _INT_CAST_COLUMNS[:5]:
            seeds.append(self._seed(
                f"SELECT id, CAST(CAST({col} AS INTEGER) AS VARCHAR(20)) AS nested FROM {table} WHERE {col} IS NOT NULL ORDER BY id",
                tags=["cast", "integer", "nested", table],
                desc=f"Nested CAST: VARCHAR(CAST({col} AS INTEGER)) from {table}",
            ))

        # CAST AS INTEGER 在 CASE 中
        seeds.append(self._seed(
            "SELECT id, score, CASE WHEN CAST(score AS INTEGER) >= 90 THEN 'A' WHEN CAST(score AS INTEGER) >= 60 THEN 'B' ELSE 'C' END AS grade FROM t_users WHERE score IS NOT NULL ORDER BY id",
            tags=["cast", "integer", "case"],
            desc="CAST AS INTEGER in CASE expression",
        ))

        # CAST AS INTEGER 对 NULL
        seeds.append(self._seed(
            "SELECT id, email, CAST(COALESCE(age, 0) AS INTEGER) AS age_safe FROM t_users ORDER BY id",
            tags=["cast", "integer", "null"],
            desc="CAST(COALESCE(age, 0) AS INTEGER) handling NULL",
        ))

        return seeds

    def _cast_to_varchar(self, schema: SchemaMetadata) -> List[SeedSQL]:
        """CAST AS VARCHAR 差异：日期格式、数值格式。"""
        seeds: List[SeedSQL] = []

        # 数值 → VARCHAR
        varchar_pairs = [
            ("t_users", "score"),
            ("t_products", "price"),
            ("t_orders", "total_price"),
            ("t_employees", "salary"),
            ("t_transactions", "amount"),
        ]
        for table, col in varchar_pairs:
            seeds.append(self._seed(
                f"SELECT id, {col}, CAST({col} AS VARCHAR(20)) AS {col}_str FROM {table} WHERE {col} IS NOT NULL ORDER BY id",
                tags=["cast", "varchar", table],
                desc=f"CAST({col} AS VARCHAR) from {table}",
            ))

        # 整数 → VARCHAR
        int_to_varchar = [
            ("t_users", "age"),
            ("t_products", "stock"),
            ("t_orders", "quantity"),
        ]
        for table, col in int_to_varchar:
            seeds.append(self._seed(
                f"SELECT id, {col}, CAST({col} AS VARCHAR(10)) AS {col}_str FROM {table} WHERE {col} IS NOT NULL ORDER BY id",
                tags=["cast", "varchar", table],
                desc=f"CAST({col} AS VARCHAR) from {table}",
            ))

        # 日期 → VARCHAR（关键差异点）
        date_cols = [
            ("t_users", "birth_date"),
            ("t_products", "release_date"),
            ("t_metrics", "measurement_date"),
            ("t_employees", "hire_date"),
            ("t_events", "event_date"),
        ]
        for table, col in date_cols:
            seeds.append(self._seed(
                f"SELECT id, {col}, CAST({col} AS VARCHAR(30)) AS {col}_str FROM {table} WHERE {col} IS NOT NULL ORDER BY id",
                tags=["cast", "varchar", "date", table],
                desc=f"CAST({col} AS VARCHAR) - date format diff from {table}",
            ))

        # VARCHAR 拼接
        for table, col in varchar_pairs[:3]:
            seeds.append(self._seed(
                f"SELECT id, 'Value: ' || CAST({col} AS VARCHAR(20)) AS labeled FROM {table} WHERE {col} IS NOT NULL ORDER BY id",
                tags=["cast", "varchar", "concat", table],
                desc=f"CAST({col} AS VARCHAR) concatenation from {table}",
            ))

        # VARCHAR 比较
        seeds.append(self._seed(
            "SELECT id, price, CAST(price AS VARCHAR(20)) AS price_str FROM t_products WHERE CAST(price AS VARCHAR(20)) > '100' ORDER BY id",
            tags=["cast", "varchar", "comparison"],
            desc="CAST(price AS VARCHAR) in comparison",
        ))

        return seeds

    def _cast_to_real(self, schema: SchemaMetadata) -> List[SeedSQL]:
        """CAST AS REAL / FLOAT 差异。"""
        seeds: List[SeedSQL] = []

        for table, col, alias in _REAL_CAST_COLUMNS:
            seeds.append(self._seed(
                f"SELECT id, {col}, CAST({col} AS REAL) AS {alias}_real FROM {table} WHERE {col} IS NOT NULL ORDER BY id",
                tags=["cast", "real", table],
                desc=f"CAST({col} AS REAL) from {table}",
            ))

        # REAL 在计算中
        seeds.append(self._seed(
            "SELECT id, age, CAST(age AS REAL) / 3 AS age_third FROM t_users WHERE age IS NOT NULL ORDER BY id",
            tags=["cast", "real", "division"],
            desc="CAST(age AS REAL) / 3 - float division",
        ))

        seeds.append(self._seed(
            "SELECT id, price, CAST(price AS REAL) AS price_real, ROUND(CAST(price AS REAL), 2) AS price_r2 FROM t_products ORDER BY id",
            tags=["cast", "real", "round"],
            desc="CAST(price AS REAL) + ROUND",
        ))

        # REAL vs INTEGER 差异
        seeds.append(self._seed(
            "SELECT id, score, CAST(score AS INTEGER) AS int_val, CAST(score AS REAL) AS real_val FROM t_users WHERE score IS NOT NULL ORDER BY id",
            tags=["cast", "real", "integer", "comparison"],
            desc="CAST AS INTEGER vs CAST AS REAL",
        ))

        # 负数 CAST AS REAL
        seeds.append(self._seed(
            "SELECT id, metric_value, CAST(metric_value AS REAL) AS mv_real FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id",
            tags=["cast", "real", "negative"],
            desc="CAST(metric_value AS REAL) including negatives",
        ))

        return seeds

    def _cast_to_decimal(self, schema: SchemaMetadata) -> List[SeedSQL]:
        """CAST AS DECIMAL 精度差异。"""
        seeds: List[SeedSQL] = []

        decimal_tests = [
            ("t_users", "score", "DECIMAL(5,2)"),
            ("t_products", "price", "DECIMAL(10,4)"),
            ("t_orders", "total_price", "DECIMAL(8,2)"),
            ("t_employees", "salary", "DECIMAL(12,2)"),
            ("t_transactions", "amount", "DECIMAL(10,3)"),
        ]
        for table, col, dtype in decimal_tests:
            seeds.append(self._seed(
                f"SELECT id, {col}, CAST({col} AS {dtype}) AS {col}_dec FROM {table} WHERE {col} IS NOT NULL ORDER BY id",
                tags=["cast", "decimal", table],
                desc=f"CAST({col} AS {dtype}) from {table}",
            ))

        # DECIMAL 在聚合中
        seeds.append(self._seed(
            "SELECT CAST(AVG(price) AS DECIMAL(8,2)) AS avg_price FROM t_products WHERE price IS NOT NULL",
            tags=["cast", "decimal", "aggregate"],
            desc="CAST(AVG(price) AS DECIMAL) precision",
        ))

        seeds.append(self._seed(
            "SELECT CAST(SUM(total_price) AS DECIMAL(12,2)) AS total FROM t_orders",
            tags=["cast", "decimal", "aggregate"],
            desc="CAST(SUM(total_price) AS DECIMAL) precision",
        ))

        return seeds

    def _cast_in_expressions(self, schema: SchemaMetadata) -> List[SeedSQL]:
        """CAST 在复杂表达式中的使用。"""
        seeds: List[SeedSQL] = []

        # CAST + COALESCE
        seeds.append(self._seed(
            "SELECT id, CAST(COALESCE(score, 0) AS INTEGER) AS score_int, CAST(COALESCE(age, 0) AS INTEGER) AS age_int FROM t_users ORDER BY id",
            tags=["cast", "integer", "coalesce"],
            desc="CAST(COALESCE(x, 0) AS INTEGER)",
        ))

        # CAST + CASE
        seeds.append(self._seed(
            "SELECT id, CAST(CASE WHEN price > 100 THEN price ELSE 0 END AS INTEGER) AS price_cat FROM t_products ORDER BY id",
            tags=["cast", "integer", "case"],
            desc="CAST(CASE WHEN ... END AS INTEGER)",
        ))

        # CAST + 聚合函数
        seeds.append(self._seed(
            "SELECT category, CAST(AVG(price) AS INTEGER) AS avg_int, CAST(SUM(price) AS INTEGER) AS sum_int FROM t_products WHERE category IS NOT NULL GROUP BY category ORDER BY category",
            tags=["cast", "integer", "aggregate"],
            desc="CAST(AVG/SUM AS INTEGER) in GROUP BY",
        ))

        # CAST + 子查询
        seeds.append(self._seed(
            "SELECT id, username, CAST(score AS INTEGER) AS score_int FROM t_users WHERE CAST(score AS INTEGER) > (SELECT CAST(AVG(score) AS INTEGER) FROM t_users WHERE score IS NOT NULL) ORDER BY id",
            tags=["cast", "integer", "subquery"],
            desc="CAST in subquery comparison",
        ))

        # CAST + JOIN
        seeds.append(self._seed(
            "SELECT u.id, u.username, CAST(u.score AS INTEGER) AS score_int, COUNT(o.id) AS orders FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id GROUP BY u.id, u.username, u.score ORDER BY u.id",
            tags=["cast", "integer", "join", "aggregate"],
            desc="CAST in JOIN + GROUP BY",
        ))

        # 多列 CAST 组合
        seeds.append(self._seed(
            "SELECT id, CAST(score AS INTEGER) + CAST(COALESCE(age, 0) AS INTEGER) AS combo_int FROM t_users ORDER BY id",
            tags=["cast", "integer", "multi"],
            desc="Multi-column CAST arithmetic",
        ))

        # CAST + NULLIF
        seeds.append(self._seed(
            "SELECT id, CAST(NULLIF(age, 0) AS INTEGER) AS safe_age FROM t_users ORDER BY id",
            tags=["cast", "integer", "nullif"],
            desc="CAST(NULLIF(age, 0) AS INTEGER)",
        ))

        # CAST 对日期计算
        seeds.append(self._seed(
            "SELECT id, CAST(score AS VARCHAR(10)) || '-' || CAST(age AS VARCHAR(10)) AS score_age_label FROM t_users WHERE score IS NOT NULL AND age IS NOT NULL ORDER BY id",
            tags=["cast", "varchar", "concat"],
            desc="Multi-column CAST + concatenation",
        ))

        # CAST 在窗口函数中
        seeds.append(self._seed(
            "SELECT id, user_id, CAST(SUM(total_price) OVER (PARTITION BY user_id ORDER BY id) AS INTEGER) AS running_int FROM t_orders ORDER BY user_id, id",
            tags=["cast", "integer", "window"],
            desc="CAST(window_function AS INTEGER)",
        ))

        return seeds
