"""NULL 处理函数方言差异模板 — 测试 SQLite/Oracle NULL 函数行为差异。

覆盖差异点：
- IFNULL (SQLite) vs NVL (Oracle): SQLGlot 自动转译，需验证边界情况
- NULLIF: 两者语法一致，但类型处理有差异
- COALESCE: 参数数量、类型推导差异
- NULL 在聚合函数中: COUNT(col) vs COUNT(*) 语义一致但优化器行为不同
- NULL 排序: SQLite 中 NULL 排在最后（ASC/DESC 均如此），
  Oracle 中 NULL 排在最后（ASC）或最前（DESC）
- NULL + CASE: 条件表达式中 NULL 的三值逻辑
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class NullFunctionsTemplate(SeedTemplate):
    """NULL 处理函数方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "null_functions"

    @property
    def description(self) -> str:
        return "NULL 处理函数方言差异测试（IFNULL/NULLIF/COALESCE/COUNT/排序/CASE）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._ifnull_queries())
        seeds.extend(self._nullif_queries())
        seeds.extend(self._coalesce_queries())
        seeds.extend(self._null_in_count_queries())
        seeds.extend(self._null_ordering_queries())
        seeds.extend(self._null_case_queries())
        return seeds

    # ── IFNULL (~10) ────────────────────────────────────
    def _ifnull_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, IFNULL(email, 'no_email') AS email_display "
                "FROM t_users ORDER BY id",
                tags=["ifnull", "ifnull_email"],
                desc="IFNULL 对 email 列 — VARCHAR 默认值",
            ),
            self._seed(
                "SELECT id, username, IFNULL(age, 0) AS age_filled "
                "FROM t_users ORDER BY id",
                tags=["ifnull", "ifnull_age"],
                desc="IFNULL 对 age 列 — INTEGER 默认值",
            ),
            self._seed(
                "SELECT id, username, IFNULL(score, 0) AS score_filled "
                "FROM t_users ORDER BY id",
                tags=["ifnull", "ifnull_score"],
                desc="IFNULL 对 score 列 — DECIMAL 默认值",
            ),
            self._seed(
                "SELECT id, name, IFNULL(stock, 0) AS stock_filled "
                "FROM t_products ORDER BY id",
                tags=["ifnull", "ifnull_stock"],
                desc="IFNULL 对 stock 列",
            ),
            self._seed(
                "SELECT id, name, IFNULL(category, 'uncategorized') AS category_filled "
                "FROM t_products ORDER BY id",
                tags=["ifnull", "ifnull_category"],
                desc="IFNULL 对 category 列",
            ),
            self._seed(
                "SELECT id, name, IFNULL(salary, 0) AS salary_filled "
                "FROM t_employees ORDER BY id",
                tags=["ifnull", "ifnull_salary"],
                desc="IFNULL 对 salary 列",
            ),
            self._seed(
                "SELECT id, name, IFNULL(bio, 'No bio available') AS bio_display "
                "FROM t_employees ORDER BY id",
                tags=["ifnull", "ifnull_bio"],
                desc="IFNULL 对 bio 列 — 长文本默认值",
            ),
            self._seed(
                "SELECT id, name, IFNULL(location, 'unknown') AS location_filled "
                "FROM t_departments ORDER BY id",
                tags=["ifnull", "ifnull_location"],
                desc="IFNULL 对 location 列",
            ),
            self._seed(
                "SELECT id, IFNULL(amount, 0) AS amount_filled "
                "FROM t_transactions ORDER BY id",
                tags=["ifnull", "ifnull_amount"],
                desc="IFNULL 对 amount 列",
            ),
            self._seed(
                "SELECT id, IFNULL(status, 'pending') AS status_filled "
                "FROM t_transactions ORDER BY id",
                tags=["ifnull", "ifnull_status"],
                desc="IFNULL 对 status 列",
            ),
        ]

    # ── NULLIF (~8) ─────────────────────────────────────
    def _nullif_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, NULLIF(email, '') AS email_nonempty "
                "FROM t_users ORDER BY id",
                tags=["nullif", "nullif_empty_string"],
                desc="NULLIF(col, '') — 空字符串转 NULL",
            ),
            self._seed(
                "SELECT id, username, NULLIF(score, 0) AS score_nonzero "
                "FROM t_users ORDER BY id",
                tags=["nullif", "nullif_zero"],
                desc="NULLIF(score, 0) — 零值转 NULL",
            ),
            self._seed(
                "SELECT id, name, NULLIF(price, 0) AS price_nonzero "
                "FROM t_products ORDER BY id",
                tags=["nullif", "nullif_price_zero"],
                desc="NULLIF(price, 0) — 价格为零转 NULL",
            ),
            self._seed(
                "SELECT id, name, NULLIF(stock, 0) AS stock_nonzero "
                "FROM t_products ORDER BY id",
                tags=["nullif", "nullif_stock_zero"],
                desc="NULLIF(stock, 0) — 库存为零转 NULL",
            ),
            self._seed(
                "SELECT id, name, NULLIF(category, 'misc') AS cat_not_misc "
                "FROM t_products ORDER BY id",
                tags=["nullif", "nullif_category"],
                desc="NULLIF(category, 'misc') — 特定值转 NULL",
            ),
            self._seed(
                "SELECT id, username, NULLIF(age, 0) AS age_nonzero "
                "FROM t_users ORDER BY id",
                tags=["nullif", "nullif_age_zero"],
                desc="NULLIF(age, 0)",
            ),
            self._seed(
                "SELECT id, name, NULLIF(status, '') AS status_nonempty "
                "FROM t_employees ORDER BY id",
                tags=["nullif", "nullif_status_empty"],
                desc="NULLIF(status, '') — 空状态转 NULL",
            ),
            self._seed(
                "SELECT id, NULLIF(tx_type, '') AS tx_type_nonempty, "
                "NULLIF(status, '') AS status_nonempty "
                "FROM t_transactions ORDER BY id",
                tags=["nullif", "nullif_multi_col"],
                desc="多列 NULLIF 组合",
            ),
        ]

    # ── COALESCE 多参数 (~10) ───────────────────────────
    def _coalesce_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, COALESCE(email, 'no_email') AS email_filled "
                "FROM t_users ORDER BY id",
                tags=["coalesce", "coalesce_2arg_email"],
                desc="COALESCE 2 参数 — email",
            ),
            self._seed(
                "SELECT id, username, COALESCE(score, 0) AS score_filled "
                "FROM t_users ORDER BY id",
                tags=["coalesce", "coalesce_2arg_score"],
                desc="COALESCE 2 参数 — score",
            ),
            self._seed(
                "SELECT id, name, COALESCE(stock, 0) AS stock_filled "
                "FROM t_products ORDER BY id",
                tags=["coalesce", "coalesce_2arg_stock"],
                desc="COALESCE 2 参数 — stock",
            ),
            self._seed(
                "SELECT id, name, COALESCE(category, 'N/A') AS cat_filled "
                "FROM t_products ORDER BY id",
                tags=["coalesce", "coalesce_2arg_category"],
                desc="COALESCE 2 参数 — category",
            ),
            self._seed(
                "SELECT id, COALESCE(amount, 0) AS amount_filled "
                "FROM t_transactions ORDER BY id",
                tags=["coalesce", "coalesce_2arg_amount"],
                desc="COALESCE 2 参数 — amount",
            ),
            self._seed(
                "SELECT id, name, COALESCE(salary, 0) AS salary_filled "
                "FROM t_employees ORDER BY id",
                tags=["coalesce", "coalesce_2arg_salary"],
                desc="COALESCE 2 参数 — salary",
            ),
            self._seed(
                "SELECT id, username, "
                "COALESCE(score, age, 0) AS first_non_null "
                "FROM t_users ORDER BY id",
                tags=["coalesce", "coalesce_3arg_score_age"],
                desc="COALESCE 3 参数 — score, age, 0",
            ),
            self._seed(
                "SELECT id, name, "
                "COALESCE(stock, price, 0) AS first_non_null "
                "FROM t_products ORDER BY id",
                tags=["coalesce", "coalesce_3arg_stock_price"],
                desc="COALESCE 3 参数 — stock, price, 0（类型混合）",
            ),
            self._seed(
                "SELECT id, name, "
                "COALESCE(location, category, 'unknown') AS first_non_null "
                "FROM t_departments ORDER BY id",
                tags=["coalesce", "coalesce_3arg_dept"],
                desc="COALESCE 3 参数 — location, category, literal",
            ),
            self._seed(
                "SELECT id, username, "
                "COALESCE(email, CAST(age AS VARCHAR(20)), 'N/A') AS contact "
                "FROM t_users ORDER BY id",
                tags=["coalesce", "coalesce_cast"],
                desc="COALESCE 含 CAST — 类型推导差异",
            ),
        ]

    # ── NULL in COUNT (~8) ──────────────────────────────
    def _null_in_count_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT COUNT(*) AS total_rows, COUNT(email) AS non_null_email, "
                "COUNT(*) - COUNT(email) AS null_email_count "
                "FROM t_users",
                tags=["count_null", "count_email_vs_star"],
                desc="COUNT(*) vs COUNT(email) — NULL 计数差异",
            ),
            self._seed(
                "SELECT COUNT(*) AS total_rows, COUNT(score) AS non_null_score, "
                "COUNT(*) - COUNT(score) AS null_score_count "
                "FROM t_users",
                tags=["count_null", "count_score_vs_star"],
                desc="COUNT(*) vs COUNT(score)",
            ),
            self._seed(
                "SELECT COUNT(*) AS total_rows, COUNT(stock) AS non_null_stock "
                "FROM t_products",
                tags=["count_null", "count_stock"],
                desc="COUNT(*) vs COUNT(stock)",
            ),
            self._seed(
                "SELECT COUNT(*) AS total_rows, COUNT(salary) AS non_null_salary "
                "FROM t_employees",
                tags=["count_null", "count_salary"],
                desc="COUNT(*) vs COUNT(salary)",
            ),
            self._seed(
                "SELECT COUNT(*) AS total_rows, COUNT(amount) AS non_null_amount "
                "FROM t_transactions",
                tags=["count_null", "count_amount"],
                desc="COUNT(*) vs COUNT(amount)",
            ),
            self._seed(
                "SELECT COUNT(DISTINCT category) AS distinct_cats, "
                "COUNT(category) AS non_null_cats, "
                "COUNT(*) AS total_products "
                "FROM t_products",
                tags=["count_null", "count_distinct_category"],
                desc="COUNT(DISTINCT) vs COUNT(col) vs COUNT(*)",
            ),
            self._seed(
                "SELECT COUNT(DISTINCT score) AS distinct_scores, "
                "COUNT(score) AS non_null_scores, "
                "COUNT(*) AS total_users "
                "FROM t_users",
                tags=["count_null", "count_distinct_score"],
                desc="COUNT(DISTINCT score) — NULL 不参与去重",
            ),
            self._seed(
                "SELECT COUNT(DISTINCT status) AS distinct_statuses, "
                "COUNT(status) AS non_null_statuses, "
                "COUNT(*) AS total_orders "
                "FROM t_orders",
                tags=["count_null", "count_distinct_status"],
                desc="COUNT(DISTINCT status) — 订单状态",
            ),
        ]

    # ── NULL 排序 (~8) ─────────────────────────────────
    def _null_ordering_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, email FROM t_users ORDER BY email ASC, id",
                tags=["null_order", "order_email_asc"],
                desc="ORDER BY nullable email ASC — NULL 位置因方言而异",
            ),
            self._seed(
                "SELECT id, username, email FROM t_users ORDER BY email DESC, id",
                tags=["null_order", "order_email_desc"],
                desc="ORDER BY nullable email DESC — NULL 位置因方言而异",
            ),
            self._seed(
                "SELECT id, username, score FROM t_users ORDER BY score ASC, id",
                tags=["null_order", "order_score_asc"],
                desc="ORDER BY nullable score ASC",
            ),
            self._seed(
                "SELECT id, username, score FROM t_users ORDER BY score DESC, id",
                tags=["null_order", "order_score_desc"],
                desc="ORDER BY nullable score DESC",
            ),
            self._seed(
                "SELECT id, name, stock FROM t_products ORDER BY stock ASC, id",
                tags=["null_order", "order_stock_asc"],
                desc="ORDER BY nullable stock ASC",
            ),
            self._seed(
                "SELECT id, name, stock FROM t_products ORDER BY stock DESC, id",
                tags=["null_order", "order_stock_desc"],
                desc="ORDER BY nullable stock DESC",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees ORDER BY salary ASC, id",
                tags=["null_order", "order_salary_asc"],
                desc="ORDER BY nullable salary ASC",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees ORDER BY salary DESC, id",
                tags=["null_order", "order_salary_desc"],
                desc="ORDER BY nullable salary DESC",
            ),
        ]

    # ── NULL + CASE (~6) ────────────────────────────────
    def _null_case_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, "
                "CASE WHEN email IS NULL THEN 'missing' ELSE 'provided' END AS email_status "
                "FROM t_users ORDER BY id",
                tags=["null_case", "case_email_null"],
                desc="CASE WHEN email IS NULL — NULL 检测",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN score IS NULL THEN 0 ELSE score END AS score_or_zero "
                "FROM t_users ORDER BY id",
                tags=["null_case", "case_score_null"],
                desc="CASE 替代 IFNULL — score",
            ),
            self._seed(
                "SELECT id, name, "
                "CASE WHEN stock IS NULL THEN 'unknown' "
                "WHEN stock = 0 THEN 'out_of_stock' "
                "ELSE 'available' END AS stock_status "
                "FROM t_products ORDER BY id",
                tags=["null_case", "case_stock_tier"],
                desc="CASE 多条件含 NULL — 三值逻辑",
            ),
            self._seed(
                "SELECT id, name, "
                "CASE WHEN salary IS NULL THEN 'no_salary' "
                "WHEN salary < 3000 THEN 'junior' "
                "WHEN salary < 8000 THEN 'mid' "
                "ELSE 'senior' END AS salary_level "
                "FROM t_employees ORDER BY id",
                tags=["null_case", "case_salary_level"],
                desc="CASE 含 NULL 条件 — 薪资分级",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN score IS NULL AND age IS NULL THEN 'both_null' "
                "WHEN score IS NULL THEN 'score_null' "
                "WHEN age IS NULL THEN 'age_null' "
                "ELSE 'both_present' END AS null_profile "
                "FROM t_users ORDER BY id",
                tags=["null_case", "case_multi_null"],
                desc="CASE 多列 NULL 检测 — 组合判断",
            ),
            self._seed(
                "SELECT id, COALESCE(email, 'N/A') AS email_filled, "
                "CASE WHEN email IS NULL THEN 1 ELSE 0 END AS is_email_null, "
                "COALESCE(score, 0) AS score_filled, "
                "CASE WHEN score IS NULL THEN 1 ELSE 0 END AS is_score_null "
                "FROM t_users ORDER BY id",
                tags=["null_case", "case_coalesce_combined"],
                desc="IFNULL/COALESCE + CASE 组合 — 多列 NULL 处理",
            ),
        ]
