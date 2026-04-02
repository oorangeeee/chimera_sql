"""嵌套函数方言差异模板 — 测试 SQLite/Oracle 嵌套函数行为差异。

覆盖差异点：
- 函数嵌套如 ROUND(AVG(...))
- 多层嵌套 COALESCE(ABS(...), ...)
- 聚合函数嵌套
- 类型转换 + 函数嵌套
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class NestedFuncsTemplate(SeedTemplate):
    """嵌套函数方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "nested_funcs"

    @property
    def description(self) -> str:
        return "嵌套函数方言差异测试（ROUND(AVG)/COALESCE(ABS)/CAST+函数）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._round_agg())
        seeds.extend(self._nested_string())
        seeds.extend(self._nested_numeric())
        seeds.extend(self._nested_cast())
        return seeds

    # ── ROUND + 聚合 (~8) ───────────────────────────────
    def _round_agg(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT ROUND(AVG(score), 2) AS avg_score FROM t_users WHERE score IS NOT NULL",
                tags=["round_agg", "round_avg_score"],
                desc="ROUND(AVG(score)) — 平均分保留两位",
            ),
            self._seed(
                "SELECT ROUND(AVG(salary), 2) AS avg_salary FROM t_employees WHERE salary IS NOT NULL",
                tags=["round_agg", "round_avg_salary"],
                desc="ROUND(AVG(salary)) — 平均薪资",
            ),
            self._seed(
                "SELECT dept_id, ROUND(AVG(salary), 2) AS avg_sal, "
                "ROUND(SUM(salary), 0) AS total_sal "
                "FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL "
                "GROUP BY dept_id ORDER BY dept_id",
                tags=["round_agg", "dept_avg_total"],
                desc="ROUND + AVG/SUM — 部门统计",
            ),
            self._seed(
                "SELECT category, ROUND(AVG(price), 2) AS avg_price, "
                "ROUND(MAX(price) - MIN(price), 2) AS price_range "
                "FROM t_products WHERE category IS NOT NULL "
                "GROUP BY category ORDER BY category",
                tags=["round_agg", "cat_price_stats"],
                desc="ROUND + MAX/MIN — 分类价格统计",
            ),
            self._seed(
                "SELECT status, ROUND(AVG(amount), 2) AS avg_amount, "
                "ROUND(SUM(amount), 2) AS total_amount "
                "FROM t_transactions WHERE status IS NOT NULL AND amount IS NOT NULL "
                "GROUP BY status ORDER BY status",
                tags=["round_agg", "tx_status_stats"],
                desc="ROUND + 聚合 — 交易状态统计",
            ),
            self._seed(
                "SELECT ROUND(AVG(price), 0) AS avg_int, "
                "ROUND(AVG(price), 2) AS avg_dec, "
                "AVG(price) AS avg_raw FROM t_products",
                tags=["round_agg", "round_precision"],
                desc="ROUND 不同精度 — 整数 vs 小数",
            ),
            self._seed(
                "SELECT u.username, ROUND(AVG(o.total_amount), 2) AS avg_order "
                "FROM t_users u JOIN t_orders o ON u.id = o.user_id "
                "GROUP BY u.id, u.username ORDER BY avg_order DESC",
                tags=["round_agg", "user_avg_order"],
                desc="JOIN + ROUND + AVG — 用户平均订单额",
            ),
            self._seed(
                "SELECT d.name, ROUND(AVG(e.salary), 2) AS avg_sal "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name ORDER BY avg_sal DESC",
                tags=["round_agg", "dept_avg_left"],
                desc="LEFT JOIN + ROUND(AVG) — 含空部门",
            ),
        ]

    # ── 嵌套字符串函数 (~8) ─────────────────────────────
    def _nested_string(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, UPPER(TRIM(username)) AS clean_name FROM t_users ORDER BY id",
                tags=["nested_str", "upper_trim"],
                desc="UPPER(TRIM()) — 先去空格再大写",
            ),
            self._seed(
                "SELECT id, UPPER(SUBSTR(username, 1, 3)) AS short_name FROM t_users ORDER BY id",
                tags=["nested_str", "upper_substr"],
                desc="UPPER(SUBSTR()) — 截取后大写",
            ),
            self._seed(
                "SELECT id, LENGTH(TRIM(username)) AS name_len FROM t_users ORDER BY id",
                tags=["nested_str", "length_trim"],
                desc="LENGTH(TRIM()) — 去空格后长度",
            ),
            self._seed(
                "SELECT id, REPLACE(UPPER(email), '@', ' AT ') AS masked_email "
                "FROM t_users WHERE email IS NOT NULL ORDER BY id",
                tags=["nested_str", "replace_upper"],
                desc="REPLACE(UPPER()) — 大写后替换",
            ),
            self._seed(
                "SELECT id, SUBSTR(username, 1, INSTR(username, 'a')) AS before_a "
                "FROM t_users WHERE username LIKE '%a%' ORDER BY id",
                tags=["nested_str", "substr_instr"],
                desc="SUBSTR + INSTR — 按字符位置截取",
            ),
            self._seed(
                "SELECT id, COALESCE(NULLIF(TRIM(name), ''), 'unnamed') AS clean_name "
                "FROM t_departments ORDER BY id",
                tags=["nested_str", "coalesce_nullif_trim"],
                desc="COALESCE(NULLIF(TRIM())) — 三层嵌套",
            ),
            self._seed(
                "SELECT id, LOWER(SUBSTR(name, 1, 1)) || LOWER(SUBSTR(name, 2)) AS lower_name "
                "FROM t_employees ORDER BY id",
                tags=["nested_str", "lower_concat"],
                desc="LOWER + SUBSTR + 拼接",
            ),
            self._seed(
                "SELECT id, TRIM(REPLACE(name, '  ', ' ')) AS normalized "
                "FROM t_departments ORDER BY id",
                tags=["nested_str", "trim_replace"],
                desc="TRIM(REPLACE()) — 去多余空格",
            ),
        ]

    # ── 嵌套数值函数 (~7) ────────────────────────────────
    def _nested_numeric(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, ROUND(ABS(salary), 2) AS abs_salary "
                "FROM t_employees WHERE salary IS NOT NULL ORDER BY id",
                tags=["nested_num", "round_abs"],
                desc="ROUND(ABS()) — 绝对值后四舍五入",
            ),
            self._seed(
                "SELECT id, ABS(ROUND(amount, 0)) AS rounded_abs "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY id",
                tags=["nested_num", "abs_round"],
                desc="ABS(ROUND()) — 四舍五入后绝对值",
            ),
            self._seed(
                "SELECT id, COALESCE(ABS(score), 0) AS safe_score "
                "FROM t_users ORDER BY id",
                tags=["nested_num", "coalesce_abs"],
                desc="COALESCE(ABS()) — 安全取绝对值",
            ),
            self._seed(
                "SELECT id, ROUND(price * COALESCE(stock, 0), 2) AS inv_value "
                "FROM t_products ORDER BY id",
                tags=["nested_num", "round_multiply_coalesce"],
                desc="ROUND(COALESCE() * col) — 嵌套计算",
            ),
            self._seed(
                "SELECT id, CEIL(ABS(amount)) AS ceil_abs "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY id",
                tags=["nested_num", "ceil_abs"],
                desc="CEIL(ABS()) — 绝对值向上取整",
            ),
            self._seed(
                "SELECT id, FLOOR(salary / 1000) * 1000 AS salary_band "
                "FROM t_employees WHERE salary IS NOT NULL ORDER BY id",
                tags=["nested_num", "floor_band"],
                desc="FLOOR(除法) * 1000 — 薪资分段",
            ),
            self._seed(
                "SELECT id, ROUND(AVG(score) OVER (PARTITION BY age), 2) AS avg_by_age "
                "FROM t_users WHERE score IS NOT NULL AND age IS NOT NULL ORDER BY id",
                tags=["nested_num", "round_window"],
                desc="ROUND + 窗口函数 AVG",
            ),
        ]

    # ── 嵌套 CAST (~7) ──────────────────────────────────
    def _nested_cast(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, CAST(ROUND(AVG(score), 0) AS INTEGER) AS avg_int "
                "FROM t_users WHERE score IS NOT NULL",
                tags=["nested_cast", "cast_round_avg"],
                desc="CAST(ROUND(AVG())) — 三层嵌套",
            ),
            self._seed(
                "SELECT id, CAST(SUBSTR(CAST(salary AS VARCHAR(20)), 1, 6) AS DECIMAL(10,2)) AS truncated "
                "FROM t_employees WHERE salary IS NOT NULL ORDER BY id",
                tags=["nested_cast", "cast_substr_cast"],
                desc="CAST(SUBSTR(CAST())) — 三重类型转换",
            ),
            self._seed(
                "SELECT id, CAST(ROUND(price * COALESCE(stock, 0), 0) AS INTEGER) AS inv_int "
                "FROM t_products ORDER BY id",
                tags=["nested_cast", "cast_round_calc"],
                desc="CAST(ROUND(计算)) — 类型转换嵌套",
            ),
            self._seed(
                "SELECT id, COALESCE(CAST(score AS VARCHAR(10)), 'N/A') AS score_str "
                "FROM t_users ORDER BY id",
                tags=["nested_cast", "coalesce_cast"],
                desc="COALESCE(CAST()) — 安全类型转换",
            ),
            self._seed(
                "SELECT id, CAST(ABS(amount) AS INTEGER) AS abs_int "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY id",
                tags=["nested_cast", "cast_abs"],
                desc="CAST(ABS()) — 绝对值转整数",
            ),
            self._seed(
                "SELECT id, ROUND(CAST(score AS REAL), 1) AS rounded_score "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["nested_cast", "round_cast_real"],
                desc="ROUND(CAST(REAL)) — 类型转换后四舍五入",
            ),
            self._seed(
                "SELECT id, CAST(LENGTH(name) AS VARCHAR(10)) || ' chars' AS name_info "
                "FROM t_departments ORDER BY id",
                tags=["nested_cast", "cast_length_concat"],
                desc="CAST(LENGTH()) + 拼接",
            ),
        ]
