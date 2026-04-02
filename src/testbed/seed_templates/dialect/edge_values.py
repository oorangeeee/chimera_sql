"""边界值测试方言差异模板 — 测试 SQLite/Oracle 边界值处理差异。

覆盖差异点：
- 0 值（除零、乘零、比较）
- 负数（ABS、ROUND、排序）
- NULL 边界（各种操作与 NULL 交互）
- 空字符串（Oracle ''=NULL vs SQLite ''≠NULL）
- 极端值（大数、极小数）
- Unicode（中文、特殊字符）
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class EdgeValuesTemplate(SeedTemplate):
    """边界值测试方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "edge_values"

    @property
    def description(self) -> str:
        return "边界值方言差异测试（0/负数/NULL/空字符串/极端值/Unicode）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._zero_values())
        seeds.extend(self._negative_values())
        seeds.extend(self._null_boundary())
        seeds.extend(self._empty_string())
        seeds.extend(self._extreme_values())
        return seeds

    # ── 零值 (~6) ──────────────────────────────────────
    def _zero_values(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, name, salary FROM t_employees "
                "WHERE salary = 0 OR salary IS NULL ORDER BY id",
                tags=["zero", "salary_zero_or_null"],
                desc="salary = 0 OR IS NULL — 零与 NULL",
            ),
            self._seed(
                "SELECT id, name, price * 0 AS zero_price FROM t_products ORDER BY id",
                tags=["zero", "multiply_zero"],
                desc="price * 0 — 乘零",
            ),
            self._seed(
                "SELECT id, name, stock FROM t_products "
                "WHERE stock = 0 ORDER BY id",
                tags=["zero", "stock_zero"],
                desc="stock = 0 — 零库存",
            ),
            self._seed(
                "SELECT id, amount FROM t_transactions "
                "WHERE amount = 0 ORDER BY id",
                tags=["zero", "amount_zero"],
                desc="amount = 0 — 零金额交易",
            ),
            self._seed(
                "SELECT id, name, budget FROM t_departments "
                "WHERE budget = 0 OR budget < 0 ORDER BY id",
                tags=["zero", "budget_zero_neg"],
                desc="budget <= 0 — 零或负预算",
            ),
            self._seed(
                "SELECT id, name, COALESCE(stock, 0) + 0 AS safe_stock "
                "FROM t_products ORDER BY id",
                tags=["zero", "coalesce_zero"],
                desc="COALESCE + 0 — NULL 转 0 加法",
            ),
        ]

    # ── 负数 (~6) ──────────────────────────────────────
    def _negative_values(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, name, salary, ABS(salary) AS abs_salary "
                "FROM t_employees WHERE salary < 0 ORDER BY id",
                tags=["negative", "negative_salary"],
                desc="salary < 0 — 负数工资",
            ),
            self._seed(
                "SELECT id, amount, ABS(amount) AS abs_amount "
                "FROM t_transactions WHERE amount < 0 ORDER BY id",
                tags=["negative", "negative_amount"],
                desc="amount < 0 — 负数金额（退款）",
            ),
            self._seed(
                "SELECT id, name, budget, ABS(budget) AS abs_budget "
                "FROM t_departments WHERE budget < 0 ORDER BY id",
                tags=["negative", "negative_budget"],
                desc="budget < 0 — 负预算",
            ),
            self._seed(
                "SELECT id, name, salary, "
                "CASE WHEN salary < 0 THEN 'negative' "
                "WHEN salary = 0 THEN 'zero' "
                "WHEN salary > 0 THEN 'positive' "
                "ELSE 'null' END AS salary_sign "
                "FROM t_employees ORDER BY id",
                tags=["negative", "salary_sign"],
                desc="CASE — 正/零/负/NULL 分类",
            ),
            self._seed(
                "SELECT id, amount, ROUND(amount, 0) AS rounded "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY ABS(amount)",
                tags=["negative", "round_negative"],
                desc="ROUND 负数 — 银行家舍入 vs 标准",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees "
                "WHERE salary IS NOT NULL ORDER BY salary ASC, id",
                tags=["negative", "order_neg_pos"],
                desc="ORDER BY 含负数 — 排序行为",
            ),
        ]

    # ── NULL 边界 (~6) ──────────────────────────────────
    def _null_boundary(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, NULL + score AS null_plus, "
                "score + NULL AS score_plus_null, "
                "NULL * 10 AS null_times "
                "FROM t_users ORDER BY id",
                tags=["null_boundary", "null_arithmetic"],
                desc="NULL + col / col + NULL / NULL * N — NULL 传播",
            ),
            self._seed(
                "SELECT id, NULL = NULL AS null_eq_null, "
                "NULL <> NULL AS null_neq_null, "
                "NULL > 0 AS null_gt_zero "
                "FROM t_users ORDER BY id LIMIT 1",
                tags=["null_boundary", "null_comparison"],
                desc="NULL = NULL / NULL <> NULL — 比较结果",
            ),
            self._seed(
                "SELECT id, COALESCE(NULL, NULL, NULL, 42) AS deep_coalesce "
                "FROM t_users ORDER BY id LIMIT 1",
                tags=["null_boundary", "deep_coalesce"],
                desc="COALESCE 多 NULL — 第一个非 NULL",
            ),
            self._seed(
                "SELECT id, NULLIF(score, score) AS same_val, "
                "NULLIF(score, -1) AS diff_val "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["null_boundary", "nullif_self"],
                desc="NULLIF(col, col) — 相同值返回 NULL",
            ),
            self._seed(
                "SELECT id, CASE NULL WHEN NULL THEN 'match' ELSE 'no_match' END AS null_case "
                "FROM t_users ORDER BY id LIMIT 1",
                tags=["null_boundary", "case_null"],
                desc="CASE NULL WHEN NULL — NULL 不等于 NULL",
            ),
            self._seed(
                "SELECT id, username, "
                "NULL OR score > 0 AS or_null, "
                "NULL AND score > 0 AS and_null "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["null_boundary", "logic_null"],
                desc="NULL OR TRUE / NULL AND TRUE — 三值逻辑",
            ),
        ]

    # ── 空字符串 (~6) ──────────────────────────────────
    def _empty_string(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, name, bio FROM t_employees "
                "WHERE bio = '' OR bio IS NULL ORDER BY id",
                tags=["empty_str", "bio_empty_or_null"],
                desc="bio = '' OR IS NULL — Oracle ''=NULL 差异",
            ),
            self._seed(
                "SELECT id, name, "
                "CASE WHEN bio = '' THEN 'empty' "
                "WHEN bio IS NULL THEN 'null' "
                "ELSE 'has_content' END AS bio_status "
                "FROM t_employees ORDER BY id",
                tags=["empty_str", "bio_case_empty_null"],
                desc="CASE 区分空字符串和 NULL",
            ),
            self._seed(
                "SELECT id, name, LENGTH(bio) AS bio_len "
                "FROM t_employees ORDER BY id",
                tags=["empty_str", "length_empty_null"],
                desc="LENGTH('') vs LENGTH(NULL) — 差异行为",
            ),
            self._seed(
                "SELECT id, name, "
                "CASE WHEN COALESCE(bio, '') = '' THEN 'empty_or_null' "
                "ELSE 'has_content' END AS bio_check "
                "FROM t_employees ORDER BY id",
                tags=["empty_str", "coalesce_empty"],
                desc="COALESCE(bio, '') = '' — 空/NULL 合并检测",
            ),
            self._seed(
                "SELECT id, name, bio || 'suffix' AS bio_with_suffix "
                "FROM t_employees ORDER BY id",
                tags=["empty_str", "concat_empty_null"],
                desc="'' || 'suffix' vs NULL || 'suffix' — 拼接差异",
            ),
            self._seed(
                "SELECT id, name, "
                "NULLIF(bio, '') AS bio_nullif_empty "
                "FROM t_employees ORDER BY id",
                tags=["empty_str", "nullif_empty"],
                desc="NULLIF(bio, '') — 空字符串转 NULL",
            ),
        ]

    # ── 极端值 (~6) ────────────────────────────────────
    def _extreme_values(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, name, budget FROM t_departments "
                "WHERE budget > 400000 OR budget < 0 ORDER BY budget DESC",
                tags=["extreme", "budget_extremes"],
                desc="极端预算 — 很大或负数",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees "
                "WHERE salary > 150000 OR salary <= 0 ORDER BY salary DESC",
                tags=["extreme", "salary_extremes"],
                desc="极端薪资 — 很高或零/负",
            ),
            self._seed(
                "SELECT id, event_date FROM t_events "
                "WHERE event_date = DATE('1970-01-01') OR event_date >= DATE('2099-01-01') "
                "ORDER BY event_date",
                tags=["extreme", "date_extremes"],
                desc="极端日期 — epoch 和远未来",
            ),
            self._seed(
                "SELECT id, amount FROM t_transactions "
                "WHERE ABS(amount) >= 99999 ORDER BY ABS(amount) DESC",
                tags=["extreme", "amount_extremes"],
                desc="极端金额 — 接近最大值",
            ),
            self._seed(
                "SELECT id, name, price, "
                "CAST(price AS INTEGER) AS price_int, "
                "CAST(price AS REAL) AS price_real "
                "FROM t_products WHERE price IS NOT NULL ORDER BY price DESC",
                tags=["extreme", "cast_precision"],
                desc="CAST 精度 — 大数转换精度差异",
            ),
            self._seed(
                "SELECT id, username, score, "
                "CAST(score AS INTEGER) AS score_int, "
                "CAST(score AS VARCHAR(5)) AS score_str "
                "FROM t_users WHERE score IS NOT NULL ORDER BY score DESC",
                tags=["extreme", "type_precision"],
                desc="类型转换精度 — 截断 vs 四舍五入",
            ),
        ]
