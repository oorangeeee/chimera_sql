"""排序方言差异模板 — 测试 SQLite/Oracle ORDER BY 行为差异。

覆盖差异点：
- NULL 在排序中的位置（SQLite vs Oracle ASC/DESC 不同）
- 多列排序
- 表达式排序
- ORDER BY + LIMIT
- 排序方向差异
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class OrderingTemplate(SeedTemplate):
    """排序方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "ordering"

    @property
    def description(self) -> str:
        return "排序方言差异测试（NULL 排序位置、多列排序、表达式排序）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._null_ordering())
        seeds.extend(self._multi_column())
        seeds.extend(self._expression_order())
        return seeds

    # ── NULL 排序差异 (~10) ─────────────────────────────
    def _null_ordering(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, email FROM t_users ORDER BY email ASC, id",
                tags=["null_order", "email_asc"],
                desc="ORDER BY nullable ASC — NULL 位置因方言异",
            ),
            self._seed(
                "SELECT id, username, email FROM t_users ORDER BY email DESC, id",
                tags=["null_order", "email_desc"],
                desc="ORDER BY nullable DESC — NULL 位置因方言异",
            ),
            self._seed(
                "SELECT id, username, score FROM t_users ORDER BY score ASC, id",
                tags=["null_order", "score_asc"],
                desc="ORDER BY score ASC — NULL 在首或末",
            ),
            self._seed(
                "SELECT id, username, score FROM t_users ORDER BY score DESC, id",
                tags=["null_order", "score_desc"],
                desc="ORDER BY score DESC — NULL 在首或末",
            ),
            self._seed(
                "SELECT id, name, stock FROM t_products ORDER BY stock ASC, id",
                tags=["null_order", "stock_asc"],
                desc="ORDER BY stock ASC — NULL 位置差异",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees ORDER BY salary DESC, id",
                tags=["null_order", "salary_desc"],
                desc="ORDER BY salary DESC — NULL 位置差异",
            ),
            self._seed(
                "SELECT id, name, budget FROM t_departments ORDER BY budget ASC, id",
                tags=["null_order", "budget_asc"],
                desc="ORDER BY budget ASC — NULL 位置差异",
            ),
            self._seed(
                "SELECT id, name, hire_date FROM t_employees ORDER BY hire_date DESC, id",
                tags=["null_order", "hire_date_desc"],
                desc="ORDER BY hire_date DESC — NULL 日期",
            ),
            self._seed(
                "SELECT id, amount FROM t_transactions ORDER BY amount ASC, id",
                tags=["null_order", "amount_asc"],
                desc="ORDER BY amount ASC — NULL 位置",
            ),
            self._seed(
                "SELECT id, event_date FROM t_events ORDER BY event_date ASC, id",
                tags=["null_order", "event_date_asc"],
                desc="ORDER BY event_date ASC — NULL 日期",
            ),
        ]

    # ── 多列排序 (~10) ──────────────────────────────────
    def _multi_column(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, age, score FROM t_users "
                "ORDER BY age ASC, score DESC, id",
                tags=["multi_order", "age_score"],
                desc="ORDER BY 两列不同方向",
            ),
            self._seed(
                "SELECT id, name, category, price FROM t_products "
                "ORDER BY category ASC, price DESC, id",
                tags=["multi_order", "cat_price"],
                desc="ORDER BY category + price DESC",
            ),
            self._seed(
                "SELECT id, name, dept_id, salary FROM t_employees "
                "ORDER BY dept_id ASC, salary DESC, id",
                tags=["multi_order", "dept_salary"],
                desc="ORDER BY dept + salary DESC",
            ),
            self._seed(
                "SELECT id, event_type, event_date FROM t_events "
                "ORDER BY event_type ASC, event_date DESC, id",
                tags=["multi_order", "type_date"],
                desc="ORDER BY type + date DESC",
            ),
            self._seed(
                "SELECT id, status, amount FROM t_transactions "
                "ORDER BY status ASC, amount DESC, id",
                tags=["multi_order", "status_amount"],
                desc="ORDER BY status + amount DESC",
            ),
            self._seed(
                "SELECT id, username, score, age FROM t_users "
                "ORDER BY score DESC NULLS LAST, age ASC, id",
                tags=["multi_order", "nulls_last"],
                desc="NULLS LAST — 显式 NULL 排序位置",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id FROM t_employees "
                "ORDER BY dept_id ASC NULLS FIRST, salary DESC, id",
                tags=["multi_order", "nulls_first"],
                desc="NULLS FIRST — 显式 NULL 排序位置",
            ),
            self._seed(
                "SELECT e.id, e.name, d.name AS dept "
                "FROM t_employees e LEFT JOIN t_departments d ON e.dept_id = d.id "
                "ORDER BY d.name ASC, e.name ASC",
                tags=["multi_order", "join_order"],
                desc="JOIN + 多列排序",
            ),
            self._seed(
                "SELECT id, username, score, age FROM t_users "
                "ORDER BY COALESCE(score, 0) DESC, COALESCE(age, 0) ASC, id",
                tags=["multi_order", "coalesce_order"],
                desc="ORDER BY COALESCE — NULL 值排序控制",
            ),
            self._seed(
                "SELECT id, name, budget, location FROM t_departments "
                "ORDER BY parent_id ASC NULLS FIRST, budget DESC, id",
                tags=["multi_order", "dept_hierarchy"],
                desc="ORDER BY parent_id NULLS FIRST — 根部门优先",
            ),
        ]

    # ── 表达式排序 (~10) ────────────────────────────────
    def _expression_order(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score FROM t_users "
                "WHERE score IS NOT NULL ORDER BY ABS(score - 75) ASC, id",
                tags=["expr_order", "abs_deviation"],
                desc="ORDER BY ABS(score - 75) — 按偏差排序",
            ),
            self._seed(
                "SELECT id, name, price, stock FROM t_products "
                "ORDER BY price * COALESCE(stock, 0) DESC, id",
                tags=["expr_order", "inventory_value"],
                desc="ORDER BY 计算列 — 库存价值",
            ),
            self._seed(
                "SELECT id, username, LOWER(username) AS name_lower "
                "FROM t_users ORDER BY name_lower, id",
                tags=["expr_order", "lower_order"],
                desc="ORDER BY LOWER() — 大小写无关排序",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees "
                "WHERE salary IS NOT NULL "
                "ORDER BY salary / (SELECT MAX(salary) FROM t_employees) DESC, id",
                tags=["expr_order", "salary_ratio"],
                desc="ORDER BY 计算比例 — 薪资/最高薪资",
            ),
            self._seed(
                "SELECT id, username, score FROM t_users "
                "ORDER BY CASE WHEN score IS NULL THEN 1 ELSE 0 END, score DESC, id",
                tags=["expr_order", "null_last_case"],
                desc="CASE 排序 — NULL 排最后",
            ),
            self._seed(
                "SELECT id, name, price FROM t_products "
                "ORDER BY LENGTH(name) ASC, name, id",
                tags=["expr_order", "length_order"],
                desc="ORDER BY LENGTH(name) — 按名称长度",
            ),
            self._seed(
                "SELECT id, amount, ABS(amount) AS abs_amt "
                "FROM t_transactions ORDER BY abs_amt DESC, amount ASC, id",
                tags=["expr_order", "abs_order"],
                desc="ORDER BY ABS(amount) — 按绝对值",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees "
                "ORDER BY "
                "CASE WHEN salary > 100000 THEN 1 "
                "WHEN salary > 50000 THEN 2 "
                "WHEN salary IS NOT NULL THEN 3 ELSE 4 END, salary DESC, id",
                tags=["expr_order", "tier_order"],
                desc="CASE 分级排序",
            ),
            self._seed(
                "SELECT id, username, score FROM t_users "
                "ORDER BY COALESCE(score, -1) DESC, id",
                tags=["expr_order", "coalesce_order"],
                desc="ORDER BY COALESCE(score, -1) — NULL 视为 -1",
            ),
            self._seed(
                "SELECT id, name, budget FROM t_departments "
                "WHERE budget IS NOT NULL "
                "ORDER BY budget - COALESCE("
                "(SELECT SUM(e.salary) FROM t_employees e WHERE e.dept_id = t_departments.id), 0"
                ") DESC, id",
                tags=["expr_order", "budget_remaining"],
                desc="ORDER BY 子查询计算 — 预算余额",
            ),
        ]
