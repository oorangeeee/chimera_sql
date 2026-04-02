"""LIMIT/OFFSET 方言差异模板 — 测试 SQLite/Oracle 分页语法差异。

覆盖差异点：
- SQLite: LIMIT N / LIMIT N OFFSET M
- Oracle: FETCH FIRST N ROWS ONLY / OFFSET M ROWS FETCH NEXT N ROWS ONLY
- SQLGlot 自动转译，需验证各种参数组合
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class LimitOffsetTemplate(SeedTemplate):
    """LIMIT/OFFSET 方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "limit_offset"

    @property
    def description(self) -> str:
        return "LIMIT/OFFSET 方言差异测试（分页语法转译）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._basic_limit())
        seeds.extend(self._limit_offset())
        seeds.extend(self._limit_with_order())
        seeds.extend(self._limit_subquery())
        seeds.extend(self._limit_aggregate())
        return seeds

    # ── 基本 LIMIT (~8) ─────────────────────────────────
    def _basic_limit(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users ORDER BY id LIMIT 5",
                tags=["limit", "limit_basic"],
                desc="基本 LIMIT 5",
            ),
            self._seed(
                "SELECT id, username FROM t_users ORDER BY id LIMIT 1",
                tags=["limit", "limit_one"],
                desc="LIMIT 1 — 单行",
            ),
            self._seed(
                "SELECT id, username FROM t_users ORDER BY id LIMIT 10",
                tags=["limit", "limit_ten"],
                desc="LIMIT 10",
            ),
            self._seed(
                "SELECT id, name, price FROM t_products ORDER BY price DESC LIMIT 3",
                tags=["limit", "limit_top_price"],
                desc="LIMIT + ORDER BY DESC — Top 3 价格",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees ORDER BY salary DESC LIMIT 5",
                tags=["limit", "limit_top_salary"],
                desc="LIMIT + ORDER BY DESC — Top 5 薪资",
            ),
            self._seed(
                "SELECT id, event_type FROM t_events ORDER BY id LIMIT 15",
                tags=["limit", "limit_events"],
                desc="LIMIT 15 — events",
            ),
            self._seed(
                "SELECT id, amount FROM t_transactions ORDER BY amount DESC LIMIT 5",
                tags=["limit", "limit_top_amount"],
                desc="LIMIT + ORDER BY DESC — Top 5 交易额",
            ),
            self._seed(
                "SELECT id, tag FROM t_tags ORDER BY id LIMIT 20",
                tags=["limit", "limit_tags"],
                desc="LIMIT 20 — tags",
            ),
        ]

    # ── LIMIT + OFFSET (~10) ─────────────────────────────
    def _limit_offset(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users ORDER BY id LIMIT 5 OFFSET 0",
                tags=["limit_offset", "offset_zero"],
                desc="LIMIT 5 OFFSET 0 — 等价于无 OFFSET",
            ),
            self._seed(
                "SELECT id, username FROM t_users ORDER BY id LIMIT 5 OFFSET 5",
                tags=["limit_offset", "offset_page2"],
                desc="LIMIT 5 OFFSET 5 — 第二页",
            ),
            self._seed(
                "SELECT id, username FROM t_users ORDER BY id LIMIT 5 OFFSET 10",
                tags=["limit_offset", "offset_page3"],
                desc="LIMIT 5 OFFSET 10 — 第三页",
            ),
            self._seed(
                "SELECT id, name, price FROM t_products ORDER BY id LIMIT 10 OFFSET 5",
                tags=["limit_offset", "offset_products"],
                desc="LIMIT 10 OFFSET 5 — products 分页",
            ),
            self._seed(
                "SELECT id, name, salary FROM t_employees ORDER BY salary DESC LIMIT 5 OFFSET 5",
                tags=["limit_offset", "offset_salary_page2"],
                desc="LIMIT 5 OFFSET 5 — 薪资排名 6-10",
            ),
            self._seed(
                "SELECT id, event_type, event_date FROM t_events ORDER BY event_date LIMIT 10 OFFSET 10",
                tags=["limit_offset", "offset_events"],
                desc="LIMIT 10 OFFSET 10 — events 分页",
            ),
            self._seed(
                "SELECT id, amount, tx_type FROM t_transactions ORDER BY id LIMIT 5 OFFSET 15",
                tags=["limit_offset", "offset_tx_last"],
                desc="LIMIT 5 OFFSET 15 — transactions 后段",
            ),
            self._seed(
                "SELECT id, username, score FROM t_users ORDER BY score DESC LIMIT 3 OFFSET 0",
                tags=["limit_offset", "offset_score_top3"],
                desc="LIMIT 3 OFFSET 0 — score Top 3",
            ),
            self._seed(
                "SELECT id, username, score FROM t_users ORDER BY score DESC LIMIT 3 OFFSET 3",
                tags=["limit_offset", "offset_score_4_6"],
                desc="LIMIT 3 OFFSET 3 — score 排名 4-6",
            ),
            self._seed(
                "SELECT id, name, budget FROM t_departments ORDER BY budget DESC LIMIT 3 OFFSET 2",
                tags=["limit_offset", "offset_dept_budget"],
                desc="LIMIT 3 OFFSET 2 — budget 排名 3-5",
            ),
        ]

    # ── LIMIT + ORDER BY 多列 (~10) ─────────────────────
    def _limit_with_order(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, age, score FROM t_users ORDER BY age ASC, score DESC LIMIT 5",
                tags=["limit_order", "limit_multi_order"],
                desc="LIMIT + ORDER BY 多列",
            ),
            self._seed(
                "SELECT id, name, price, stock FROM t_products ORDER BY category, price LIMIT 10",
                tags=["limit_order", "limit_cat_price"],
                desc="LIMIT + ORDER BY category, price",
            ),
            self._seed(
                "SELECT id, name, dept_id, salary FROM t_employees ORDER BY dept_id, salary DESC LIMIT 10",
                tags=["limit_order", "limit_dept_salary"],
                desc="LIMIT + ORDER BY dept, salary DESC",
            ),
            self._seed(
                "SELECT id, event_type, event_date FROM t_events ORDER BY event_type, event_date DESC LIMIT 10",
                tags=["limit_order", "limit_type_date"],
                desc="LIMIT + ORDER BY type, date DESC",
            ),
            self._seed(
                "SELECT id, status, amount FROM t_transactions ORDER BY status, amount DESC LIMIT 10",
                tags=["limit_order", "limit_status_amount"],
                desc="LIMIT + ORDER BY status, amount",
            ),
            self._seed(
                "SELECT id, username, COALESCE(score, 0) AS sc FROM t_users ORDER BY sc DESC LIMIT 5",
                tags=["limit_order", "limit_expr_order"],
                desc="LIMIT + ORDER BY 含表达式",
            ),
            self._seed(
                "SELECT id, name, price * COALESCE(stock, 0) AS total_value "
                "FROM t_products ORDER BY total_value DESC LIMIT 5",
                tags=["limit_order", "limit_computed_order"],
                desc="LIMIT + ORDER BY 计算列",
            ),
            self._seed(
                "SELECT id, name, dept_id, salary FROM t_employees "
                "WHERE dept_id IS NOT NULL ORDER BY dept_id, salary DESC LIMIT 10",
                tags=["limit_order", "limit_where_order"],
                desc="LIMIT + WHERE + ORDER BY",
            ),
            self._seed(
                "SELECT id, username, age FROM t_users WHERE age IS NOT NULL "
                "ORDER BY age LIMIT 5",
                tags=["limit_order", "limit_where_not_null"],
                desc="LIMIT + WHERE NOT NULL + ORDER BY",
            ),
            self._seed(
                "SELECT id, name, hire_date FROM t_employees "
                "WHERE hire_date IS NOT NULL ORDER BY hire_date DESC LIMIT 5",
                tags=["limit_order", "limit_hire_date"],
                desc="LIMIT + ORDER BY hire_date DESC — 最近入职",
            ),
        ]

    # ── 子查询中的 LIMIT (~6) ─────────────────────────────
    def _limit_subquery(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT * FROM t_orders WHERE user_id IN "
                "(SELECT id FROM t_users ORDER BY score DESC LIMIT 3) ORDER BY id",
                tags=["limit_subquery", "in_limit"],
                desc="IN 子查询含 LIMIT",
            ),
            self._seed(
                "SELECT e.id, e.name, e.salary FROM t_employees e "
                "WHERE e.dept_id IN (SELECT id FROM t_departments WHERE budget > 100000) "
                "ORDER BY e.salary DESC LIMIT 5",
                tags=["limit_subquery", "dept_budget_limit"],
                desc="子查询过滤 + LIMIT",
            ),
            self._seed(
                "SELECT * FROM t_events WHERE user_id IN "
                "(SELECT id FROM t_users WHERE score > 80 ORDER BY id LIMIT 5) "
                "ORDER BY id",
                tags=["limit_subquery", "events_high_score"],
                desc="子查询过滤高分用户 + LIMIT",
            ),
            self._seed(
                "SELECT t.id, t.amount, t.tx_type FROM t_transactions t "
                "WHERE t.from_user IN (SELECT id FROM t_users ORDER BY id LIMIT 10) "
                "ORDER BY t.amount DESC LIMIT 5",
                tags=["limit_subquery", "tx_user_limit"],
                desc="子查询限制用户范围 + LIMIT",
            ),
            self._seed(
                "SELECT u.id, u.username, u.score FROM t_users u "
                "WHERE u.score > (SELECT AVG(score) FROM t_users WHERE score IS NOT NULL) "
                "ORDER BY u.score DESC LIMIT 5",
                tags=["limit_subquery", "above_avg_limit"],
                desc="子查询计算平均值 + LIMIT",
            ),
            self._seed(
                "SELECT e.id, e.name, e.salary, d.name AS dept "
                "FROM t_employees e JOIN t_departments d ON e.dept_id = d.id "
                "WHERE e.salary > (SELECT AVG(salary) FROM t_employees WHERE salary IS NOT NULL) "
                "ORDER BY e.salary DESC LIMIT 5",
                tags=["limit_subquery", "join_above_avg_limit"],
                desc="JOIN + 子查询 + LIMIT",
            ),
        ]

    # ── 聚合 + LIMIT (~6) ─────────────────────────────────
    def _limit_aggregate(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT dept_id, COUNT(*) AS cnt, AVG(salary) AS avg_sal "
                "FROM t_employees WHERE dept_id IS NOT NULL "
                "GROUP BY dept_id ORDER BY avg_sal DESC LIMIT 3",
                tags=["limit_agg", "agg_dept_limit"],
                desc="GROUP BY + ORDER BY + LIMIT — 部门平均薪资 Top 3",
            ),
            self._seed(
                "SELECT user_id, COUNT(*) AS order_count, SUM(amount) AS total "
                "FROM t_transactions WHERE from_user IS NOT NULL "
                "GROUP BY from_user ORDER BY total DESC LIMIT 5",
                tags=["limit_agg", "agg_user_spending"],
                desc="GROUP BY + LIMIT — 用户消费 Top 5",
            ),
            self._seed(
                "SELECT event_type, COUNT(*) AS cnt FROM t_events "
                "GROUP BY event_type ORDER BY cnt DESC LIMIT 5",
                tags=["limit_agg", "agg_event_type"],
                desc="GROUP BY event_type + LIMIT",
            ),
            self._seed(
                "SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price "
                "FROM t_products WHERE category IS NOT NULL "
                "GROUP BY category ORDER BY avg_price DESC LIMIT 5",
                tags=["limit_agg", "agg_cat_price"],
                desc="GROUP BY category + LIMIT",
            ),
            self._seed(
                "SELECT status, COUNT(*) AS cnt, SUM(amount) AS total "
                "FROM t_transactions GROUP BY status ORDER BY total DESC LIMIT 3",
                tags=["limit_agg", "agg_tx_status"],
                desc="GROUP BY status + LIMIT",
            ),
            self._seed(
                "SELECT u.username, COUNT(o.id) AS order_count "
                "FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id "
                "GROUP BY u.id, u.username ORDER BY order_count DESC LIMIT 5",
                tags=["limit_agg", "agg_join_limit"],
                desc="LEFT JOIN + GROUP BY + LIMIT",
            ),
        ]
