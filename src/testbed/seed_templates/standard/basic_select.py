"""基础 SELECT 标准模板 — 验证标准 SQL 正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class BasicSelectTemplate(SeedTemplate):
    """基础 SELECT 标准种子模板。"""

    @property
    def domain(self) -> str:
        return "basic_select"

    @property
    def description(self) -> str:
        return "基础 SELECT 标准SQL测试（WHERE/ORDER BY/DISTINCT）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._simple_selects())
        seeds.extend(self._where_conditions())
        seeds.extend(self._order_by())
        seeds.extend(self._distinct())
        return seeds

    def _simple_selects(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT * FROM t_users ORDER BY id", tags=["simple", "all_users"], desc="全列查询 t_users"),
            self._seed("SELECT * FROM t_products ORDER BY id", tags=["simple", "all_products"], desc="全列查询 t_products"),
            self._seed("SELECT * FROM t_orders ORDER BY id", tags=["simple", "all_orders"], desc="全列查询 t_orders"),
            self._seed("SELECT * FROM t_employees ORDER BY id", tags=["simple", "all_employees"], desc="全列查询 t_employees"),
            self._seed("SELECT * FROM t_departments ORDER BY id", tags=["simple", "all_depts"], desc="全列查询 t_departments"),
            self._seed("SELECT id, username, email FROM t_users ORDER BY id", tags=["simple", "user_cols"], desc="部分列查询"),
            self._seed("SELECT id, name, price, stock FROM t_products ORDER BY id", tags=["simple", "product_cols"], desc="产品部分列"),
            self._seed("SELECT id, name, salary, dept_id FROM t_employees ORDER BY id", tags=["simple", "emp_cols"], desc="员工部分列"),
        ]

    def _where_conditions(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, score FROM t_users WHERE score > 80 ORDER BY id", tags=["where", "score_gt_80"], desc="WHERE 单条件"),
            self._seed("SELECT id, name, price FROM t_products WHERE price BETWEEN 10 AND 100 ORDER BY id", tags=["where", "price_range"], desc="WHERE BETWEEN"),
            self._seed("SELECT id, name, salary FROM t_employees WHERE salary > 80000 AND dept_id IS NOT NULL ORDER BY id", tags=["where", "salary_and_dept"], desc="WHERE AND"),
            self._seed("SELECT id, username FROM t_users WHERE age > 25 OR score > 80 ORDER BY id", tags=["where", "age_or_score"], desc="WHERE OR"),
            self._seed("SELECT id, name FROM t_products WHERE category IN ('electronics', 'clothing') ORDER BY id", tags=["where", "in_category"], desc="WHERE IN"),
            self._seed("SELECT id, name FROM t_employees WHERE status = 'active' ORDER BY id", tags=["where", "active"], desc="WHERE 等于"),
            self._seed("SELECT id, username FROM t_users WHERE email IS NOT NULL AND score IS NOT NULL ORDER BY id", tags=["where", "not_null"], desc="WHERE IS NOT NULL"),
            self._seed("SELECT id, name FROM t_products WHERE price > 50 AND stock > 0 ORDER BY id", tags=["where", "price_stock"], desc="WHERE 双条件"),
            self._seed("SELECT id, name FROM t_employees WHERE dept_id IS NOT NULL AND hire_date IS NOT NULL ORDER BY id", tags=["where", "emp_not_null"], desc="WHERE 多列 NOT NULL"),
            self._seed("SELECT id, amount FROM t_transactions WHERE amount > 0 AND status = 'completed' ORDER BY id", tags=["where", "completed_positive"], desc="WHERE 组合条件"),
            self._seed("SELECT id, username FROM t_users WHERE username LIKE 'A%' ORDER BY id", tags=["where", "like_prefix"], desc="WHERE LIKE"),
            self._seed("SELECT id, name FROM t_departments WHERE budget IS NOT NULL ORDER BY id", tags=["where", "budget_not_null"], desc="WHERE NOT NULL"),
            self._seed("SELECT id, event_type FROM t_events WHERE user_id IS NOT NULL ORDER BY id", tags=["where", "event_known_user"], desc="WHERE NOT NULL events"),
            self._seed("SELECT id, name, price FROM t_products WHERE price > 100 OR category IS NULL ORDER BY id", tags=["where", "price_or_null"], desc="WHERE OR NULL"),
        ]

    def _order_by(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, score FROM t_users ORDER BY score DESC, id", tags=["order", "score_desc"], desc="ORDER BY DESC"),
            self._seed("SELECT id, name, price FROM t_products ORDER BY price ASC, id", tags=["order", "price_asc"], desc="ORDER BY ASC"),
            self._seed("SELECT id, name, salary FROM t_employees ORDER BY dept_id, salary DESC, id", tags=["order", "dept_salary"], desc="多列 ORDER BY"),
            self._seed("SELECT id, event_type, event_date FROM t_events ORDER BY event_date DESC, id", tags=["order", "event_date"], desc="ORDER BY 日期 DESC"),
            self._seed("SELECT id, amount, status FROM t_transactions ORDER BY amount DESC, id", tags=["order", "amount_desc"], desc="ORDER BY 金额 DESC"),
            self._seed("SELECT id, name, budget FROM t_departments ORDER BY budget DESC, id", tags=["order", "budget_desc"], desc="ORDER BY budget DESC"),
            self._seed("SELECT id, username, age FROM t_users WHERE age IS NOT NULL ORDER BY age, id", tags=["order", "age_asc"], desc="WHERE + ORDER BY"),
            self._seed("SELECT id, name, salary FROM t_employees WHERE dept_id IS NOT NULL ORDER BY salary DESC, id", tags=["order", "salary_filtered"], desc="过滤后排序"),
        ]

    def _distinct(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT DISTINCT category FROM t_products WHERE category IS NOT NULL ORDER BY category", tags=["distinct", "categories"], desc="DISTINCT 分类"),
            self._seed("SELECT DISTINCT status FROM t_orders ORDER BY status", tags=["distinct", "order_statuses"], desc="DISTINCT 订单状态"),
            self._seed("SELECT DISTINCT dept_id FROM t_employees WHERE dept_id IS NOT NULL ORDER BY dept_id", tags=["distinct", "dept_ids"], desc="DISTINCT 部门"),
            self._seed("SELECT DISTINCT event_type FROM t_events ORDER BY event_type", tags=["distinct", "event_types"], desc="DISTINCT 事件类型"),
            self._seed("SELECT DISTINCT tx_type FROM t_transactions WHERE tx_type IS NOT NULL ORDER BY tx_type", tags=["distinct", "tx_types"], desc="DISTINCT 交易类型"),
            self._seed("SELECT DISTINCT status FROM t_employees ORDER BY status", tags=["distinct", "emp_statuses"], desc="DISTINCT 员工状态"),
            self._seed("SELECT DISTINCT location FROM t_departments WHERE location IS NOT NULL ORDER BY location", tags=["distinct", "locations"], desc="DISTINCT 位置"),
            self._seed("SELECT DISTINCT user_id FROM t_events WHERE user_id IS NOT NULL ORDER BY user_id", tags=["distinct", "active_users"], desc="DISTINCT 活跃用户"),
        ]
