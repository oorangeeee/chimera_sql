"""排序+分页标准模板 — 验证标准 ORDER BY 正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class StandardOrderingTemplate(SeedTemplate):

    @property
    def domain(self) -> str:
        return "ordering"

    @property
    def description(self) -> str:
        return "标准SQL排序+分页测试（ORDER BY/多列/稳定排序）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._single_col())
        seeds.extend(self._multi_col())
        seeds.extend(self._stable_order())
        return seeds

    def _single_col(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, score FROM t_users WHERE score IS NOT NULL ORDER BY score DESC, id", tags=["order", "score_desc"], desc="单列 DESC + id 稳定"),
            self._seed("SELECT id, name, price FROM t_products ORDER BY price ASC, id", tags=["order", "price_asc"], desc="价格 ASC"),
            self._seed("SELECT id, name, salary FROM t_employees WHERE salary IS NOT NULL ORDER BY salary DESC, id", tags=["order", "salary_desc"], desc="薪资 DESC"),
            self._seed("SELECT id, amount FROM t_transactions WHERE amount IS NOT NULL ORDER BY amount DESC, id", tags=["order", "amount_desc"], desc="金额 DESC"),
            self._seed("SELECT id, name, budget FROM t_departments WHERE budget IS NOT NULL ORDER BY budget DESC, id", tags=["order", "budget_desc"], desc="预算 DESC"),
            self._seed("SELECT id, username, age FROM t_users WHERE age IS NOT NULL ORDER BY age ASC, id", tags=["order", "age_asc"], desc="年龄 ASC"),
            self._seed("SELECT id, event_date FROM t_events WHERE event_date IS NOT NULL ORDER BY event_date DESC, id", tags=["order", "event_date_desc"], desc="日期 DESC"),
            self._seed("SELECT id, username FROM t_users ORDER BY username ASC, id", tags=["order", "name_asc"], desc="名称 ASC"),
            self._seed("SELECT id, name, hire_date FROM t_employees WHERE hire_date IS NOT NULL ORDER BY hire_date ASC, id", tags=["order", "hire_asc"], desc="入职日期 ASC"),
            self._seed("SELECT id, tag FROM t_tags ORDER BY tag ASC, id", tags=["order", "tag_asc"], desc="标签 ASC"),
        ]

    def _multi_col(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, age, score FROM t_users ORDER BY age ASC, score DESC, id", tags=["multi_order", "age_score"], desc="age ASC, score DESC"),
            self._seed("SELECT id, name, category, price FROM t_products WHERE category IS NOT NULL ORDER BY category ASC, price DESC, id", tags=["multi_order", "cat_price"], desc="category, price"),
            self._seed("SELECT id, name, dept_id, salary FROM t_employees WHERE dept_id IS NOT NULL ORDER BY dept_id ASC, salary DESC, id", tags=["multi_order", "dept_salary"], desc="dept, salary"),
            self._seed("SELECT id, status, amount FROM t_transactions WHERE status IS NOT NULL ORDER BY status ASC, amount DESC, id", tags=["multi_order", "status_amount"], desc="status, amount"),
            self._seed("SELECT id, event_type, event_date FROM t_events ORDER BY event_type ASC, event_date DESC, id", tags=["multi_order", "type_date"], desc="type, date"),
            self._seed("SELECT id, name, parent_id, budget FROM t_departments ORDER BY parent_id ASC, budget DESC, id", tags=["multi_order", "parent_budget"], desc="parent, budget"),
            self._seed("SELECT id, username, COALESCE(score, 0) AS sc FROM t_users ORDER BY sc DESC, id", tags=["multi_order", "expr_order"], desc="表达式排序"),
            self._seed("SELECT id, name, dept_id, salary FROM t_employees WHERE dept_id IS NOT NULL ORDER BY dept_id, name, id", tags=["multi_order", "dept_name"], desc="三列排序"),
            self._seed("SELECT id, username, email FROM t_users ORDER BY CASE WHEN email IS NULL THEN 1 ELSE 0 END, username, id", tags=["multi_order", "null_last_expr"], desc="NULL 最后表达式排序"),
            self._seed("SELECT id, name, price, stock FROM t_products ORDER BY CASE WHEN stock IS NULL THEN 1 ELSE 0 END, price, id", tags=["multi_order", "stock_null_last"], desc="NULL stock 在后"),
        ]

    def _stable_order(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, score, age FROM t_users ORDER BY score DESC, age ASC, username, id", tags=["stable", "four_col"], desc="四列稳定排序"),
            self._seed("SELECT id, name, dept_id, salary, hire_date FROM t_employees WHERE dept_id IS NOT NULL ORDER BY dept_id, salary DESC, hire_date, id", tags=["stable", "emp_stable"], desc="员工稳定排序"),
            self._seed("SELECT id, event_type, event_date, user_id FROM t_events ORDER BY event_type, event_date DESC, user_id, id", tags=["stable", "event_stable"], desc="事件稳定排序"),
            self._seed("SELECT id, tx_type, amount, status FROM t_transactions WHERE tx_type IS NOT NULL ORDER BY tx_type, amount DESC, status, id", tags=["stable", "tx_stable"], desc="交易稳定排序"),
            self._seed("SELECT id, name, category, price, stock FROM t_products WHERE category IS NOT NULL ORDER BY category, price DESC, COALESCE(stock, 0) DESC, id", tags=["stable", "product_stable"], desc="产品稳定排序"),
            self._seed("SELECT id, name, parent_id, budget FROM t_departments ORDER BY COALESCE(parent_id, 0), name, id", tags=["stable", "dept_stable"], desc="部门稳定排序"),
            self._seed("SELECT id, username, score FROM t_users ORDER BY COALESCE(score, -1) DESC, LOWER(username), id", tags=["stable", "coalesce_stable"], desc="COALESCE + LOWER 稳定"),
            self._seed("SELECT id, name, salary, dept_id FROM t_employees ORDER BY dept_id ASC, CASE WHEN salary IS NULL THEN 1 ELSE 0 END, salary DESC, id", tags=["stable", "null_aware"], desc="NULL 感知稳定排序"),
            self._seed("SELECT id, tag, entity_type FROM t_tags ORDER BY entity_type, tag, id", tags=["stable", "tag_stable"], desc="标签稳定排序"),
            self._seed("SELECT id, username, score, age FROM t_users WHERE score IS NOT NULL AND age IS NOT NULL ORDER BY score / age DESC, id", tags=["stable", "ratio_order"], desc="计算列排序"),
        ]
