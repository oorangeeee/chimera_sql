"""聚合+分组标准模板 — 验证标准聚合 SQL 正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class StandardAggregationTemplate(SeedTemplate):

    @property
    def domain(self) -> str:
        return "aggregation"

    @property
    def description(self) -> str:
        return "标准SQL聚合+分组测试（GROUP BY/HAVING/多聚合）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._basic_agg())
        seeds.extend(self._group_by())
        seeds.extend(self._having())
        return seeds

    def _basic_agg(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT COUNT(*) AS total FROM t_users", tags=["basic_agg", "count_users"], desc="COUNT(*) 用户"),
            self._seed("SELECT AVG(score) AS avg_score, MAX(score) AS max_score, MIN(score) AS min_score FROM t_users WHERE score IS NOT NULL", tags=["basic_agg", "score_stats"], desc="AVG/MAX/MIN score"),
            self._seed("SELECT SUM(salary) AS total, AVG(salary) AS avg_sal FROM t_employees WHERE salary IS NOT NULL", tags=["basic_agg", "salary_sum_avg"], desc="SUM/AVG salary"),
            self._seed("SELECT COUNT(DISTINCT category) AS cat_count FROM t_products", tags=["basic_agg", "distinct_cat"], desc="COUNT DISTINCT 分类"),
            self._seed("SELECT MIN(hire_date) AS earliest, MAX(hire_date) AS latest FROM t_employees WHERE hire_date IS NOT NULL", tags=["basic_agg", "hire_range"], desc="MIN/MAX 入职日期"),
            self._seed("SELECT COUNT(*) AS total, COUNT(email) AS with_email, COUNT(score) AS with_score FROM t_users", tags=["basic_agg", "null_counts"], desc="COUNT(*) vs COUNT(col)"),
            self._seed("SELECT SUM(amount) AS total, AVG(amount) AS avg_amt, MAX(amount) AS max_amt FROM t_transactions WHERE amount IS NOT NULL", tags=["basic_agg", "tx_stats"], desc="交易金额统计"),
            self._seed("SELECT COUNT(DISTINCT event_type) AS types, COUNT(DISTINCT user_id) AS users FROM t_events", tags=["basic_agg", "event_distinct"], desc="COUNT DISTINCT 事件"),
            self._seed("SELECT SUM(budget) AS total_budget, AVG(budget) AS avg_budget FROM t_departments WHERE budget IS NOT NULL", tags=["basic_agg", "budget_stats"], desc="预算统计"),
            self._seed("SELECT COUNT(*) AS total, SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed FROM t_orders", tags=["basic_agg", "order_completion"], desc="CASE + SUM 计数"),
        ]

    def _group_by(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT dept_id, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM t_employees WHERE dept_id IS NOT NULL GROUP BY dept_id ORDER BY dept_id", tags=["group", "dept_stats"], desc="GROUP BY dept_id"),
            self._seed("SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM t_products WHERE category IS NOT NULL GROUP BY category ORDER BY category", tags=["group", "cat_stats"], desc="GROUP BY category"),
            self._seed("SELECT user_id, COUNT(*) AS cnt FROM t_orders GROUP BY user_id ORDER BY cnt DESC", tags=["group", "user_orders"], desc="GROUP BY user_id"),
            self._seed("SELECT event_type, COUNT(*) AS cnt FROM t_events GROUP BY event_type ORDER BY cnt DESC", tags=["group", "event_types"], desc="GROUP BY event_type"),
            self._seed("SELECT status, COUNT(*) AS cnt, AVG(amount) AS avg_amt FROM t_transactions WHERE status IS NOT NULL GROUP BY status ORDER BY status", tags=["group", "tx_status"], desc="GROUP BY status"),
            self._seed("SELECT dept_id, status, COUNT(*) AS cnt FROM t_employees WHERE dept_id IS NOT NULL GROUP BY dept_id, status ORDER BY dept_id, status", tags=["group", "dept_status"], desc="GROUP BY 两列"),
            self._seed("SELECT d.name, COUNT(e.id) AS cnt FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name ORDER BY d.name", tags=["group", "dept_emp_count"], desc="LEFT JOIN + GROUP BY"),
            self._seed("SELECT user_id, status, COUNT(*) AS cnt, SUM(total_amount) AS total FROM t_orders GROUP BY user_id, status ORDER BY user_id, status", tags=["group", "user_order_status"], desc="GROUP BY 两列 + SUM"),
            self._seed("SELECT tx_type, status, COUNT(*) AS cnt FROM t_transactions WHERE tx_type IS NOT NULL AND status IS NOT NULL GROUP BY tx_type, status ORDER BY tx_type, status", tags=["group", "tx_type_status"], desc="GROUP BY 两列 过滤"),
            self._seed("SELECT d.name, SUM(e.salary) AS total_sal FROM t_departments d JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name ORDER BY total_sal DESC", tags=["group", "dept_salary"], desc="JOIN + GROUP BY + SUM"),
        ]

    def _having(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT dept_id, COUNT(*) AS cnt FROM t_employees WHERE dept_id IS NOT NULL GROUP BY dept_id HAVING COUNT(*) > 2 ORDER BY dept_id", tags=["having", "large_dept"], desc="HAVING > 2"),
            self._seed("SELECT category, AVG(price) AS avg_price FROM t_products WHERE category IS NOT NULL GROUP BY category HAVING AVG(price) > 50 ORDER BY avg_price DESC", tags=["having", "expensive_cat"], desc="HAVING AVG > 50"),
            self._seed("SELECT user_id, COUNT(*) AS cnt FROM t_orders GROUP BY user_id HAVING COUNT(*) >= 2 ORDER BY cnt DESC", tags=["having", "repeat_buyer"], desc="HAVING >= 2"),
            self._seed("SELECT dept_id, SUM(salary) AS total FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL GROUP BY dept_id HAVING SUM(salary) > 150000 ORDER BY total DESC", tags=["having", "high_salary_dept"], desc="HAVING SUM > 150000"),
            self._seed("SELECT event_type, COUNT(*) AS cnt FROM t_events GROUP BY event_type HAVING COUNT(*) >= 3 ORDER BY cnt DESC", tags=["having", "frequent_events"], desc="HAVING >= 3"),
            self._seed("SELECT status, COUNT(*) AS cnt FROM t_transactions WHERE status IS NOT NULL GROUP BY status HAVING COUNT(*) > 5 ORDER BY cnt DESC", tags=["having", "common_status"], desc="HAVING > 5"),
            self._seed("SELECT d.name, COUNT(e.id) AS cnt FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name HAVING COUNT(e.id) > 0 ORDER BY cnt DESC", tags=["having", "non_empty_dept"], desc="LEFT JOIN + HAVING > 0"),
            self._seed("SELECT dept_id, AVG(salary) AS avg_sal, COUNT(*) AS cnt FROM t_employees WHERE dept_id IS NOT NULL GROUP BY dept_id HAVING AVG(salary) > 70000 AND COUNT(*) >= 2 ORDER BY avg_sal DESC", tags=["having", "multi_having"], desc="HAVING 多条件"),
            self._seed("SELECT tx_type, SUM(amount) AS total FROM t_transactions WHERE tx_type IS NOT NULL GROUP BY tx_type HAVING SUM(amount) > 100 ORDER BY total DESC", tags=["having", "tx_type_total"], desc="HAVING SUM > 100"),
            self._seed("SELECT from_user, COUNT(*) AS cnt, MAX(amount) AS max_amt FROM t_transactions WHERE from_user IS NOT NULL GROUP BY from_user HAVING MAX(amount) > 200 ORDER BY max_amt DESC", tags=["having", "big_spender"], desc="HAVING MAX > 200"),
        ]
