"""窗口函数标准模板 — 验证标准窗口函数 SQL 正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class StandardWindowFuncsTemplate(SeedTemplate):

    @property
    def domain(self) -> str:
        return "window_funcs"

    @property
    def description(self) -> str:
        return "标准SQL窗口函数测试（ROW_NUMBER/RANK/LEAD/LAG/PARTITION BY）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._row_number())
        seeds.extend(self._rank_dense())
        seeds.extend(self._lead_lag())
        seeds.extend(self._running_total())
        return seeds

    def _row_number(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnk FROM t_users WHERE score IS NOT NULL ORDER BY rnk", tags=["rownum", "score_rank"], desc="ROW_NUMBER 按 score"),
            self._seed("SELECT id, name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rnk FROM t_employees WHERE salary IS NOT NULL ORDER BY rnk", tags=["rownum", "salary_rank"], desc="ROW_NUMBER 按 salary"),
            self._seed("SELECT id, name, salary, dept_id, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dept_rnk FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL ORDER BY dept_id, dept_rnk", tags=["rownum", "dept_salary"], desc="ROW_NUMBER PARTITION BY dept"),
            self._seed("SELECT id, username, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rnk FROM t_users WHERE score IS NOT NULL ORDER BY rnk LIMIT 5", tags=["rownum", "top5_score"], desc="ROW_NUMBER + LIMIT Top 5"),
            self._seed("SELECT id, name, price, ROW_NUMBER() OVER (ORDER BY price DESC) AS rnk FROM t_products WHERE price IS NOT NULL ORDER BY rnk", tags=["rownum", "price_rank"], desc="ROW_NUMBER 按 price"),
            self._seed("SELECT id, name, dept_id, salary, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY hire_date) AS hire_order FROM t_employees WHERE dept_id IS NOT NULL AND hire_date IS NOT NULL ORDER BY dept_id, hire_order", tags=["rownum", "hire_order"], desc="ROW_NUMBER 入职顺序"),
            self._seed("SELECT id, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) AS rnk FROM t_transactions WHERE amount IS NOT NULL ORDER BY rnk", tags=["rownum", "amount_rank"], desc="ROW_NUMBER 按 amount"),
            self._seed("SELECT id, username, age, ROW_NUMBER() OVER (ORDER BY age) AS rnk FROM t_users WHERE age IS NOT NULL ORDER BY rnk", tags=["rownum", "age_rank"], desc="ROW_NUMBER 按 age"),
            self._seed("SELECT id, name, budget, ROW_NUMBER() OVER (ORDER BY budget DESC) AS rnk FROM t_departments WHERE budget IS NOT NULL ORDER BY rnk", tags=["rownum", "budget_rank"], desc="ROW_NUMBER 按 budget"),
        ]

    def _rank_dense(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, score, RANK() OVER (ORDER BY score DESC) AS rnk, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM t_users WHERE score IS NOT NULL ORDER BY rnk", tags=["rank", "score_ranks"], desc="RANK + DENSE_RANK score"),
            self._seed("SELECT id, name, salary, RANK() OVER (ORDER BY salary DESC) AS rnk FROM t_employees WHERE salary IS NOT NULL ORDER BY rnk", tags=["rank", "salary_rank"], desc="RANK salary"),
            self._seed("SELECT id, name, dept_id, salary, DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dept_rnk FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL ORDER BY dept_id, dept_rnk", tags=["rank", "dept_dense_rank"], desc="DENSE_RANK PARTITION BY dept"),
            self._seed("SELECT id, name, price, RANK() OVER (ORDER BY price DESC) AS rnk FROM t_products WHERE price IS NOT NULL ORDER BY rnk", tags=["rank", "price_rank"], desc="RANK price"),
            self._seed("SELECT id, event_type, COUNT(*) OVER (PARTITION BY event_type) AS type_count FROM t_events ORDER BY id", tags=["rank", "event_count_window"], desc="COUNT OVER PARTITION"),
            self._seed("SELECT id, username, score, NTILE(4) OVER (ORDER BY score DESC) AS quartile FROM t_users WHERE score IS NOT NULL ORDER BY score DESC", tags=["rank", "ntile_score"], desc="NTILE(4) — 四分位"),
            self._seed("SELECT id, name, salary, PERCENT_RANK() OVER (ORDER BY salary DESC) AS pct FROM t_employees WHERE salary IS NOT NULL ORDER BY salary DESC", tags=["rank", "percent_rank"], desc="PERCENT_RANK salary"),
            self._seed("SELECT id, name, dept_id, salary, RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS dept_rnk FROM t_employees WHERE dept_id IS NOT NULL ORDER BY dept_id, dept_rnk", tags=["rank", "dept_rank"], desc="RANK PARTITION BY dept"),
        ]

    def _lead_lag(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, score, LAG(score, 1) OVER (ORDER BY score DESC) AS prev_score FROM t_users WHERE score IS NOT NULL ORDER BY score DESC", tags=["lead_lag", "lag_score"], desc="LAG(score) — 前一名分数"),
            self._seed("SELECT id, name, salary, LEAD(salary, 1) OVER (ORDER BY salary DESC) AS next_salary FROM t_employees WHERE salary IS NOT NULL ORDER BY salary DESC", tags=["lead_lag", "lead_salary"], desc="LEAD(salary) — 后一名薪资"),
            self._seed("SELECT id, amount, LAG(amount, 1) OVER (ORDER BY id) AS prev_amt, amount - LAG(amount, 1) OVER (ORDER BY id) AS diff FROM t_transactions WHERE amount IS NOT NULL ORDER BY id", tags=["lead_lag", "amount_diff"], desc="LAG 差值计算"),
            self._seed("SELECT id, name, hire_date, LAG(hire_date, 1) OVER (PARTITION BY dept_id ORDER BY hire_date) AS prev_hire FROM t_employees WHERE dept_id IS NOT NULL AND hire_date IS NOT NULL ORDER BY dept_id, hire_date", tags=["lead_lag", "hire_lag_dept"], desc="LAG PARTITION BY dept"),
            self._seed("SELECT id, event_date, LEAD(event_date, 1) OVER (ORDER BY event_date) AS next_date FROM t_events WHERE event_date IS NOT NULL ORDER BY event_date", tags=["lead_lag", "event_lead"], desc="LEAD 日期"),
            self._seed("SELECT id, username, score, score - LAG(score, 1) OVER (ORDER BY id) AS score_change FROM t_users WHERE score IS NOT NULL ORDER BY id", tags=["lead_lag", "score_change"], desc="LAG 变化量"),
            self._seed("SELECT id, name, price, price - LAG(price, 1) OVER (ORDER BY price) AS price_diff FROM t_products WHERE price IS NOT NULL ORDER BY price", tags=["lead_lag", "price_diff"], desc="LAG 价格差"),
            self._seed("SELECT id, name, salary, dept_id, salary - LAG(salary, 1) OVER (PARTITION BY dept_id ORDER BY salary) AS dept_diff FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL ORDER BY dept_id, salary", tags=["lead_lag", "dept_salary_diff"], desc="LAG PARTITION 差值"),
        ]

    def _running_total(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, score, SUM(score) OVER (ORDER BY id) AS running_total FROM t_users WHERE score IS NOT NULL ORDER BY id", tags=["running", "score_running"], desc="SUM OVER — 累计 score"),
            self._seed("SELECT id, name, salary, SUM(salary) OVER (ORDER BY id) AS running_total FROM t_employees WHERE salary IS NOT NULL ORDER BY id", tags=["running", "salary_running"], desc="SUM OVER — 累计 salary"),
            self._seed("SELECT id, amount, SUM(amount) OVER (ORDER BY id) AS running_total FROM t_transactions WHERE amount IS NOT NULL ORDER BY id", tags=["running", "amount_running"], desc="SUM OVER — 累计 amount"),
            self._seed("SELECT id, name, salary, dept_id, SUM(salary) OVER (PARTITION BY dept_id ORDER BY id) AS dept_running FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL ORDER BY dept_id, id", tags=["running", "dept_running"], desc="SUM OVER PARTITION — 部门累计"),
            self._seed("SELECT id, username, score, AVG(score) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg FROM t_users WHERE score IS NOT NULL ORDER BY id", tags=["running", "moving_avg"], desc="ROWS BETWEEN 移动平均"),
            self._seed("SELECT id, name, price, AVG(price) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS smooth_price FROM t_products WHERE price IS NOT NULL ORDER BY id", tags=["running", "smooth_price"], desc="ROWS BETWEEN 平滑"),
            self._seed("SELECT id, amount, SUM(amount) OVER (PARTITION BY tx_type ORDER BY id) AS type_running FROM t_transactions WHERE tx_type IS NOT NULL AND amount IS NOT NULL ORDER BY tx_type, id", tags=["running", "type_running"], desc="SUM OVER PARTITION — 类型累计"),
            self._seed("SELECT id, username, score, COUNT(*) OVER () AS total_count FROM t_users WHERE score IS NOT NULL ORDER BY id", tags=["running", "total_count"], desc="COUNT OVER () — 总计数"),
        ]
