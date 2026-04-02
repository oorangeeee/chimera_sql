"""CTE 标准模板 — 验证标准 WITH 子句正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class StandardCteTemplate(SeedTemplate):

    @property
    def domain(self) -> str:
        return "cte"

    @property
    def description(self) -> str:
        return "标准SQL CTE测试（普通WITH/多CTE/CTE+JOIN）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._basic_cte())
        seeds.extend(self._multi_cte())
        seeds.extend(self._cte_join())
        return seeds

    def _basic_cte(self) -> List[SeedSQL]:
        return [
            self._seed("WITH dept_stats AS (SELECT dept_id, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL GROUP BY dept_id) SELECT * FROM dept_stats ORDER BY dept_id", tags=["cte", "dept_stats"], desc="CTE 部门统计"),
            self._seed("WITH user_orders AS (SELECT user_id, COUNT(*) AS order_count, SUM(total_amount) AS total FROM t_orders GROUP BY user_id) SELECT u.username, COALESCE(o.order_count, 0) AS orders, COALESCE(o.total, 0) AS total FROM t_users u LEFT JOIN user_orders o ON u.id = o.user_id ORDER BY u.id", tags=["cte", "user_orders"], desc="CTE + LEFT JOIN"),
            self._seed("WITH high_scorers AS (SELECT id, username, score FROM t_users WHERE score > 80) SELECT * FROM high_scorers ORDER BY score DESC", tags=["cte", "high_scorers"], desc="CTE 高分用户"),
            self._seed("WITH cat_summary AS (SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price FROM t_products WHERE category IS NOT NULL GROUP BY category) SELECT * FROM cat_summary ORDER BY avg_price DESC", tags=["cte", "cat_summary"], desc="CTE 分类统计"),
            self._seed("WITH active_emp AS (SELECT id, name, salary, dept_id FROM t_employees WHERE status = 'active' AND salary IS NOT NULL) SELECT * FROM active_emp ORDER BY salary DESC", tags=["cte", "active_emp"], desc="CTE 活跃员工"),
            self._seed("WITH tx_summary AS (SELECT tx_type, COUNT(*) AS cnt, SUM(amount) AS total FROM t_transactions WHERE tx_type IS NOT NULL AND amount IS NOT NULL GROUP BY tx_type) SELECT * FROM tx_summary ORDER BY total DESC", tags=["cte", "tx_summary"], desc="CTE 交易类型统计"),
            self._seed("WITH event_users AS (SELECT user_id, COUNT(*) AS event_count FROM t_events WHERE user_id IS NOT NULL GROUP BY user_id) SELECT u.username, COALESCE(eu.event_count, 0) AS events FROM t_users u LEFT JOIN event_users eu ON u.id = eu.user_id ORDER BY u.id", tags=["cte", "event_users"], desc="CTE + LEFT JOIN 事件"),
            self._seed("WITH dept_budget AS (SELECT d.name, d.budget, COUNT(e.id) AS emp_count FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name, d.budget) SELECT * FROM dept_budget ORDER BY budget DESC", tags=["cte", "dept_budget"], desc="CTE 部门预算+人数"),
            self._seed("WITH avg_score AS (SELECT AVG(score) AS overall_avg FROM t_users WHERE score IS NOT NULL) SELECT u.id, u.username, u.score, a.overall_avg, u.score - a.overall_avg AS diff FROM t_users u CROSS JOIN avg_score a WHERE u.score IS NOT NULL ORDER BY diff DESC", tags=["cte", "score_vs_avg"], desc="CTE + CROSS JOIN"),
            self._seed("WITH salary_ranks AS (SELECT id, name, salary, dept_id, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rnk FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL) SELECT * FROM salary_ranks WHERE rnk <= 2 ORDER BY dept_id, rnk", tags=["cte", "top_per_dept"], desc="CTE + 窗口函数过滤"),
        ]

    def _multi_cte(self) -> List[SeedSQL]:
        return [
            self._seed("WITH dept_stats AS (SELECT dept_id, AVG(salary) AS avg_sal FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL GROUP BY dept_id), high_depts AS (SELECT dept_id FROM dept_stats WHERE avg_sal > 80000) SELECT e.id, e.name, e.salary, e.dept_id FROM t_employees e WHERE e.dept_id IN (SELECT dept_id FROM high_depts) ORDER BY e.dept_id, e.salary DESC", tags=["multi_cte", "two_cte"], desc="两个 CTE 链式"),
            self._seed("WITH user_totals AS (SELECT user_id, SUM(total_amount) AS total FROM t_orders GROUP BY user_id), event_counts AS (SELECT user_id, COUNT(*) AS cnt FROM t_events WHERE user_id IS NOT NULL GROUP BY user_id) SELECT u.username, COALESCE(ut.total, 0) AS order_total, COALESCE(ec.cnt, 0) AS event_count FROM t_users u LEFT JOIN user_totals ut ON u.id = ut.user_id LEFT JOIN event_counts ec ON u.id = ec.user_id ORDER BY u.id", tags=["multi_cte", "orders_events"], desc="两个 CTE LEFT JOIN"),
            self._seed("WITH completed AS (SELECT * FROM t_orders WHERE status = 'completed'), pending AS (SELECT * FROM t_orders WHERE status = 'pending') SELECT 'completed' AS type, COUNT(*) AS cnt FROM completed UNION ALL SELECT 'pending' AS type, COUNT(*) AS cnt FROM pending ORDER BY type", tags=["multi_cte", "status_split"], desc="两个 CTE + UNION ALL"),
            self._seed("WITH dept_emp AS (SELECT d.name AS dept, COUNT(e.id) AS cnt FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name), big_depts AS (SELECT dept, cnt FROM dept_emp WHERE cnt > 2) SELECT * FROM big_depts ORDER BY cnt DESC", tags=["multi_cte", "big_depts"], desc="两个 CTE 过滤"),
            self._seed("WITH salaries AS (SELECT dept_id, salary FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL), dept_avg AS (SELECT dept_id, AVG(salary) AS avg_sal FROM salaries GROUP BY dept_id) SELECT s.dept_id, s.salary, d.avg_sal, s.salary - d.avg_sal AS diff FROM salaries s JOIN dept_avg d ON s.dept_id = d.dept_id ORDER BY s.dept_id, diff DESC", tags=["multi_cte", "salary_vs_avg"], desc="两个 CTE 计算偏差"),
            self._seed("WITH active_users AS (SELECT DISTINCT user_id FROM t_events WHERE user_id IS NOT NULL), buyers AS (SELECT DISTINCT user_id FROM t_orders) SELECT u.username FROM t_users u WHERE u.id IN (SELECT user_id FROM active_users) AND u.id IN (SELECT user_id FROM buyers) ORDER BY u.id", tags=["multi_cte", "active_buyers"], desc="两个 CTE 交集"),
        ]

    def _cte_join(self) -> List[SeedSQL]:
        return [
            self._seed("WITH dept_stats AS (SELECT dept_id, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM t_employees WHERE dept_id IS NOT NULL GROUP BY dept_id) SELECT d.name, ds.cnt, ROUND(ds.avg_sal, 2) AS avg_sal FROM t_departments d JOIN dept_stats ds ON d.id = ds.dept_id ORDER BY d.name", tags=["cte_join", "dept_stats_join"], desc="CTE + JOIN"),
            self._seed("WITH user_spending AS (SELECT from_user, SUM(amount) AS total FROM t_transactions WHERE from_user IS NOT NULL GROUP BY from_user) SELECT u.username, COALESCE(us.total, 0) AS total_spent FROM t_users u LEFT JOIN user_spending us ON u.id = us.from_user ORDER BY total_spent DESC", tags=["cte_join", "user_spending"], desc="CTE + LEFT JOIN"),
            self._seed("WITH ranked AS (SELECT id, name, salary, dept_id, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rnk FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL) SELECT r.name, r.salary, d.name AS dept FROM ranked r JOIN t_departments d ON r.dept_id = d.id WHERE r.rnk = 1 ORDER BY d.name", tags=["cte_join", "top_per_dept"], desc="CTE + 窗口 + JOIN"),
            self._seed("WITH user_events AS (SELECT user_id, event_type, COUNT(*) AS cnt FROM t_events WHERE user_id IS NOT NULL GROUP BY user_id, event_type) SELECT u.username, ue.event_type, ue.cnt FROM t_users u JOIN user_events ue ON u.id = ue.user_id ORDER BY u.id, ue.event_type", tags=["cte_join", "user_event_detail"], desc="CTE + JOIN 事件详情"),
            self._seed("WITH dept_salary AS (SELECT dept_id, SUM(salary) AS total_sal FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL GROUP BY dept_id) SELECT d.name, d.budget, ds.total_sal, d.budget - COALESCE(ds.total_sal, 0) AS remaining FROM t_departments d LEFT JOIN dept_salary ds ON d.id = ds.dept_id ORDER BY d.name", tags=["cte_join", "budget_remaining"], desc="CTE + LEFT JOIN 计算余额"),
            self._seed("WITH order_summary AS (SELECT user_id, COUNT(*) AS cnt, MAX(total_amount) AS max_order FROM t_orders GROUP BY user_id) SELECT u.username, os.cnt, os.max_order FROM t_users u LEFT JOIN order_summary os ON u.id = os.user_id ORDER BY os.max_order DESC", tags=["cte_join", "order_summary"], desc="CTE + LEFT JOIN 订单摘要"),
            self._seed("WITH emp_dept AS (SELECT e.id, e.name, e.salary, d.name AS dept FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.salary IS NOT NULL) SELECT dept, COUNT(*) AS cnt, MAX(salary) AS max_sal FROM emp_dept GROUP BY dept ORDER BY max_sal DESC", tags=["cte_join", "dept_max"], desc="CTE + JOIN + 聚合"),
            self._seed("WITH large_tx AS (SELECT * FROM t_transactions WHERE amount > 200 AND status = 'completed') SELECT lt.id, lt.amount, u.username AS sender FROM large_tx lt JOIN t_users u ON lt.from_user = u.id ORDER BY lt.amount DESC", tags=["cte_join", "large_tx"], desc="CTE 过滤 + JOIN"),
        ]
