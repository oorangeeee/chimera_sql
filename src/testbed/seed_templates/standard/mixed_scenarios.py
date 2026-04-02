"""综合场景标准模板 — 验证标准 SQL 复杂组合正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class StandardMixedScenariosTemplate(SeedTemplate):

    @property
    def domain(self) -> str:
        return "mixed_scenarios"

    @property
    def description(self) -> str:
        return "标准SQL综合场景测试（JOIN+聚合+子查询+窗口组合）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._join_agg_sub())
        seeds.extend(self._join_window())
        seeds.extend(self._cte_complex())
        return seeds

    def _join_agg_sub(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT d.name AS dept, COUNT(e.id) AS emp_count, AVG(e.salary) AS avg_sal FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name HAVING AVG(e.salary) > 70000 OR AVG(e.salary) IS NULL ORDER BY d.name", tags=["join_agg_sub", "dept_salary_having"], desc="JOIN + GROUP BY + HAVING"),
            self._seed("SELECT u.username, COUNT(o.id) AS order_count, COALESCE(SUM(o.total_amount), 0) AS total_spent FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id GROUP BY u.id, u.username ORDER BY total_spent DESC", tags=["join_agg_sub", "user_spending"], desc="LEFT JOIN + GROUP BY + COALESCE"),
            self._seed("SELECT e.name, d.name AS dept, e.salary FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.salary > (SELECT AVG(salary) FROM t_employees WHERE salary IS NOT NULL) ORDER BY e.salary DESC", tags=["join_agg_sub", "above_avg"], desc="JOIN + 子查询 + ORDER BY"),
            self._seed("SELECT d.name AS dept, COUNT(e.id) AS emp_cnt, SUM(CASE WHEN e.salary > 100000 THEN 1 ELSE 0 END) AS high_earners FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name ORDER BY dept", tags=["join_agg_sub", "dept_high_earners"], desc="LEFT JOIN + CASE + GROUP BY"),
            self._seed("SELECT u.username, COALESCE(SUM(CASE WHEN o.status = 'completed' THEN o.total_amount END), 0) AS completed_total FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id GROUP BY u.id, u.username ORDER BY completed_total DESC", tags=["join_agg_sub", "user_completed"], desc="LEFT JOIN + CASE + SUM"),
            self._seed("SELECT e.name, d.name AS dept, e.salary, e.salary - (SELECT AVG(e2.salary) FROM t_employees e2 WHERE e2.dept_id = e.dept_id AND e2.salary IS NOT NULL) AS dept_diff FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.salary IS NOT NULL ORDER BY e.salary DESC", tags=["join_agg_sub", "dept_diff"], desc="JOIN + 相关子查询"),
            self._seed("SELECT u.username, COUNT(DISTINCT o.id) AS orders, COUNT(DISTINCT ev.id) AS events FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id LEFT JOIN t_events ev ON u.id = ev.user_id GROUP BY u.id, u.username ORDER BY u.id", tags=["join_agg_sub", "user_activity"], desc="多 LEFT JOIN + COUNT DISTINCT"),
            self._seed("SELECT e.name, d.name AS dept, e.salary, ROUND(e.salary * 100.0 / SUM(e.salary) OVER (PARTITION BY e.dept_id), 2) AS pct_of_dept FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.dept_id IS NOT NULL AND e.salary IS NOT NULL ORDER BY d.name, e.salary DESC", tags=["join_agg_sub", "dept_pct"], desc="JOIN + 窗口函数 + 计算"),
            self._seed("SELECT u.username, SUM(t.amount) AS total_sent, (SELECT COUNT(*) FROM t_orders o WHERE o.user_id = u.id) AS order_count FROM t_users u LEFT JOIN t_transactions t ON u.id = t.from_user AND t.amount > 0 GROUP BY u.id, u.username ORDER BY total_sent DESC", tags=["join_agg_sub", "sent_and_orders"], desc="LEFT JOIN + 子查询 + 聚合"),
            self._seed("SELECT d.name, COUNT(e.id) AS emp_count, ROUND(AVG(e.salary), 2) AS avg_sal, MAX(e.salary) AS max_sal, MIN(e.salary) AS min_sal FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name ORDER BY avg_sal DESC", tags=["join_agg_sub", "dept_full_stats"], desc="多聚合 + LEFT JOIN"),
            self._seed("SELECT e.name, d.name AS dept, e.salary, RANK() OVER (PARTITION BY e.dept_id ORDER BY e.salary DESC) AS dept_rank FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.salary IS NOT NULL ORDER BY d.name, dept_rank", tags=["join_agg_sub", "dept_ranked"], desc="JOIN + RANK"),
            self._seed("SELECT u.username, COALESCE(SUM(o.total_amount), 0) AS total, CASE WHEN COALESCE(SUM(o.total_amount), 0) > 200 THEN 'VIP' ELSE 'regular' END AS tier FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id GROUP BY u.id, u.username ORDER BY total DESC", tags=["join_agg_sub", "user_tier"], desc="LEFT JOIN + SUM + CASE"),
            self._seed("SELECT d.name, e.name, e.salary, SUM(e.salary) OVER (PARTITION BY d.id ORDER BY e.salary DESC ROWS UNBOUNDED PRECEDING) AS dept_running FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.salary IS NOT NULL ORDER BY d.name, e.salary DESC", tags=["join_agg_sub", "dept_running"], desc="JOIN + 窗口累计"),
        ]

    def _join_window(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT e.name, d.name AS dept, e.salary, ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY e.salary DESC) AS rnk FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.salary IS NOT NULL ORDER BY d.name, rnk", tags=["join_window", "dept_ranked"], desc="JOIN + ROW_NUMBER"),
            self._seed("SELECT u.username, o.total_amount, SUM(o.total_amount) OVER (PARTITION BY u.id ORDER BY o.id) AS running FROM t_users u JOIN t_orders o ON u.id = o.user_id ORDER BY u.id, o.id", tags=["join_window", "user_running"], desc="JOIN + SUM OVER"),
            self._seed("SELECT e.name, d.name AS dept, e.salary, AVG(e.salary) OVER (PARTITION BY d.id) AS dept_avg FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.salary IS NOT NULL ORDER BY d.name, e.salary DESC", tags=["join_window", "dept_avg_window"], desc="JOIN + AVG OVER"),
            self._seed("SELECT t.id, t.amount, u.username, LAG(t.amount) OVER (PARTITION BY t.from_user ORDER BY t.id) AS prev_amt FROM t_transactions t JOIN t_users u ON t.from_user = u.id WHERE t.amount IS NOT NULL ORDER BY t.from_user, t.id", tags=["join_window", "tx_lag"], desc="JOIN + LAG"),
            self._seed("SELECT e.name, d.name AS dept, e.salary, e.salary - AVG(e.salary) OVER (PARTITION BY d.id) AS deviation FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.salary IS NOT NULL AND d.id IS NOT NULL ORDER BY ABS(deviation) DESC", tags=["join_window", "salary_deviation"], desc="JOIN + 窗口偏差"),
            self._seed("SELECT e.name, d.name AS dept, e.salary, NTILE(3) OVER (ORDER BY e.salary DESC) AS salary_tier FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.salary IS NOT NULL ORDER BY e.salary DESC", tags=["join_window", "salary_tier"], desc="JOIN + NTILE"),
            self._seed("SELECT ev.id, ev.event_type, u.username, COUNT(*) OVER (PARTITION BY u.id) AS user_event_count FROM t_events ev JOIN t_users u ON ev.user_id = u.id ORDER BY u.id, ev.id", tags=["join_window", "event_count_window"], desc="JOIN + COUNT OVER"),
        ]

    def _cte_complex(self) -> List[SeedSQL]:
        return [
            self._seed("WITH dept_stats AS (SELECT dept_id, COUNT(*) AS cnt, AVG(salary) AS avg_sal, MAX(salary) AS max_sal FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL GROUP BY dept_id) SELECT d.name, ds.cnt, ROUND(ds.avg_sal, 2) AS avg_sal, ds.max_sal FROM t_departments d JOIN dept_stats ds ON d.id = ds.dept_id ORDER BY ds.avg_sal DESC", tags=["cte_complex", "dept_full_report"], desc="CTE + JOIN + 多聚合"),
            self._seed("WITH ranked AS (SELECT id, name, salary, dept_id, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rnk FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL), top_earners AS (SELECT * FROM ranked WHERE rnk <= 2) SELECT t.name, t.salary, d.name AS dept, t.rnk FROM top_earners t JOIN t_departments d ON t.dept_id = d.id ORDER BY d.name, t.rnk", tags=["cte_complex", "top2_per_dept"], desc="两个 CTE + 窗口 + JOIN"),
            self._seed("WITH user_totals AS (SELECT user_id, COUNT(*) AS cnt, SUM(total_amount) AS total FROM t_orders GROUP BY user_id) SELECT u.username, COALESCE(ut.cnt, 0) AS orders, COALESCE(ut.total, 0) AS total, CASE WHEN COALESCE(ut.total, 0) > 200 THEN 'VIP' WHEN COALESCE(ut.cnt, 0) > 1 THEN 'repeat' ELSE 'new' END AS tier FROM t_users u LEFT JOIN user_totals ut ON u.id = ut.user_id ORDER BY total DESC", tags=["cte_complex", "user_tier_cte"], desc="CTE + LEFT JOIN + CASE"),
            self._seed("WITH dept_salary AS (SELECT d.name AS dept, SUM(e.salary) AS total_sal, COUNT(e.id) AS emp_cnt FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name) SELECT dept, total_sal, emp_cnt, CASE WHEN emp_cnt > 0 THEN ROUND(total_sal / emp_cnt, 2) ELSE 0 END AS avg_sal FROM dept_salary ORDER BY total_sal DESC", tags=["cte_complex", "dept_avg_cte"], desc="CTE + CASE 计算"),
            self._seed("WITH active_users AS (SELECT u.id, u.username, COUNT(ev.id) AS event_cnt FROM t_users u LEFT JOIN t_events ev ON u.id = ev.user_id GROUP BY u.id, u.username) SELECT username, event_cnt, RANK() OVER (ORDER BY event_cnt DESC) AS activity_rank FROM active_users ORDER BY activity_rank", tags=["cte_complex", "user_rank_cte"], desc="CTE + RANK"),
            self._seed("WITH monthly_tx AS (SELECT tx_type, status, COUNT(*) AS cnt, SUM(amount) AS total FROM t_transactions WHERE tx_type IS NOT NULL AND status IS NOT NULL GROUP BY tx_type, status) SELECT tx_type, status, cnt, total, ROUND(total * 100.0 / SUM(total) OVER (PARTITION BY tx_type), 2) AS pct FROM monthly_tx ORDER BY tx_type, status", tags=["cte_complex", "tx_pct_cte"], desc="CTE + 窗口百分比"),
            self._seed("WITH emp_dept AS (SELECT e.id, e.name, e.salary, e.dept_id, d.name AS dept_name, d.budget FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.salary IS NOT NULL) SELECT dept_name, COUNT(*) AS cnt, SUM(salary) AS total_sal, budget - SUM(salary) AS budget_remaining FROM emp_dept GROUP BY dept_name, budget ORDER BY budget_remaining DESC", tags=["cte_complex", "budget_remaining"], desc="CTE + JOIN + GROUP BY + 计算"),
            self._seed("WITH user_activity AS (SELECT u.id, u.username, COUNT(DISTINCT o.id) AS order_cnt, COUNT(DISTINCT ev.id) AS event_cnt, COALESCE(SUM(t.amount), 0) AS total_tx FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id LEFT JOIN t_events ev ON u.id = ev.user_id LEFT JOIN t_transactions t ON u.id = t.from_user AND t.amount > 0 GROUP BY u.id, u.username) SELECT username, order_cnt, event_cnt, total_tx, order_cnt + event_cnt AS total_activity FROM user_activity ORDER BY total_activity DESC", tags=["cte_complex", "full_user_report"], desc="CTE + 多 LEFT JOIN + 聚合"),
            self._seed("WITH salary_bands AS (SELECT id, name, salary, dept_id, CASE WHEN salary >= 120000 THEN 'senior' WHEN salary >= 80000 THEN 'mid' WHEN salary >= 50000 THEN 'junior' ELSE 'entry' END AS band FROM t_employees WHERE salary IS NOT NULL) SELECT band, COUNT(*) AS cnt, AVG(salary) AS avg_sal, MIN(salary) AS min_sal, MAX(salary) AS max_sal FROM salary_bands GROUP BY band ORDER BY avg_sal DESC", tags=["cte_complex", "salary_bands_cte"], desc="CTE + CASE + GROUP BY + 多聚合"),
            self._seed("WITH event_summary AS (SELECT event_type, user_id, COUNT(*) OVER (PARTITION BY event_type) AS type_total, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_date DESC) AS recent_rank FROM t_events WHERE user_id IS NOT NULL AND event_date IS NOT NULL) SELECT event_type, user_id, type_total FROM event_summary WHERE recent_rank <= 3 ORDER BY event_type, recent_rank", tags=["cte_complex", "recent_events_cte"], desc="CTE + 双窗口 + 过滤"),
        ]
