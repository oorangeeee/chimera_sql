"""子查询标准模板 — 验证标准子查询 SQL 正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class StandardSubqueriesTemplate(SeedTemplate):

    @property
    def domain(self) -> str:
        return "subqueries"

    @property
    def description(self) -> str:
        return "标准SQL子查询测试（标量/IN/EXISTS/派生表/相关子查询）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._scalar())
        seeds.extend(self._in_subquery())
        seeds.extend(self._exists())
        seeds.extend(self._derived())
        seeds.extend(self._correlated())
        return seeds

    def _scalar(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, score - (SELECT AVG(score) FROM t_users WHERE score IS NOT NULL) AS diff FROM t_users WHERE score IS NOT NULL ORDER BY diff DESC", tags=["scalar", "score_vs_avg"], desc="标量子查询 — 与平均值差"),
            self._seed("SELECT id, name, price - (SELECT AVG(price) FROM t_products) AS diff FROM t_products WHERE price IS NOT NULL ORDER BY diff DESC", tags=["scalar", "price_vs_avg"], desc="标量子查询 — 价格偏差"),
            self._seed("SELECT id, name, salary - (SELECT AVG(salary) FROM t_employees WHERE salary IS NOT NULL) AS diff FROM t_employees WHERE salary IS NOT NULL ORDER BY diff DESC", tags=["scalar", "salary_vs_avg"], desc="标量子查询 — 薪资偏差"),
            self._seed("SELECT id, amount, amount / (SELECT MAX(ABS(amount)) FROM t_transactions) AS pct FROM t_transactions WHERE amount IS NOT NULL ORDER BY id", tags=["scalar", "amount_pct"], desc="标量子查询 — 占比"),
            self._seed("SELECT id, name, salary, (SELECT COUNT(*) FROM t_employees e2 WHERE e2.salary > t_employees.salary) AS higher_count FROM t_employees WHERE salary IS NOT NULL ORDER BY salary DESC", tags=["scalar", "salary_rank"], desc="相关标量子查询 — 排名"),
            self._seed("SELECT id, username, score, (SELECT MAX(score) FROM t_users) AS max_score FROM t_users WHERE score IS NOT NULL ORDER BY score DESC", tags=["scalar", "score_with_max"], desc="标量子查询 — 含最大值"),
            self._seed("SELECT d.name, d.budget, (SELECT SUM(e.salary) FROM t_employees e WHERE e.dept_id = d.id) AS total_salary FROM t_departments d ORDER BY d.id", tags=["scalar", "dept_salary"], desc="相关标量子查询 — 部门薪资"),
            self._seed("SELECT id, amount, (SELECT AVG(amount) FROM t_transactions WHERE tx_type = t.tx_type) AS type_avg FROM t_transactions t WHERE amount IS NOT NULL AND tx_type IS NOT NULL ORDER BY id", tags=["scalar", "type_avg"], desc="相关标量子查询 — 类型平均"),
            self._seed("SELECT id, event_type, (SELECT COUNT(*) FROM t_events e2 WHERE e2.event_type = t_events.event_type) AS type_count FROM t_events t ORDER BY id", tags=["scalar", "event_type_count"], desc="相关标量子查询 — 事件计数"),
        ]

    def _in_subquery(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username FROM t_users WHERE id IN (SELECT user_id FROM t_orders) ORDER BY id", tags=["in_sub", "users_with_orders"], desc="IN 子查询 — 有订单的用户"),
            self._seed("SELECT id, name FROM t_employees WHERE dept_id IN (SELECT id FROM t_departments WHERE budget > 100000) ORDER BY id", tags=["in_sub", "high_budget_dept"], desc="IN 子查询 — 高预算部门员工"),
            self._seed("SELECT id, username FROM t_users WHERE id IN (SELECT user_id FROM t_events WHERE event_type = 'login') ORDER BY id", tags=["in_sub", "login_users"], desc="IN 子查询 — 登录用户"),
            self._seed("SELECT id, name FROM t_products WHERE id IN (SELECT product_id FROM t_orders WHERE status = 'completed') ORDER BY id", tags=["in_sub", "ordered_products"], desc="IN 子查询 — 已订购产品"),
            self._seed("SELECT id, name FROM t_employees WHERE dept_id NOT IN (SELECT id FROM t_departments WHERE budget IS NULL) AND dept_id IS NOT NULL ORDER BY id", tags=["in_sub", "not_null_budget_dept"], desc="NOT IN 子查询"),
            self._seed("SELECT id, username FROM t_users WHERE id NOT IN (SELECT user_id FROM t_orders WHERE user_id IS NOT NULL) ORDER BY id", tags=["in_sub", "no_orders"], desc="NOT IN — 无订单用户"),
            self._seed("SELECT id FROM t_departments WHERE id IN (SELECT dept_id FROM t_employees WHERE salary > 100000) ORDER BY id", tags=["in_sub", "dept_with_high_salary"], desc="IN 子查询 — 含高薪员工部门"),
            self._seed("SELECT id, from_user FROM t_transactions WHERE from_user IN (SELECT id FROM t_users WHERE score > 80) ORDER BY id", tags=["in_sub", "tx_from_high_score"], desc="IN 子查询 — 高分用户交易"),
            self._seed("SELECT id, name FROM t_employees WHERE manager_id IN (SELECT id FROM t_employees WHERE salary > 120000) ORDER BY id", tags=["in_sub", "managed_by_high"], desc="IN 子查询 — 高薪经理下属"),
        ]

    def _exists(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT u.id, u.username FROM t_users u WHERE EXISTS (SELECT 1 FROM t_orders o WHERE o.user_id = u.id) ORDER BY u.id", tags=["exists", "users_with_orders"], desc="EXISTS — 有订单用户"),
            self._seed("SELECT u.id, u.username FROM t_users u WHERE NOT EXISTS (SELECT 1 FROM t_orders o WHERE o.user_id = u.id) ORDER BY u.id", tags=["exists", "no_orders"], desc="NOT EXISTS — 无订单用户"),
            self._seed("SELECT d.id, d.name FROM t_departments d WHERE EXISTS (SELECT 1 FROM t_employees e WHERE e.dept_id = d.id AND e.salary > 100000) ORDER BY d.id", tags=["exists", "dept_high_salary"], desc="EXISTS — 含高薪员工部门"),
            self._seed("SELECT e.id, e.name FROM t_employees e WHERE EXISTS (SELECT 1 FROM t_employees m WHERE m.id = e.manager_id AND m.salary > 120000) ORDER BY e.id", tags=["exists", "high_mgr"], desc="EXISTS — 有高薪经理"),
            self._seed("SELECT u.id, u.username FROM t_users u WHERE EXISTS (SELECT 1 FROM t_events ev WHERE ev.user_id = u.id AND ev.event_type = 'purchase') ORDER BY u.id", tags=["exists", "purchasing_users"], desc="EXISTS — 有购买行为用户"),
            self._seed("SELECT d.id, d.name FROM t_departments d WHERE NOT EXISTS (SELECT 1 FROM t_employees e WHERE e.dept_id = d.id) ORDER BY d.id", tags=["exists", "empty_depts"], desc="NOT EXISTS — 空部门"),
            self._seed("SELECT e.id, e.name FROM t_employees e WHERE EXISTS (SELECT 1 FROM t_transactions t WHERE t.from_user IN (SELECT id FROM t_users WHERE username = e.name)) ORDER BY e.id", tags=["exists", "emp_with_tx"], desc="EXISTS + 嵌套子查询"),
            self._seed("SELECT u.id, u.username FROM t_users u WHERE EXISTS (SELECT 1 FROM t_orders o WHERE o.user_id = u.id AND o.status = 'completed') AND EXISTS (SELECT 1 FROM t_events ev WHERE ev.user_id = u.id) ORDER BY u.id", tags=["exists", "dual_exists"], desc="双 EXISTS — 有订单且有事件"),
            self._seed("SELECT d.name FROM t_departments d WHERE EXISTS (SELECT 1 FROM t_departments child WHERE child.parent_id = d.id) ORDER BY d.id", tags=["exists", "parent_depts"], desc="EXISTS 自引用 — 有子部门"),
        ]

    def _derived(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT sub.dept_id, sub.cnt FROM (SELECT dept_id, COUNT(*) AS cnt FROM t_employees WHERE dept_id IS NOT NULL GROUP BY dept_id) sub ORDER BY sub.cnt DESC", tags=["derived", "dept_counts"], desc="派生表 — 部门人数"),
            self._seed("SELECT sub.category, sub.avg_price FROM (SELECT category, AVG(price) AS avg_price FROM t_products WHERE category IS NOT NULL GROUP BY category) sub ORDER BY sub.avg_price DESC", tags=["derived", "cat_avg_price"], desc="派生表 — 分类均价"),
            self._seed("SELECT u.username, sub.total FROM t_users u JOIN (SELECT user_id, SUM(total_amount) AS total FROM t_orders GROUP BY user_id) sub ON u.id = sub.user_id ORDER BY sub.total DESC", tags=["derived", "user_total_join"], desc="派生表 + JOIN"),
            self._seed("SELECT sub.status, sub.cnt FROM (SELECT status, COUNT(*) AS cnt FROM t_transactions WHERE status IS NOT NULL GROUP BY status) sub ORDER BY sub.cnt DESC", tags=["derived", "tx_status_counts"], desc="派生表 — 交易状态计数"),
            self._seed("SELECT sub.dept, sub.emp_count FROM (SELECT d.name AS dept, COUNT(e.id) AS emp_count FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name) sub WHERE sub.emp_count > 0 ORDER BY sub.emp_count DESC", tags=["derived", "dept_filtered"], desc="派生表 + WHERE 过滤"),
            self._seed("SELECT sub.event_type, sub.cnt FROM (SELECT event_type, COUNT(*) AS cnt FROM t_events GROUP BY event_type) sub ORDER BY sub.cnt DESC", tags=["derived", "event_counts"], desc="派生表 — 事件类型计数"),
            self._seed("SELECT u.username, COALESCE(sub.total, 0) AS total FROM t_users u LEFT JOIN (SELECT from_user, SUM(amount) AS total FROM t_transactions WHERE from_user IS NOT NULL GROUP BY from_user) sub ON u.id = sub.from_user ORDER BY u.id", tags=["derived", "user_tx_total"], desc="派生表 + LEFT JOIN"),
            self._seed("SELECT * FROM (SELECT id, name, salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk FROM t_employees WHERE salary IS NOT NULL) sub WHERE rnk <= 5 ORDER BY rnk", tags=["derived", "top_salary"], desc="派生表 + 窗口函数 + 过滤"),
        ]

    def _correlated(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT e.id, e.name, e.salary, (SELECT AVG(e2.salary) FROM t_employees e2 WHERE e2.dept_id = e.dept_id AND e2.salary IS NOT NULL) AS dept_avg FROM t_employees e WHERE e.dept_id IS NOT NULL ORDER BY e.id", tags=["correlated", "emp_dept_avg"], desc="相关子查询 — 部门平均"),
            self._seed("SELECT u.id, u.username, (SELECT COUNT(*) FROM t_orders o WHERE o.user_id = u.id) AS order_count FROM t_users u ORDER BY u.id", tags=["correlated", "user_order_count"], desc="相关子查询 — 订单计数"),
            self._seed("SELECT d.id, d.name, (SELECT MAX(e.salary) FROM t_employees e WHERE e.dept_id = d.id) AS max_salary FROM t_departments d ORDER BY d.id", tags=["correlated", "dept_max_salary"], desc="相关子查询 — 部门最高薪"),
            self._seed("SELECT e.id, e.name, e.salary FROM t_employees e WHERE e.salary > (SELECT AVG(e2.salary) FROM t_employees e2 WHERE e2.dept_id = e.dept_id AND e2.salary IS NOT NULL) ORDER BY e.id", tags=["correlated", "above_dept_avg"], desc="相关子查询 — 高于部门平均"),
            self._seed("SELECT p.id, p.name, (SELECT COUNT(*) FROM t_orders o WHERE o.product_id = p.id) AS order_count FROM t_products p ORDER BY p.id", tags=["correlated", "product_order_count"], desc="相关子查询 — 产品订单数"),
            self._seed("SELECT u.id, u.username, (SELECT COUNT(*) FROM t_events ev WHERE ev.user_id = u.id) AS event_count FROM t_users u ORDER BY u.id", tags=["correlated", "user_event_count"], desc="相关子查询 — 用户事件数"),
            self._seed("SELECT t.id, t.amount, (SELECT AVG(t2.amount) FROM t_transactions t2 WHERE t2.tx_type = t.tx_type AND t2.amount IS NOT NULL) AS type_avg FROM t_transactions t WHERE t.amount IS NOT NULL ORDER BY t.id", tags=["correlated", "tx_type_avg"], desc="相关子查询 — 类型平均"),
            self._seed("SELECT e.id, e.name, e.salary, e.dept_id, (SELECT SUM(e2.salary) FROM t_employees e2 WHERE e2.dept_id = e.dept_id) AS dept_total FROM t_employees e WHERE e.dept_id IS NOT NULL ORDER BY e.id", tags=["correlated", "dept_total"], desc="相关子查询 — 部门总额"),
            self._seed("SELECT d.name, (SELECT COUNT(*) FROM t_departments child WHERE child.parent_id = d.id) AS child_count FROM t_departments d ORDER BY d.id", tags=["correlated", "child_dept_count"], desc="相关子查询 — 子部门计数"),
        ]
