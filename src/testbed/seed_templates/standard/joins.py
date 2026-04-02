"""多表 JOIN 标准模板 — 验证标准 JOIN SQL 正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class StandardJoinsTemplate(SeedTemplate):

    @property
    def domain(self) -> str:
        return "joins"

    @property
    def description(self) -> str:
        return "标准SQL多表JOIN测试（INNER/LEFT/自连接/3+表）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._inner())
        seeds.extend(self._left())
        seeds.extend(self._self())
        seeds.extend(self._multi())
        return seeds

    def _inner(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT e.id, e.name, d.name AS dept FROM t_employees e JOIN t_departments d ON e.dept_id = d.id ORDER BY e.id", tags=["inner", "emp_dept"], desc="INNER JOIN 员工+部门"),
            self._seed("SELECT u.id, u.username, o.id AS oid FROM t_users u JOIN t_orders o ON u.id = o.user_id ORDER BY u.id", tags=["inner", "user_order"], desc="INNER JOIN 用户+订单"),
            self._seed("SELECT ev.id, ev.event_type, u.username FROM t_events ev JOIN t_users u ON ev.user_id = u.id ORDER BY ev.id", tags=["inner", "event_user"], desc="INNER JOIN 事件+用户"),
            self._seed("SELECT t.id, t.amount, u.username FROM t_transactions t JOIN t_users u ON t.from_user = u.id ORDER BY t.id", tags=["inner", "tx_sender"], desc="INNER JOIN 交易+发送者"),
            self._seed("SELECT e.id, e.name, d.name AS dept, d.location FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE d.location IS NOT NULL ORDER BY e.id", tags=["inner", "emp_located_dept"], desc="JOIN + WHERE"),
            self._seed("SELECT u.username, o.status FROM t_users u JOIN t_orders o ON u.id = o.user_id WHERE o.status = 'completed' ORDER BY u.id", tags=["inner", "completed_orders"], desc="JOIN + 条件过滤"),
            self._seed("SELECT e.name, d.name AS dept FROM t_employees e JOIN t_departments d ON e.dept_id = d.id ORDER BY d.name, e.name", tags=["inner", "sorted_join"], desc="JOIN + ORDER BY"),
            self._seed("SELECT p.name, o.status FROM t_products p JOIN t_orders o ON p.id = o.product_id ORDER BY p.id", tags=["inner", "product_order"], desc="JOIN 产品+订单"),
            self._seed("SELECT e.id, e.name, m.name AS mgr FROM t_employees e JOIN t_employees m ON e.manager_id = m.id ORDER BY e.id", tags=["inner", "self_join"], desc="自连接 员工+经理"),
            self._seed("SELECT d1.name, d2.name AS parent FROM t_departments d1 JOIN t_departments d2 ON d1.parent_id = d2.id ORDER BY d1.id", tags=["inner", "dept_parent"], desc="自连接 部门+父部门"),
            self._seed("SELECT t.id, t.amount, u1.username AS sender, u2.username AS receiver FROM t_transactions t JOIN t_users u1 ON t.from_user = u1.id JOIN t_users u2 ON t.to_user = u2.id ORDER BY t.id", tags=["inner", "tx_both"], desc="三表 JOIN 交易+双方"),
        ]

    def _left(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT e.id, e.name, d.name AS dept FROM t_employees e LEFT JOIN t_departments d ON e.dept_id = d.id ORDER BY e.id", tags=["left", "emp_dept"], desc="LEFT JOIN 员工+部门"),
            self._seed("SELECT u.id, u.username, o.id AS oid FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id ORDER BY u.id", tags=["left", "user_order"], desc="LEFT JOIN 用户+订单"),
            self._seed("SELECT d.id, d.name, COUNT(e.id) AS cnt FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name ORDER BY d.id", tags=["left", "dept_count"], desc="LEFT JOIN + GROUP BY"),
            self._seed("SELECT e.id, e.name, m.name AS mgr FROM t_employees e LEFT JOIN t_employees m ON e.manager_id = m.id ORDER BY e.id", tags=["left", "emp_mgr"], desc="LEFT JOIN 自连接"),
            self._seed("SELECT u.id, u.username, COALESCE(SUM(o.total_amount), 0) AS total FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id GROUP BY u.id, u.username ORDER BY u.id", tags=["left", "user_total"], desc="LEFT JOIN + 聚合"),
            self._seed("SELECT d.name, d.budget, SUM(e.salary) AS total_sal FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name, d.budget ORDER BY d.id", tags=["left", "dept_salary"], desc="LEFT JOIN + SUM"),
            self._seed("SELECT ev.id, ev.event_type, u.username FROM t_events ev LEFT JOIN t_users u ON ev.user_id = u.id ORDER BY ev.id", tags=["left", "event_user"], desc="LEFT JOIN 事件+用户"),
            self._seed("SELECT t.id, t.amount, u.username FROM t_transactions t LEFT JOIN t_users u ON t.from_user = u.id ORDER BY t.id", tags=["left", "tx_sender"], desc="LEFT JOIN 交易+发送者"),
            self._seed("SELECT e.id, e.name, d.name AS dept, COALESCE(d.name, 'N/A') AS dept_display FROM t_employees e LEFT JOIN t_departments d ON e.dept_id = d.id ORDER BY e.id", tags=["left", "coalesce"], desc="LEFT JOIN + COALESCE"),
            self._seed("SELECT u.id, u.username, COUNT(o.id) AS orders FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id GROUP BY u.id, u.username HAVING COUNT(o.id) = 0 ORDER BY u.id", tags=["left", "no_orders"], desc="LEFT JOIN + HAVING = 0"),
            self._seed("SELECT d.name AS dept, d.parent_id, p.name AS parent FROM t_departments d LEFT JOIN t_departments p ON d.parent_id = p.id ORDER BY d.id", tags=["left", "dept_parent"], desc="LEFT JOIN 自连接 部门"),
        ]

    def _self(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT e.name AS emp, m.name AS mgr FROM t_employees e LEFT JOIN t_employees m ON e.manager_id = m.id ORDER BY e.id", tags=["self", "emp_mgr"], desc="自连接 员工+经理"),
            self._seed("SELECT d1.name AS dept, d2.name AS parent FROM t_departments d1 LEFT JOIN t_departments d2 ON d1.parent_id = d2.id ORDER BY d1.id", tags=["self", "dept_tree"], desc="自连接 部门树"),
            self._seed("SELECT e.name, e.salary, m.name AS mgr, m.salary AS mgr_sal FROM t_employees e JOIN t_employees m ON e.manager_id = m.id ORDER BY e.id", tags=["self", "salary_compare"], desc="自连接 薪资对比"),
        ]

    def _multi(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT e.name, d.name AS dept, m.name AS mgr FROM t_employees e JOIN t_departments d ON e.dept_id = d.id LEFT JOIN t_employees m ON e.manager_id = m.id ORDER BY e.id", tags=["multi", "emp_dept_mgr"], desc="三表 JOIN"),
            self._seed("SELECT u.username, o.status, p.name FROM t_users u JOIN t_orders o ON u.id = o.user_id JOIN t_products p ON o.product_id = p.id ORDER BY u.id", tags=["multi", "user_order_product"], desc="三表 JOIN 用户+订单+产品"),
            self._seed("SELECT d.name, e.name, m.name AS mgr FROM t_departments d JOIN t_employees e ON d.id = e.dept_id LEFT JOIN t_employees m ON e.manager_id = m.id ORDER BY d.name, e.id", tags=["multi", "dept_emp_mgr"], desc="三表 JOIN 部门+员工+经理"),
            self._seed("SELECT u.username, SUM(t.amount) AS total, d.name AS dept FROM t_users u LEFT JOIN t_transactions t ON u.id = t.from_user LEFT JOIN t_employees e ON u.username = e.name LEFT JOIN t_departments d ON e.dept_id = d.id GROUP BY u.id, u.username, d.name ORDER BY total DESC", tags=["multi", "four_table"], desc="四表 LEFT JOIN + 聚合"),
            self._seed("SELECT e.name, d.name AS dept, d2.name AS parent_dept FROM t_employees e JOIN t_departments d ON e.dept_id = d.id LEFT JOIN t_departments d2 ON d.parent_id = d2.id ORDER BY e.id", tags=["multi", "emp_dept_parent"], desc="三表 JOIN 含自连接"),
            self._seed("SELECT u.username, COUNT(o.id) AS orders, COUNT(ev.id) AS events FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id LEFT JOIN t_events ev ON u.id = ev.user_id GROUP BY u.id, u.username ORDER BY u.id", tags=["multi", "user_orders_events"], desc="多表 LEFT JOIN + COUNT"),
            self._seed("SELECT e.name, d.name AS dept, e.salary, d.budget FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE d.budget > 0 ORDER BY d.name, e.salary DESC", tags=["multi", "filtered_join"], desc="多表 JOIN + WHERE"),
            self._seed("SELECT d.name, COUNT(e.id) AS emp_count, AVG(e.salary) AS avg_sal FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name ORDER BY d.name", tags=["multi", "dept_summary"], desc="多表 JOIN + GROUP BY"),
            self._seed("SELECT t.id, t.amount, u1.username AS sender, u2.username AS receiver, t.status FROM t_transactions t LEFT JOIN t_users u1 ON t.from_user = u1.id LEFT JOIN t_users u2 ON t.to_user = u2.id ORDER BY t.id", tags=["multi", "tx_full"], desc="三表 LEFT JOIN 完整交易"),
            self._seed("SELECT e.name, d.name AS dept, e.salary FROM t_employees e JOIN t_departments d ON e.dept_id = d.id WHERE e.salary > (SELECT AVG(salary) FROM t_employees WHERE salary IS NOT NULL) ORDER BY e.salary DESC", tags=["multi", "join_subquery"], desc="JOIN + 子查询"),
        ]
