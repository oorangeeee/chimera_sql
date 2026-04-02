"""JOIN 方言差异模板 — 测试 SQLite/Oracle JOIN 语义差异。

覆盖差异点：
- INNER/LEFT/CROSS/自连接
- 多表 JOIN（3+表）
- JOIN + 聚合
- 外连接 + NULL 行为
- JOIN 条件中的 NULL
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class JoinsTemplate(SeedTemplate):
    """JOIN 方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "joins"

    @property
    def description(self) -> str:
        return "JOIN 方言差异测试（INNER/LEFT/CROSS/自连接/多表/外连接+NULL）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._inner_joins())
        seeds.extend(self._left_joins())
        seeds.extend(self._cross_joins())
        seeds.extend(self._self_joins())
        seeds.extend(self._multi_table_joins())
        seeds.extend(self._join_aggregate())
        seeds.extend(self._join_null())
        return seeds

    # ── INNER JOIN (~10) ─────────────────────────────────
    def _inner_joins(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT e.id, e.name, d.name AS dept_name "
                "FROM t_employees e INNER JOIN t_departments d ON e.dept_id = d.id "
                "ORDER BY e.id",
                tags=["inner_join", "emp_dept"],
                desc="INNER JOIN — 员工+部门",
            ),
            self._seed(
                "SELECT u.id, u.username, o.id AS order_id, o.status "
                "FROM t_users u INNER JOIN t_orders o ON u.id = o.user_id "
                "ORDER BY u.id, o.id",
                tags=["inner_join", "user_orders"],
                desc="INNER JOIN — 用户+订单",
            ),
            self._seed(
                "SELECT e.id, e.name, e.salary, d.name AS dept, d.budget "
                "FROM t_employees e INNER JOIN t_departments d ON e.dept_id = d.id "
                "WHERE e.salary > 80000 ORDER BY e.salary DESC",
                tags=["inner_join", "high_salary_dept"],
                desc="INNER JOIN + WHERE — 高薪员工及部门",
            ),
            self._seed(
                "SELECT e.id, e.name, d.name AS dept "
                "FROM t_employees e JOIN t_departments d ON e.dept_id = d.id "
                "WHERE d.budget IS NOT NULL ORDER BY e.id",
                tags=["inner_join", "dept_with_budget"],
                desc="JOIN — 有预算部门的员工",
            ),
            self._seed(
                "SELECT t.id, t.amount, u.username AS sender "
                "FROM t_transactions t INNER JOIN t_users u ON t.from_user = u.id "
                "ORDER BY t.id",
                tags=["inner_join", "tx_sender"],
                desc="INNER JOIN — 交易+发送者",
            ),
            self._seed(
                "SELECT ev.id, ev.event_type, u.username "
                "FROM t_events ev INNER JOIN t_users u ON ev.user_id = u.id "
                "ORDER BY ev.id",
                tags=["inner_join", "event_user"],
                desc="INNER JOIN — 事件+用户",
            ),
            self._seed(
                "SELECT t.id, t.amount, u1.username AS sender, u2.username AS receiver "
                "FROM t_transactions t "
                "INNER JOIN t_users u1 ON t.from_user = u1.id "
                "INNER JOIN t_users u2 ON t.to_user = u2.id "
                "ORDER BY t.id",
                tags=["inner_join", "tx_both_users"],
                desc="双 INNER JOIN — 交易+双方用户",
            ),
            self._seed(
                "SELECT u.username, o.status, o.total_amount "
                "FROM t_users u INNER JOIN t_orders o ON u.id = o.user_id "
                "WHERE o.status = 'completed' ORDER BY o.total_amount DESC",
                tags=["inner_join", "completed_orders"],
                desc="INNER JOIN — 已完成订单",
            ),
            self._seed(
                "SELECT e.id, e.name, e.salary, d.name AS dept "
                "FROM t_employees e INNER JOIN t_departments d ON e.dept_id = d.id "
                "WHERE e.hire_date IS NOT NULL ORDER BY e.hire_date DESC",
                tags=["inner_join", "recent_hires"],
                desc="INNER JOIN + WHERE NOT NULL — 最近入职",
            ),
            self._seed(
                "SELECT e.id, e.name, d.name AS dept, d.location "
                "FROM t_employees e INNER JOIN t_departments d ON e.dept_id = d.id "
                "WHERE d.location IS NOT NULL "
                "ORDER BY d.name, e.name",
                tags=["inner_join", "emp_with_location"],
                desc="INNER JOIN — 有位置的部门员工",
            ),
        ]

    # ── LEFT JOIN (~10) ──────────────────────────────────
    def _left_joins(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT e.id, e.name, d.name AS dept_name "
                "FROM t_employees e LEFT JOIN t_departments d ON e.dept_id = d.id "
                "ORDER BY e.id",
                tags=["left_join", "emp_dept_left"],
                desc="LEFT JOIN — 所有员工含无部门",
            ),
            self._seed(
                "SELECT u.id, u.username, o.id AS order_id, o.status "
                "FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id "
                "ORDER BY u.id, o.id",
                tags=["left_join", "user_orders_left"],
                desc="LEFT JOIN — 所有用户含无订单",
            ),
            self._seed(
                "SELECT d.id, d.name, COUNT(e.id) AS emp_count "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name ORDER BY d.id",
                tags=["left_join", "dept_emp_count"],
                desc="LEFT JOIN + GROUP BY — 含空部门",
            ),
            self._seed(
                "SELECT u.id, u.username, "
                "CASE WHEN o.id IS NULL THEN 'no_orders' ELSE 'has_orders' END AS order_status "
                "FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id "
                "GROUP BY u.id, u.username ORDER BY u.id",
                tags=["left_join", "user_order_flag"],
                desc="LEFT JOIN + CASE — 订单存在性标记",
            ),
            self._seed(
                "SELECT e.id, e.name, m.name AS manager_name "
                "FROM t_employees e LEFT JOIN t_employees m ON e.manager_id = m.id "
                "ORDER BY e.id",
                tags=["left_join", "emp_manager"],
                desc="LEFT JOIN 自连接 — 员工+经理名",
            ),
            self._seed(
                "SELECT d.id, d.name, d.budget, "
                "SUM(e.salary) AS total_salary "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name, d.budget ORDER BY d.id",
                tags=["left_join", "dept_salary_total"],
                desc="LEFT JOIN + SUM — 含无员工部门",
            ),
            self._seed(
                "SELECT ev.id, ev.event_type, u.username "
                "FROM t_events ev LEFT JOIN t_users u ON ev.user_id = u.id "
                "ORDER BY ev.id",
                tags=["left_join", "event_user_left"],
                desc="LEFT JOIN — 事件含无用户事件",
            ),
            self._seed(
                "SELECT t.id, t.amount, u.username AS receiver "
                "FROM t_transactions t LEFT JOIN t_users u ON t.to_user = u.id "
                "ORDER BY t.id",
                tags=["left_join", "tx_receiver_left"],
                desc="LEFT JOIN — 交易含无接收者",
            ),
            self._seed(
                "SELECT u.id, u.username, "
                "COUNT(DISTINCT o.id) AS order_count, "
                "COALESCE(SUM(o.total_amount), 0) AS total_spent "
                "FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id "
                "GROUP BY u.id, u.username ORDER BY u.id",
                tags=["left_join", "user_spending"],
                desc="LEFT JOIN + 聚合 — 用户消费含零消费",
            ),
            self._seed(
                "SELECT e.id, e.name, e.dept_id, d.name AS dept, "
                "COALESCE(d.name, 'No Department') AS dept_display "
                "FROM t_employees e LEFT JOIN t_departments d ON e.dept_id = d.id "
                "ORDER BY e.id",
                tags=["left_join", "emp_dept_coalesce"],
                desc="LEFT JOIN + COALESCE — NULL 部门名替换",
            ),
        ]

    # ── CROSS JOIN (~5) ──────────────────────────────────
    def _cross_joins(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT u.username, d.name AS dept "
                "FROM t_users u CROSS JOIN t_departments d "
                "ORDER BY u.id, d.id",
                tags=["cross_join", "user_dept_cross"],
                desc="CROSS JOIN — 用户×部门全组合",
            ),
            self._seed(
                "SELECT e.name, d.name AS dept "
                "FROM t_employees e CROSS JOIN t_departments d "
                "ORDER BY e.id, d.id LIMIT 20",
                tags=["cross_join", "emp_dept_cross"],
                desc="CROSS JOIN — 员工×部门（LIMIT）",
            ),
            self._seed(
                "SELECT p.name, d.name AS dept "
                "FROM t_products p CROSS JOIN t_departments d "
                "ORDER BY p.id, d.id LIMIT 20",
                tags=["cross_join", "product_dept_cross"],
                desc="CROSS JOIN — 产品×部门（LIMIT）",
            ),
            self._seed(
                "SELECT u.username, t.tx_type "
                "FROM t_users u CROSS JOIN (SELECT DISTINCT tx_type FROM t_transactions WHERE tx_type IS NOT NULL) t "
                "ORDER BY u.id, t.tx_type LIMIT 20",
                tags=["cross_join", "cross_subquery"],
                desc="CROSS JOIN 子查询",
            ),
            self._seed(
                "SELECT d.name, s.status "
                "FROM t_departments d CROSS JOIN (SELECT DISTINCT status FROM t_employees WHERE status IS NOT NULL) s "
                "ORDER BY d.id, s.status LIMIT 20",
                tags=["cross_join", "cross_status"],
                desc="CROSS JOIN 去重状态",
            ),
        ]

    # ── 自连接 (~5) ──────────────────────────────────────
    def _self_joins(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT d1.name AS dept, d2.name AS parent_dept "
                "FROM t_departments d1 LEFT JOIN t_departments d2 ON d1.parent_id = d2.id "
                "ORDER BY d1.id",
                tags=["self_join", "dept_parent"],
                desc="自连接 — 部门+父部门名",
            ),
            self._seed(
                "SELECT e.name AS employee, m.name AS manager, e.salary, m.salary AS mgr_salary "
                "FROM t_employees e INNER JOIN t_employees m ON e.manager_id = m.id "
                "ORDER BY e.id",
                tags=["self_join", "emp_mgr_salary"],
                desc="自连接 — 员工+经理薪资对比",
            ),
            self._seed(
                "SELECT e.name, e.salary, m.name AS manager, "
                "e.salary - m.salary AS salary_diff "
                "FROM t_employees e INNER JOIN t_employees m ON e.manager_id = m.id "
                "ORDER BY salary_diff DESC",
                tags=["self_join", "salary_diff"],
                desc="自连接 — 薪资差异",
            ),
            self._seed(
                "SELECT d1.name AS dept, d1.budget, d2.name AS parent, d2.budget AS parent_budget "
                "FROM t_departments d1 LEFT JOIN t_departments d2 ON d1.parent_id = d2.id "
                "WHERE d1.budget IS NOT NULL ORDER BY d1.id",
                tags=["self_join", "dept_budget_compare"],
                desc="自连接 — 部门预算对比",
            ),
            self._seed(
                "SELECT e.name AS employee, e.dept_id, m.name AS manager "
                "FROM t_employees e LEFT JOIN t_employees m ON e.manager_id = m.id "
                "WHERE e.dept_id IS NOT NULL "
                "ORDER BY e.dept_id, e.id",
                tags=["self_join", "emp_mgr_by_dept"],
                desc="自连接 — 按部门看员工-经理关系",
            ),
        ]

    # ── 多表 JOIN (~10) ──────────────────────────────────
    def _multi_table_joins(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT e.name, d.name AS dept, m.name AS manager "
                "FROM t_employees e "
                "JOIN t_departments d ON e.dept_id = d.id "
                "LEFT JOIN t_employees m ON e.manager_id = m.id "
                "ORDER BY e.id",
                tags=["multi_join", "emp_dept_mgr"],
                desc="三表 JOIN — 员工+部门+经理",
            ),
            self._seed(
                "SELECT u.username, o.status, p.name AS product "
                "FROM t_users u "
                "JOIN t_orders o ON u.id = o.user_id "
                "JOIN t_products p ON o.product_id = p.id "
                "ORDER BY u.id, o.id",
                tags=["multi_join", "user_order_product"],
                desc="三表 JOIN — 用户+订单+产品",
            ),
            self._seed(
                "SELECT u.username, e.event_type, d.name AS dept "
                "FROM t_users u "
                "LEFT JOIN t_events e ON u.id = e.user_id "
                "LEFT JOIN t_departments d ON d.id IN "
                "(SELECT dept_id FROM t_employees WHERE id = u.id) "
                "ORDER BY u.id",
                tags=["multi_join", "user_event_dept"],
                desc="多表 LEFT JOIN — 用户+事件+部门",
            ),
            self._seed(
                "SELECT e.name, d.name AS dept, d2.name AS parent_dept "
                "FROM t_employees e "
                "JOIN t_departments d ON e.dept_id = d.id "
                "LEFT JOIN t_departments d2 ON d.parent_id = d2.id "
                "ORDER BY e.id",
                tags=["multi_join", "emp_dept_parent"],
                desc="三表 JOIN — 员工+部门+父部门",
            ),
            self._seed(
                "SELECT t.id, t.amount, u1.username AS sender, u2.username AS receiver, "
                "t.status "
                "FROM t_transactions t "
                "LEFT JOIN t_users u1 ON t.from_user = u1.id "
                "LEFT JOIN t_users u2 ON t.to_user = u2.id "
                "ORDER BY t.id",
                tags=["multi_join", "tx_both_users"],
                desc="三表 LEFT JOIN — 交易+双方用户",
            ),
            self._seed(
                "SELECT e.name, d.name AS dept, e.salary, d.budget, "
                "ROUND(e.salary * 100.0 / d.budget, 2) AS pct_of_budget "
                "FROM t_employees e "
                "JOIN t_departments d ON e.dept_id = d.id "
                "WHERE d.budget > 0 AND e.salary IS NOT NULL "
                "ORDER BY d.name, pct_of_budget DESC",
                tags=["multi_join", "salary_budget_pct"],
                desc="JOIN + 计算 — 薪资占预算比例",
            ),
            self._seed(
                "SELECT u.username, COUNT(DISTINCT o.id) AS orders, "
                "COUNT(DISTINCT ev.id) AS events "
                "FROM t_users u "
                "LEFT JOIN t_orders o ON u.id = o.user_id "
                "LEFT JOIN t_events ev ON u.id = ev.user_id "
                "GROUP BY u.id, u.username ORDER BY u.id",
                tags=["multi_join", "user_orders_events"],
                desc="多表 LEFT JOIN + 聚合",
            ),
            self._seed(
                "SELECT d.name AS dept, e.name AS employee, e.salary, "
                "m.name AS manager, m.salary AS mgr_salary "
                "FROM t_departments d "
                "JOIN t_employees e ON d.id = e.dept_id "
                "LEFT JOIN t_employees m ON e.manager_id = m.id "
                "WHERE e.salary IS NOT NULL "
                "ORDER BY d.name, e.salary DESC",
                tags=["multi_join", "dept_emp_mgr_salary"],
                desc="四表 JOIN — 部门+员工+经理+薪资",
            ),
            self._seed(
                "SELECT u.username, SUM(t.amount) AS total_sent, "
                "COUNT(t.id) AS tx_count, d.name AS dept "
                "FROM t_users u "
                "LEFT JOIN t_transactions t ON u.id = t.from_user "
                "LEFT JOIN t_employees e ON u.username = e.name "
                "LEFT JOIN t_departments d ON e.dept_id = d.id "
                "GROUP BY u.id, u.username, d.name "
                "ORDER BY total_sent DESC",
                tags=["multi_join", "user_tx_dept"],
                desc="四表 LEFT JOIN + 聚合",
            ),
            self._seed(
                "SELECT e.name, d.name AS dept, e.hire_date, "
                "ev.event_type, ev.event_date "
                "FROM t_employees e "
                "JOIN t_departments d ON e.dept_id = d.id "
                "LEFT JOIN t_events ev ON ev.user_id = "
                "(SELECT id FROM t_users WHERE username = e.name LIMIT 1) "
                "ORDER BY e.id",
                tags=["multi_join", "emp_dept_event"],
                desc="JOIN + 子查询 + LEFT JOIN",
            ),
        ]

    # ── JOIN + 聚合 (~10) ────────────────────────────────
    def _join_aggregate(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT d.name AS dept, COUNT(e.id) AS emp_count, "
                "AVG(e.salary) AS avg_salary, MAX(e.salary) AS max_salary "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name ORDER BY d.name",
                tags=["join_agg", "dept_stats"],
                desc="LEFT JOIN + 聚合 — 部门统计",
            ),
            self._seed(
                "SELECT u.username, COUNT(o.id) AS order_count, "
                "COALESCE(SUM(o.total_amount), 0) AS total_spent "
                "FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id "
                "GROUP BY u.id, u.username ORDER BY total_spent DESC",
                tags=["join_agg", "user_spending"],
                desc="LEFT JOIN + 聚合 — 用户消费",
            ),
            self._seed(
                "SELECT d.name, SUM(e.salary) AS total_salary, "
                "COUNT(e.id) AS headcount "
                "FROM t_departments d JOIN t_employees e ON d.id = e.dept_id "
                "WHERE e.salary > 0 "
                "GROUP BY d.id, d.name "
                "HAVING SUM(e.salary) > 100000 "
                "ORDER BY total_salary DESC",
                tags=["join_agg", "dept_high_salary"],
                desc="JOIN + WHERE + HAVING — 高薪资部门",
            ),
            self._seed(
                "SELECT d.name AS dept, e.status, COUNT(*) AS cnt "
                "FROM t_departments d JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name, e.status ORDER BY d.name, e.status",
                tags=["join_agg", "dept_status"],
                desc="JOIN + GROUP BY 多列",
            ),
            self._seed(
                "SELECT u.username, COUNT(DISTINCT ev.event_type) AS event_types "
                "FROM t_users u LEFT JOIN t_events ev ON u.id = ev.user_id "
                "GROUP BY u.id, u.username ORDER BY event_types DESC",
                tags=["join_agg", "user_event_types"],
                desc="LEFT JOIN + COUNT DISTINCT — 用户事件类型",
            ),
            self._seed(
                "SELECT d.parent_id, COUNT(*) AS child_count "
                "FROM t_departments d "
                "JOIN t_departments p ON d.parent_id = p.id "
                "GROUP BY d.parent_id ORDER BY d.parent_id",
                tags=["join_agg", "dept_children"],
                desc="自连接 + 聚合 — 子部门计数",
            ),
            self._seed(
                "SELECT m.name AS manager, COUNT(e.id) AS team_size, "
                "AVG(e.salary) AS team_avg_salary "
                "FROM t_employees m "
                "INNER JOIN t_employees e ON e.manager_id = m.id "
                "GROUP BY m.id, m.name ORDER BY team_size DESC",
                tags=["join_agg", "manager_team"],
                desc="自连接 + 聚合 — 经理团队统计",
            ),
            self._seed(
                "SELECT u.username, "
                "COUNT(CASE WHEN t.tx_type = 'transfer' THEN 1 END) AS transfers, "
                "COUNT(CASE WHEN t.tx_type = 'payment' THEN 1 END) AS payments, "
                "COALESCE(SUM(t.amount), 0) AS total "
                "FROM t_users u LEFT JOIN t_transactions t ON u.id = t.from_user "
                "GROUP BY u.id, u.username ORDER BY total DESC",
                tags=["join_agg", "user_tx_type_count"],
                desc="LEFT JOIN + CASE COUNT — 用户各类型交易",
            ),
            self._seed(
                "SELECT d.name AS dept, "
                "MIN(e.hire_date) AS earliest_hire, "
                "MAX(e.hire_date) AS latest_hire "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "WHERE e.hire_date IS NOT NULL "
                "GROUP BY d.id, d.name ORDER BY d.name",
                tags=["join_agg", "dept_hire_range"],
                desc="JOIN + MIN/MAX — 部门入职日期范围",
            ),
            self._seed(
                "SELECT d.name AS dept, "
                "COUNT(e.id) AS emp_count, "
                "SUM(CASE WHEN e.salary > 100000 THEN 1 ELSE 0 END) AS high_earners "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name ORDER BY d.name",
                tags=["join_agg", "dept_high_earners"],
                desc="LEFT JOIN + CASE SUM — 部门高薪人数",
            ),
        ]

    # ── JOIN + NULL (~10) ────────────────────────────────
    def _join_null(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT e.id, e.name, d.name AS dept "
                "FROM t_employees e LEFT JOIN t_departments d ON e.dept_id = d.id "
                "WHERE d.name IS NULL ORDER BY e.id",
                tags=["join_null", "no_dept"],
                desc="LEFT JOIN 找 NULL — 无部门员工",
            ),
            self._seed(
                "SELECT u.id, u.username "
                "FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id "
                "WHERE o.id IS NULL ORDER BY u.id",
                tags=["join_null", "no_orders"],
                desc="LEFT JOIN 找 NULL — 无订单用户",
            ),
            self._seed(
                "SELECT e.id, e.name "
                "FROM t_employees e LEFT JOIN t_employees m ON e.manager_id = m.id "
                "WHERE m.id IS NULL AND e.manager_id IS NOT NULL ORDER BY e.id",
                tags=["join_null", "broken_manager"],
                desc="LEFT JOIN — 经理 ID 非空但找不到经理（数据异常）",
            ),
            self._seed(
                "SELECT ev.id, ev.event_type "
                "FROM t_events ev LEFT JOIN t_users u ON ev.user_id = u.id "
                "WHERE u.id IS NULL AND ev.user_id IS NOT NULL ORDER BY ev.id",
                tags=["join_null", "broken_event_user"],
                desc="LEFT JOIN — user_id 非空但无匹配用户",
            ),
            self._seed(
                "SELECT d.id, d.name, COUNT(e.id) AS emp_count "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name "
                "HAVING COUNT(e.id) = 0 ORDER BY d.id",
                tags=["join_null", "empty_depts"],
                desc="LEFT JOIN + HAVING — 无员工部门",
            ),
            self._seed(
                "SELECT e.id, e.name, e.salary, d.name AS dept, "
                "COALESCE(d.name, 'Unassigned') AS dept_display, "
                "COALESCE(e.salary, 0) AS salary_display "
                "FROM t_employees e LEFT JOIN t_departments d ON e.dept_id = d.id "
                "ORDER BY e.id",
                tags=["join_null", "coalesce_null"],
                desc="LEFT JOIN + COALESCE — 处理 NULL",
            ),
            self._seed(
                "SELECT e.name, e.dept_id, "
                "CASE WHEN d.name IS NULL THEN 'no_dept' ELSE d.name END AS dept_status "
                "FROM t_employees e LEFT JOIN t_departments d ON e.dept_id = d.id "
                "ORDER BY e.id",
                tags=["join_null", "case_null_dept"],
                desc="LEFT JOIN + CASE — NULL 部门标记",
            ),
            self._seed(
                "SELECT u.username, "
                "COUNT(o.id) AS order_count, "
                "COALESCE(SUM(CASE WHEN o.status = 'completed' THEN o.total_amount END), 0) AS completed_total "
                "FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id "
                "GROUP BY u.id, u.username ORDER BY u.id",
                tags=["join_null", "user_completed"],
                desc="LEFT JOIN + CASE + COALESCE",
            ),
            self._seed(
                "SELECT t.id, t.amount, "
                "u1.username AS sender, u2.username AS receiver "
                "FROM t_transactions t "
                "LEFT JOIN t_users u1 ON t.from_user = u1.id "
                "LEFT JOIN t_users u2 ON t.to_user = u2.id "
                "WHERE u1.id IS NULL OR u2.id IS NULL "
                "ORDER BY t.id",
                tags=["join_null", "tx_missing_users"],
                desc="双 LEFT JOIN 找 NULL — 缺少发送者或接收者",
            ),
            self._seed(
                "SELECT d.name, d.parent_id, "
                "p.name AS parent_name, "
                "CASE WHEN d.parent_id IS NULL THEN 'root' "
                "WHEN p.name IS NULL THEN 'orphan' "
                "ELSE p.name END AS parent_status "
                "FROM t_departments d LEFT JOIN t_departments p ON d.parent_id = p.id "
                "ORDER BY d.id",
                tags=["join_null", "dept_parent_null"],
                desc="自连接 + CASE — NULL 父部门分类",
            ),
        ]
