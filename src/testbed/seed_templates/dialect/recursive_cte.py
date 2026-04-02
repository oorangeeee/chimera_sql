"""递归 CTE 方言差异模板 — 覆盖 WITH RECURSIVE 关键字、列列表、UNION ALL 差异。

生成 ~50 条种子 SQL，重点测试 Oracle/SQLite 在递归 CTE 处理上的差异：
- SQLite 要求 WITH RECURSIVE 关键字，Oracle 不支持 RECURSIVE（transpiler 自动移除）
- SQLite 支持列列表 WITH RECURSIVE cte(col1, col2) AS (...)，Oracle 也支持但行为有差异
- 递归成员必须使用 UNION ALL（不是 UNION）
- 递归深度限制不同

所有递归 CTE 均使用 WITH RECURSIVE 语法（SQLite 要求），transpiler 为 Oracle 移除 RECURSIVE 关键字。
每条 SQL 均以 ORDER BY 结尾，保证跨数据库结果集可比较。

引用表：
- t_employees (manager_id 自关联)
- t_departments (parent_id 自关联)
- t_users (manager_id 自关联)
"""

from __future__ import annotations

from typing import List

from src.testbed.seed_templates.base import SchemaMetadata, SeedSQL, SeedTemplate
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RecursiveCteTemplate(SeedTemplate):
    """递归 CTE 方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "recursive_cte"

    @property
    def description(self) -> str:
        return "递归 CTE 方言差异种子（WITH RECURSIVE、列列表、UNION ALL）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._employee_hierarchy())
        seeds.extend(self._department_hierarchy())
        seeds.extend(self._user_hierarchy())
        seeds.extend(self._depth_counting())
        seeds.extend(self._path_enumeration())
        seeds.extend(self._aggregation_on_cte())
        logger.info("RecursiveCteTemplate 生成 %d 条种子", len(seeds))
        return seeds

    # ────────────────────────────────────────────────────────
    # 1. Employee hierarchy (~10)
    # ────────────────────────────────────────────────────────
    def _employee_hierarchy(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_tree(id, name, manager_id, dept_id, level) AS ("
                    "SELECT id, name, manager_id, dept_id, 0 AS level "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.manager_id, e.dept_id, t.level + 1 "
                    "FROM t_employees e INNER JOIN emp_tree t ON e.manager_id = t.id"
                    ") "
                    "SELECT id, name, manager_id, dept_id, level "
                    "FROM emp_tree ORDER BY level, id"
                ),
                tags=["employee", "hierarchy", "basic"],
                desc="员工层级 — 完整的经理-下属树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_chain(id, name, manager_id) AS ("
                    "SELECT id, name, manager_id "
                    "FROM t_employees WHERE id = 1 "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.manager_id "
                    "FROM t_employees e INNER JOIN emp_chain c ON e.manager_id = c.id"
                    ") "
                    "SELECT id, name, manager_id "
                    "FROM emp_chain ORDER BY id"
                ),
                tags=["employee", "hierarchy", "root_filter"],
                desc="员工层级 — 从指定根节点展开",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_subtree(id, name, salary, manager_id, depth) AS ("
                    "SELECT id, name, salary, manager_id, 0 AS depth "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.salary, e.manager_id, s.depth + 1 "
                    "FROM t_employees e INNER JOIN emp_subtree s ON e.manager_id = s.id"
                    ") "
                    "SELECT id, name, salary, depth "
                    "FROM emp_subtree "
                    "WHERE salary IS NOT NULL "
                    "ORDER BY depth, salary DESC, id"
                ),
                tags=["employee", "hierarchy", "salary", "filter"],
                desc="员工层级 — 含薪资过滤的下属树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_mgr(id, name, manager_id, mgr_name) AS ("
                    "SELECT e.id, e.name, e.manager_id, m.name AS mgr_name "
                    "FROM t_employees e "
                    "LEFT JOIN t_employees m ON e.manager_id = m.id "
                    "WHERE e.manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.manager_id, c.name AS mgr_name "
                    "FROM t_employees e INNER JOIN emp_mgr c ON e.manager_id = c.id"
                    ") "
                    "SELECT id, name, manager_id, mgr_name "
                    "FROM emp_mgr ORDER BY id"
                ),
                tags=["employee", "hierarchy", "manager_name"],
                desc="员工层级 — 带经理名称的层级树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_dept_tree(id, name, dept_id, level) AS ("
                    "SELECT id, name, dept_id, 0 AS level "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.dept_id, t.level + 1 "
                    "FROM t_employees e INNER JOIN emp_dept_tree t ON e.manager_id = t.id"
                    ") "
                    "SELECT t.id, t.name, t.dept_id, t.level, d.name AS dept_name "
                    "FROM emp_dept_tree t "
                    "LEFT JOIN t_departments d ON t.dept_id = d.id "
                    "ORDER BY t.level, t.id"
                ),
                tags=["employee", "hierarchy", "dept_join"],
                desc="员工层级 — JOIN 部门名称",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_leaf(id, name, manager_id, is_leaf) AS ("
                    "SELECT id, name, manager_id, 1 AS is_leaf "
                    "FROM t_employees "
                    "UNION ALL "
                    "SELECT c.id, c.name, c.manager_id, 0 AS is_leaf "
                    "FROM t_employees c "
                    "INNER JOIN emp_leaf p ON c.manager_id = p.id"
                    ") "
                    "SELECT DISTINCT id, name, manager_id "
                    "FROM emp_leaf "
                    "WHERE id NOT IN (SELECT DISTINCT manager_id FROM t_employees WHERE manager_id IS NOT NULL) "
                    "ORDER BY id"
                ),
                tags=["employee", "hierarchy", "leaf_nodes"],
                desc="员工层级 — 找出叶子节点（无下属）",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_ancestors(id, name, manager_id, level) AS ("
                    "SELECT id, name, manager_id, 0 AS level "
                    "FROM t_employees WHERE manager_id IS NOT NULL "
                    "UNION ALL "
                    "SELECT m.id, m.name, m.manager_id, a.level + 1 "
                    "FROM t_employees m "
                    "INNER JOIN emp_ancestors a ON m.id = a.manager_id"
                    ") "
                    "SELECT id, name, manager_id, level "
                    "FROM emp_ancestors "
                    "WHERE level <= 3 "
                    "ORDER BY level DESC, id"
                ),
                tags=["employee", "hierarchy", "ancestors"],
                desc="员工层级 — 向上追溯祖先链",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_tree(id, name, manager_id, status, level) AS ("
                    "SELECT id, name, manager_id, status, 0 AS level "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.manager_id, e.status, t.level + 1 "
                    "FROM t_employees e INNER JOIN emp_tree t ON e.manager_id = t.id"
                    ") "
                    "SELECT id, name, status, level "
                    "FROM emp_tree "
                    "WHERE status IS NOT NULL "
                    "ORDER BY level, status, id"
                ),
                tags=["employee", "hierarchy", "status"],
                desc="员工层级 — 按状态过滤的层级树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_by_dept(dept_id, id, name, manager_id, level) AS ("
                    "SELECT dept_id, id, name, manager_id, 0 AS level "
                    "FROM t_employees WHERE manager_id IS NULL AND dept_id IS NOT NULL "
                    "UNION ALL "
                    "SELECT e.dept_id, e.id, e.name, e.manager_id, d.level + 1 "
                    "FROM t_employees e INNER JOIN emp_by_dept d ON e.manager_id = d.id"
                    ") "
                    "SELECT dept_id, id, name, level "
                    "FROM emp_by_dept "
                    "ORDER BY dept_id, level, id"
                ),
                tags=["employee", "hierarchy", "by_dept"],
                desc="员工层级 — 按部门分组展示层级",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_tree(id, name, manager_id, level) AS ("
                    "SELECT id, name, manager_id, 0 AS level "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.manager_id, t.level + 1 "
                    "FROM t_employees e INNER JOIN emp_tree t ON e.manager_id = t.id"
                    ") "
                    "SELECT t1.name AS employee, t2.name AS manager, t1.level "
                    "FROM emp_tree t1 "
                    "LEFT JOIN emp_tree t2 ON t1.manager_id = t2.id "
                    "ORDER BY t1.level, t1.id"
                ),
                tags=["employee", "hierarchy", "self_join_cte"],
                desc="员工层级 — CTE 自关联获取经理名称",
            ),
        ]

    # ────────────────────────────────────────────────────────
    # 2. Department hierarchy (~8)
    # ────────────────────────────────────────────────────────
    def _department_hierarchy(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_tree(id, name, parent_id, level) AS ("
                    "SELECT id, name, parent_id, 0 AS level "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, t.level + 1 "
                    "FROM t_departments d INNER JOIN dept_tree t ON d.parent_id = t.id"
                    ") "
                    "SELECT id, name, parent_id, level "
                    "FROM dept_tree ORDER BY level, id"
                ),
                tags=["department", "hierarchy", "basic"],
                desc="部门层级 — 完整的部门树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_tree(id, name, parent_id, budget, level) AS ("
                    "SELECT id, name, parent_id, budget, 0 AS level "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, d.budget, t.level + 1 "
                    "FROM t_departments d INNER JOIN dept_tree t ON d.parent_id = t.id"
                    ") "
                    "SELECT id, name, parent_id, budget, level "
                    "FROM dept_tree "
                    "WHERE budget IS NOT NULL "
                    "ORDER BY level, budget DESC, id"
                ),
                tags=["department", "hierarchy", "budget"],
                desc="部门层级 — 含预算的部门树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_subtree(id, name, parent_id, level) AS ("
                    "SELECT id, name, parent_id, 0 AS level "
                    "FROM t_departments WHERE id = 1 "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, s.level + 1 "
                    "FROM t_departments d INNER JOIN dept_subtree s ON d.parent_id = s.id"
                    ") "
                    "SELECT id, name, parent_id, level "
                    "FROM dept_subtree ORDER BY level, id"
                ),
                tags=["department", "hierarchy", "root_filter"],
                desc="部门层级 — 从指定根部门展开子树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_children(parent_id, child_count) AS ("
                    "SELECT parent_id, COUNT(*) AS child_count "
                    "FROM t_departments WHERE parent_id IS NOT NULL "
                    "GROUP BY parent_id "
                    "UNION ALL "
                    "SELECT d.parent_id, c.child_count "
                    "FROM t_departments d "
                    "INNER JOIN dept_children c ON d.id = c.parent_id "
                    "WHERE d.parent_id IS NOT NULL"
                    ") "
                    "SELECT parent_id, SUM(child_count) AS total_children "
                    "FROM dept_children "
                    "GROUP BY parent_id "
                    "ORDER BY parent_id"
                ),
                tags=["department", "hierarchy", "child_count"],
                desc="部门层级 — 统计各部门子部门数",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_tree(id, name, parent_id, location, level) AS ("
                    "SELECT id, name, parent_id, location, 0 AS level "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, d.location, t.level + 1 "
                    "FROM t_departments d INNER JOIN dept_tree t ON d.parent_id = t.id"
                    ") "
                    "SELECT id, name, parent_id, location, level "
                    "FROM dept_tree "
                    "WHERE location IS NOT NULL "
                    "ORDER BY location, level, id"
                ),
                tags=["department", "hierarchy", "location"],
                desc="部门层级 — 按地点过滤的部门树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_leaves(id, name, parent_id) AS ("
                    "SELECT id, name, parent_id "
                    "FROM t_departments "
                    "WHERE id NOT IN (SELECT DISTINCT parent_id FROM t_departments WHERE parent_id IS NOT NULL) "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id "
                    "FROM t_departments d "
                    "INNER JOIN dept_leaves l ON d.id = l.parent_id"
                    ") "
                    "SELECT DISTINCT id, name, parent_id "
                    "FROM dept_leaves "
                    "ORDER BY id"
                ),
                tags=["department", "hierarchy", "leaf_nodes"],
                desc="部门层级 — 叶子部门追溯祖先",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_tree(id, name, parent_id, level) AS ("
                    "SELECT id, name, parent_id, 0 AS level "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, t.level + 1 "
                    "FROM t_departments d INNER JOIN dept_tree t ON d.parent_id = t.id"
                    ") "
                    "SELECT t.id, t.name, t.parent_id, t.level, "
                    "(SELECT COUNT(*) FROM t_employees e WHERE e.dept_id = t.id) AS emp_count "
                    "FROM dept_tree t "
                    "ORDER BY t.level, t.id"
                ),
                tags=["department", "hierarchy", "emp_count"],
                desc="部门层级 — 含员工数量的部门树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_root_only(id, name, parent_id) AS ("
                    "SELECT id, name, parent_id "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id "
                    "FROM t_departments d INNER JOIN dept_root_only r ON d.parent_id = r.id "
                    "WHERE d.parent_id IS NOT NULL"
                    ") "
                    "SELECT r.id, r.name, r.parent_id, "
                    "(SELECT name FROM t_departments WHERE id = r.parent_id) AS parent_name "
                    "FROM dept_root_only r "
                    "ORDER BY r.id"
                ),
                tags=["department", "hierarchy", "parent_name"],
                desc="部门层级 — 带父部门名称",
            ),
        ]

    # ────────────────────────────────────────────────────────
    # 3. User hierarchy (~8)
    # ────────────────────────────────────────────────────────
    def _user_hierarchy(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "WITH RECURSIVE user_tree(id, username, manager_id, level) AS ("
                    "SELECT id, username, manager_id, 0 AS level "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username, u.manager_id, t.level + 1 "
                    "FROM t_users u INNER JOIN user_tree t ON u.manager_id = t.id"
                    ") "
                    "SELECT id, username, manager_id, level "
                    "FROM user_tree ORDER BY level, id"
                ),
                tags=["user", "hierarchy", "basic"],
                desc="用户层级 — 完整的经理-下属树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_hierarchy(id, username, manager_id, lvl) AS ("
                    "SELECT id, username, manager_id, 0 AS lvl "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.username, e.manager_id, h.lvl + 1 "
                    "FROM t_users e INNER JOIN user_hierarchy h ON e.manager_id = h.id"
                    ") "
                    "SELECT id, username, manager_id, lvl "
                    "FROM user_hierarchy ORDER BY lvl, id"
                ),
                tags=["user", "hierarchy", "basic"],
                desc="用户层级 — 完整层级（lvl 别名）",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_tree(id, username, manager_id, score, level) AS ("
                    "SELECT id, username, manager_id, score, 0 AS level "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username, u.manager_id, u.score, t.level + 1 "
                    "FROM t_users u INNER JOIN user_tree t ON u.manager_id = t.id"
                    ") "
                    "SELECT id, username, score, level "
                    "FROM user_tree "
                    "WHERE score IS NOT NULL "
                    "ORDER BY level, score DESC, id"
                ),
                tags=["user", "hierarchy", "score"],
                desc="用户层级 — 含 score 的层级树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_chain(id, username, manager_id) AS ("
                    "SELECT id, username, manager_id "
                    "FROM t_users WHERE id = 1 "
                    "UNION ALL "
                    "SELECT u.id, u.username, u.manager_id "
                    "FROM t_users u INNER JOIN user_chain c ON u.manager_id = c.id"
                    ") "
                    "SELECT c.id, c.username, c.manager_id, "
                    "(SELECT username FROM t_users WHERE id = c.manager_id) AS mgr_name "
                    "FROM user_chain c "
                    "ORDER BY c.id"
                ),
                tags=["user", "hierarchy", "root_filter"],
                desc="用户层级 — 从指定根节点展开",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_tree(id, username, manager_id, level) AS ("
                    "SELECT id, username, manager_id, 0 AS level "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username, u.manager_id, t.level + 1 "
                    "FROM t_users u INNER JOIN user_tree t ON u.manager_id = t.id"
                    ") "
                    "SELECT t.id, t.username, t.manager_id, t.level, "
                    "(SELECT COUNT(*) FROM t_orders o WHERE o.user_id = t.id) AS order_count "
                    "FROM user_tree t "
                    "ORDER BY t.level, t.id"
                ),
                tags=["user", "hierarchy", "order_count"],
                desc="用户层级 — 含订单数量的层级树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_tree(id, username, manager_id, age, level) AS ("
                    "SELECT id, username, manager_id, age, 0 AS level "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username, u.manager_id, u.age, t.level + 1 "
                    "FROM t_users u INNER JOIN user_tree t ON u.manager_id = t.id"
                    ") "
                    "SELECT id, username, age, level "
                    "FROM user_tree "
                    "WHERE age IS NOT NULL "
                    "ORDER BY level, age, id"
                ),
                tags=["user", "hierarchy", "age"],
                desc="用户层级 — 含年龄的层级树",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_roots(id, username) AS ("
                    "SELECT id, username "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username "
                    "FROM t_users u INNER JOIN user_roots r ON u.manager_id = r.id"
                    ") "
                    "SELECT r.id, r.username, "
                    "(SELECT COUNT(*) FROM t_users sub WHERE sub.manager_id = r.id) AS direct_reports "
                    "FROM user_roots r "
                    "ORDER BY r.id"
                ),
                tags=["user", "hierarchy", "direct_reports"],
                desc="用户层级 — 每个节点的直属下属数",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_tree(id, username, manager_id, active, level) AS ("
                    "SELECT id, username, manager_id, active, 0 AS level "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username, u.manager_id, u.active, t.level + 1 "
                    "FROM t_users u INNER JOIN user_tree t ON u.manager_id = t.id"
                    ") "
                    "SELECT id, username, active, level "
                    "FROM user_tree "
                    "WHERE active = 1 "
                    "ORDER BY level, id"
                ),
                tags=["user", "hierarchy", "active_filter"],
                desc="用户层级 — 仅活跃用户的层级树",
            ),
        ]

    # ────────────────────────────────────────────────────────
    # 4. Depth counting (~8)
    # ────────────────────────────────────────────────────────
    def _depth_counting(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "WITH RECURSIVE depth_cte(id, username, lvl) AS ("
                    "SELECT id, username, 0 AS lvl "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.username, d.lvl + 1 "
                    "FROM t_users e INNER JOIN depth_cte d ON e.manager_id = d.id"
                    ") "
                    "SELECT lvl, COUNT(*) AS cnt "
                    "FROM depth_cte "
                    "GROUP BY lvl "
                    "ORDER BY lvl"
                ),
                tags=["depth", "count", "user"],
                desc="深度计数 — 每层级的用户数",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE depth_cte(id, name, parent_id, lvl) AS ("
                    "SELECT id, name, parent_id, 0 AS lvl "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, c.lvl + 1 "
                    "FROM t_departments d INNER JOIN depth_cte c ON d.parent_id = c.id"
                    ") "
                    "SELECT lvl, COUNT(*) AS dept_count "
                    "FROM depth_cte "
                    "GROUP BY lvl "
                    "ORDER BY lvl"
                ),
                tags=["depth", "count", "department"],
                desc="深度计数 — 每层级的部门数",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_depth(id, name, manager_id, lvl) AS ("
                    "SELECT id, name, manager_id, 0 AS lvl "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.manager_id, d.lvl + 1 "
                    "FROM t_employees e INNER JOIN emp_depth d ON e.manager_id = d.id"
                    ") "
                    "SELECT lvl, COUNT(*) AS emp_count "
                    "FROM emp_depth "
                    "GROUP BY lvl "
                    "ORDER BY lvl"
                ),
                tags=["depth", "count", "employee"],
                desc="深度计数 — 每层级的员工数",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE max_depth_cte(id, username, lvl) AS ("
                    "SELECT id, username, 0 AS lvl "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username, m.lvl + 1 "
                    "FROM t_users u INNER JOIN max_depth_cte m ON u.manager_id = m.id"
                    ") "
                    "SELECT MAX(lvl) AS max_depth "
                    "FROM max_depth_cte ORDER BY max_depth"
                ),
                tags=["depth", "max", "user"],
                desc="深度计数 — 用户树最大深度",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_depth(id, name, parent_id, lvl) AS ("
                    "SELECT id, name, parent_id, 0 AS lvl "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, c.lvl + 1 "
                    "FROM t_departments d INNER JOIN dept_depth c ON d.parent_id = c.id"
                    ") "
                    "SELECT MAX(lvl) AS max_depth "
                    "FROM dept_depth ORDER BY max_depth"
                ),
                tags=["depth", "max", "department"],
                desc="深度计数 — 部门树最大深度",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_lvl(id, name, salary, lvl) AS ("
                    "SELECT id, name, salary, 0 AS lvl "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.salary, l.lvl + 1 "
                    "FROM t_employees e INNER JOIN emp_lvl l ON e.manager_id = l.id"
                    ") "
                    "SELECT lvl, AVG(salary) AS avg_salary "
                    "FROM emp_lvl "
                    "WHERE salary IS NOT NULL "
                    "GROUP BY lvl "
                    "ORDER BY lvl"
                ),
                tags=["depth", "avg_salary", "employee"],
                desc="深度计数 — 每层级平均薪资",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_depth(id, username, score, lvl) AS ("
                    "SELECT id, username, score, 0 AS lvl "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username, u.score, d.lvl + 1 "
                    "FROM t_users u INNER JOIN user_depth d ON u.manager_id = d.id"
                    ") "
                    "SELECT lvl, COUNT(*) AS cnt, AVG(score) AS avg_score "
                    "FROM user_depth "
                    "GROUP BY lvl "
                    "ORDER BY lvl"
                ),
                tags=["depth", "count", "avg", "user"],
                desc="深度计数 — 每层级人数和平均分",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_depth(id, name, dept_id, lvl) AS ("
                    "SELECT id, name, dept_id, 0 AS lvl "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.dept_id, d.lvl + 1 "
                    "FROM t_employees e INNER JOIN emp_depth d ON e.manager_id = d.id"
                    ") "
                    "SELECT lvl, dept_id, COUNT(*) AS cnt "
                    "FROM emp_depth "
                    "WHERE dept_id IS NOT NULL "
                    "GROUP BY lvl, dept_id "
                    "ORDER BY lvl, dept_id"
                ),
                tags=["depth", "count", "employee", "dept_group"],
                desc="深度计数 — 按层级和部门统计员工数",
            ),
        ]

    # ────────────────────────────────────────────────────────
    # 5. Path enumeration (~8)
    # ────────────────────────────────────────────────────────
    def _path_enumeration(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_path(id, name, manager_id, path) AS ("
                    "SELECT id, name, manager_id, CAST(name AS VARCHAR(500)) AS path "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.manager_id, "
                    "CAST(p.path || ' > ' || e.name AS VARCHAR(500)) "
                    "FROM t_employees e INNER JOIN emp_path p ON e.manager_id = p.id"
                    ") "
                    "SELECT id, name, manager_id, path "
                    "FROM emp_path ORDER BY path"
                ),
                tags=["path", "employee", "basic"],
                desc="路径枚举 — 员工层级路径 'Manager > Employee'",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_path(id, name, parent_id, path) AS ("
                    "SELECT id, name, parent_id, CAST(name AS VARCHAR(500)) AS path "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, "
                    "CAST(p.path || ' > ' || d.name AS VARCHAR(500)) "
                    "FROM t_departments d INNER JOIN dept_path p ON d.parent_id = p.id"
                    ") "
                    "SELECT id, name, parent_id, path "
                    "FROM dept_path ORDER BY path"
                ),
                tags=["path", "department", "basic"],
                desc="路径枚举 — 部门层级路径 'Root > Child > Leaf'",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_path(id, username, manager_id, path) AS ("
                    "SELECT id, username, manager_id, CAST(username AS VARCHAR(500)) AS path "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username, u.manager_id, "
                    "CAST(p.path || ' > ' || u.username AS VARCHAR(500)) "
                    "FROM t_users u INNER JOIN user_path p ON u.manager_id = p.id"
                    ") "
                    "SELECT id, username, manager_id, path "
                    "FROM user_path ORDER BY path"
                ),
                tags=["path", "user", "basic"],
                desc="路径枚举 — 用户层级路径",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_id_path(id, name, manager_id, id_path) AS ("
                    "SELECT id, name, manager_id, CAST(id AS VARCHAR(500)) AS id_path "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.manager_id, "
                    "CAST(p.id_path || ' > ' || CAST(e.id AS VARCHAR(50)) AS VARCHAR(500)) "
                    "FROM t_employees e INNER JOIN emp_id_path p ON e.manager_id = p.id"
                    ") "
                    "SELECT id, name, manager_id, id_path "
                    "FROM emp_id_path ORDER BY id_path"
                ),
                tags=["path", "employee", "id_path"],
                desc="路径枚举 — 员工 ID 路径 '1 > 2 > 3'",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_path(id, name, parent_id, path, level) AS ("
                    "SELECT id, name, parent_id, CAST(name AS VARCHAR(500)) AS path, 0 AS level "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, "
                    "CAST(p.path || ' > ' || d.name AS VARCHAR(500)), p.level + 1 "
                    "FROM t_departments d INNER JOIN dept_path p ON d.parent_id = p.id"
                    ") "
                    "SELECT id, name, parent_id, path, level "
                    "FROM dept_path ORDER BY level, path"
                ),
                tags=["path", "department", "level"],
                desc="路径枚举 — 部门路径含层级深度",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_path(id, username, manager_id, path, depth) AS ("
                    "SELECT id, username, manager_id, CAST(username AS VARCHAR(500)) AS path, 0 AS depth "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username, u.manager_id, "
                    "CAST(p.path || ' > ' || u.username AS VARCHAR(500)), p.depth + 1 "
                    "FROM t_users u INNER JOIN user_path p ON u.manager_id = p.id"
                    ") "
                    "SELECT id, username, path, depth "
                    "FROM user_path "
                    "WHERE depth <= 3 "
                    "ORDER BY depth, path"
                ),
                tags=["path", "user", "depth_limit"],
                desc="路径枚举 — 用户路径（限制深度 <= 3）",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_path(id, name, manager_id, path) AS ("
                    "SELECT id, name, manager_id, CAST(name AS VARCHAR(500)) AS path "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.manager_id, "
                    "CAST(p.path || ' > ' || e.name AS VARCHAR(500)) "
                    "FROM t_employees e INNER JOIN emp_path p ON e.manager_id = p.id"
                    ") "
                    "SELECT id, name, path, LENGTH(path) AS path_len "
                    "FROM emp_path "
                    "ORDER BY LENGTH(path), path"
                ),
                tags=["path", "employee", "length"],
                desc="路径枚举 — 员工路径含字符串长度",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_id_path(id, name, parent_id, id_path) AS ("
                    "SELECT id, name, parent_id, CAST(id AS VARCHAR(500)) AS id_path "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, "
                    "CAST(p.id_path || ' > ' || CAST(d.id AS VARCHAR(50)) AS VARCHAR(500)) "
                    "FROM t_departments d INNER JOIN dept_id_path p ON d.parent_id = p.id"
                    ") "
                    "SELECT id, name, parent_id, id_path "
                    "FROM dept_id_path ORDER BY id_path"
                ),
                tags=["path", "department", "id_path"],
                desc="路径枚举 — 部门 ID 路径",
            ),
        ]

    # ────────────────────────────────────────────────────────
    # 6. Aggregation on CTE (~8)
    # ────────────────────────────────────────────────────────
    def _aggregation_on_cte(self) -> List[SeedSQL]:
        return [
            self._seed(
                sql=(
                    "WITH RECURSIVE sub_tree(id, root_id) AS ("
                    "SELECT id, id AS root_id "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, s.root_id "
                    "FROM t_users e INNER JOIN sub_tree s ON e.manager_id = s.id"
                    ") "
                    "SELECT root_id, COUNT(*) - 1 AS subordinate_count "
                    "FROM sub_tree GROUP BY root_id "
                    "ORDER BY root_id"
                ),
                tags=["aggregation", "count", "user", "subordinate"],
                desc="CTE 聚合 — 每个根用户的下属总数",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_tree(id, name, salary, manager_id, root_id) AS ("
                    "SELECT id, name, salary, manager_id, id AS root_id "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.salary, e.manager_id, t.root_id "
                    "FROM t_employees e INNER JOIN emp_tree t ON e.manager_id = t.id"
                    ") "
                    "SELECT root_id, SUM(salary) AS total_salary "
                    "FROM emp_tree "
                    "WHERE salary IS NOT NULL "
                    "GROUP BY root_id "
                    "ORDER BY root_id"
                ),
                tags=["aggregation", "sum", "employee", "salary"],
                desc="CTE 聚合 — 每棵子树的薪资总和",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_tree(id, name, parent_id, budget, root_id) AS ("
                    "SELECT id, name, parent_id, budget, id AS root_id "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, d.budget, t.root_id "
                    "FROM t_departments d INNER JOIN dept_tree t ON d.parent_id = t.id"
                    ") "
                    "SELECT root_id, COUNT(*) AS subtree_size, SUM(budget) AS total_budget "
                    "FROM dept_tree "
                    "GROUP BY root_id "
                    "ORDER BY root_id"
                ),
                tags=["aggregation", "count", "sum", "department", "budget"],
                desc="CTE 聚合 — 每棵部门子树的大小和预算总和",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_tree(id, username, score, manager_id, root_id) AS ("
                    "SELECT id, username, score, manager_id, id AS root_id "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username, u.score, u.manager_id, t.root_id "
                    "FROM t_users u INNER JOIN user_tree t ON u.manager_id = t.id"
                    ") "
                    "SELECT root_id, AVG(score) AS avg_score "
                    "FROM user_tree "
                    "WHERE score IS NOT NULL "
                    "GROUP BY root_id "
                    "ORDER BY avg_score DESC, root_id"
                ),
                tags=["aggregation", "avg", "user", "score"],
                desc="CTE 聚合 — 每棵子树的平均分",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_tree(id, name, dept_id, manager_id, root_id) AS ("
                    "SELECT id, name, dept_id, manager_id, id AS root_id "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.dept_id, e.manager_id, t.root_id "
                    "FROM t_employees e INNER JOIN emp_tree t ON e.manager_id = t.id"
                    ") "
                    "SELECT root_id, dept_id, COUNT(*) AS cnt "
                    "FROM emp_tree "
                    "WHERE dept_id IS NOT NULL "
                    "GROUP BY root_id, dept_id "
                    "ORDER BY root_id, dept_id"
                ),
                tags=["aggregation", "count", "employee", "dept"],
                desc="CTE 聚合 — 每棵子树按部门统计人数",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE user_tree(id, username, manager_id, root_id) AS ("
                    "SELECT id, username, manager_id, id AS root_id "
                    "FROM t_users WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT u.id, u.username, u.manager_id, t.root_id "
                    "FROM t_users u INNER JOIN user_tree t ON u.manager_id = t.id"
                    ") "
                    "SELECT root_id, "
                    "(SELECT username FROM t_users WHERE id = r.root_id) AS root_name, "
                    "COUNT(*) AS tree_size "
                    "FROM user_tree r "
                    "GROUP BY root_id "
                    "ORDER BY tree_size DESC, root_id"
                ),
                tags=["aggregation", "count", "user", "root_name"],
                desc="CTE 聚合 — 每棵子树大小（含根名称）",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE dept_subtree(id, name, parent_id, root_id) AS ("
                    "SELECT id, name, parent_id, id AS root_id "
                    "FROM t_departments WHERE parent_id IS NULL "
                    "UNION ALL "
                    "SELECT d.id, d.name, d.parent_id, s.root_id "
                    "FROM t_departments d INNER JOIN dept_subtree s ON d.parent_id = s.id"
                    ") "
                    "SELECT s.root_id, (SELECT name FROM t_departments WHERE id = s.root_id) AS root_name, "
                    "COUNT(*) AS total_depts "
                    "FROM dept_subtree s "
                    "GROUP BY s.root_id "
                    "HAVING COUNT(*) > 1 "
                    "ORDER BY total_depts DESC, s.root_id"
                ),
                tags=["aggregation", "count", "having", "department"],
                desc="CTE 聚合 — 子部门数 > 1 的根部门",
            ),
            self._seed(
                sql=(
                    "WITH RECURSIVE emp_tree(id, name, salary, manager_id, level, root_id) AS ("
                    "SELECT id, name, salary, manager_id, 0 AS level, id AS root_id "
                    "FROM t_employees WHERE manager_id IS NULL "
                    "UNION ALL "
                    "SELECT e.id, e.name, e.salary, e.manager_id, t.level + 1, t.root_id "
                    "FROM t_employees e INNER JOIN emp_tree t ON e.manager_id = t.id"
                    ") "
                    "SELECT level, COUNT(*) AS emp_count, AVG(salary) AS avg_salary, MAX(salary) AS max_salary "
                    "FROM emp_tree "
                    "WHERE salary IS NOT NULL "
                    "GROUP BY level "
                    "ORDER BY level"
                ),
                tags=["aggregation", "count", "avg", "max", "employee", "level"],
                desc="CTE 聚合 — 按层级统计员工数、均薪、最高薪",
            ),
        ]
