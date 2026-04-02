"""集合操作方言差异模板 — 测试 SQLite/Oracle 集合操作语法差异。

覆盖差异点：
- EXCEPT (SQLite) ↔ MINUS (Oracle)
- INTERSECT 语法一致但行为可能不同
- UNION / UNION ALL 列数类型匹配
- ORDER BY 位置差异
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class SetOperationsTemplate(SeedTemplate):
    """集合操作方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "set_operations"

    @property
    def description(self) -> str:
        return "集合操作方言差异测试（EXCEPT/MINUS/INTERSECT/UNION）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._except_queries())
        seeds.extend(self._intersect_queries())
        seeds.extend(self._union_queries())
        seeds.extend(self._compound_queries())
        return seeds

    # ── EXCEPT (~10) ─────────────────────────────────────
    def _except_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users WHERE score IS NOT NULL "
                "EXCEPT SELECT id, username FROM t_users WHERE age IS NULL "
                "ORDER BY id",
                tags=["except", "except_score_not_null"],
                desc="EXCEPT — 有 score 但 age 不为 NULL 的用户",
            ),
            self._seed(
                "SELECT id, name FROM t_products WHERE category IS NOT NULL "
                "EXCEPT SELECT id, name FROM t_products WHERE price < 50 "
                "ORDER BY id",
                tags=["except", "except_cat_no_cheap"],
                desc="EXCEPT — 有分类且不便宜的产品",
            ),
            self._seed(
                "SELECT id, name FROM t_employees WHERE dept_id IS NOT NULL "
                "EXCEPT SELECT id, name FROM t_employees WHERE salary < 60000 "
                "ORDER BY id",
                tags=["except", "except_dept_high_salary"],
                desc="EXCEPT — 有部门且薪资不低于 60000",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "EXCEPT SELECT id, username FROM t_users WHERE email IS NOT NULL "
                "ORDER BY id",
                tags=["except", "except_no_email"],
                desc="EXCEPT — 没有 email 的用户",
            ),
            self._seed(
                "SELECT id, event_type FROM t_events WHERE user_id IS NOT NULL "
                "EXCEPT SELECT id, event_type FROM t_events WHERE event_date IS NULL "
                "ORDER BY id",
                tags=["except", "except_events_with_date"],
                desc="EXCEPT — 有用户且有日期的事件",
            ),
            self._seed(
                "SELECT id, status FROM t_orders WHERE status = 'completed' "
                "EXCEPT SELECT id, status FROM t_orders WHERE user_id IS NULL "
                "ORDER BY id",
                tags=["except", "except_completed_known_user"],
                desc="EXCEPT — 已完成且有用户的订单",
            ),
            self._seed(
                "SELECT dept_id FROM t_employees WHERE dept_id IS NOT NULL "
                "EXCEPT SELECT id FROM t_departments WHERE budget IS NULL "
                "ORDER BY dept_id",
                tags=["except", "except_dept_with_budget"],
                desc="EXCEPT — 员工部门中有预算的",
            ),
            self._seed(
                "SELECT from_user FROM t_transactions WHERE from_user IS NOT NULL "
                "EXCEPT SELECT to_user FROM t_transactions WHERE to_user IS NOT NULL "
                "ORDER BY from_user",
                tags=["except", "except_senders_not_receivers"],
                desc="EXCEPT — 是发送者但不是接收者",
            ),
            self._seed(
                "SELECT id, name FROM t_products WHERE stock > 0 "
                "EXCEPT SELECT id, name FROM t_products WHERE category IS NULL "
                "ORDER BY id",
                tags=["except", "except_in_stock_categorized"],
                desc="EXCEPT — 有库存且有分类",
            ),
            self._seed(
                "SELECT id, username FROM t_users WHERE age IS NOT NULL "
                "EXCEPT SELECT id, username FROM t_users WHERE score IS NULL "
                "ORDER BY id",
                tags=["except", "except_age_with_score"],
                desc="EXCEPT — 有年龄且有 score",
            ),
        ]

    # ── INTERSECT (~8) ────────────────────────────────────
    def _intersect_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users WHERE score IS NOT NULL "
                "INTERSECT SELECT id, username FROM t_users WHERE email IS NOT NULL "
                "ORDER BY id",
                tags=["intersect", "intersect_score_email"],
                desc="INTERSECT — 同时有 score 和 email 的用户",
            ),
            self._seed(
                "SELECT id, name FROM t_products WHERE price > 100 "
                "INTERSECT SELECT id, name FROM t_products WHERE stock > 0 "
                "ORDER BY id",
                tags=["intersect", "intersect_expensive_instock"],
                desc="INTERSECT — 昂贵且有库存的产品",
            ),
            self._seed(
                "SELECT id, name FROM t_employees WHERE salary > 80000 "
                "INTERSECT SELECT id, name FROM t_employees WHERE dept_id IS NOT NULL "
                "ORDER BY id",
                tags=["intersect", "intersect_high_salary_dept"],
                desc="INTERSECT — 高薪且有部门的员工",
            ),
            self._seed(
                "SELECT user_id FROM t_orders WHERE status = 'completed' "
                "INTERSECT SELECT user_id FROM t_orders WHERE status = 'pending' "
                "ORDER BY user_id",
                tags=["intersect", "intersect_completed_pending"],
                desc="INTERSECT — 同时有完成和待处理订单的用户",
            ),
            self._seed(
                "SELECT id FROM t_users WHERE age > 25 "
                "INTERSECT SELECT id FROM t_users WHERE score > 80 "
                "ORDER BY id",
                tags=["intersect", "intersect_age_score"],
                desc="INTERSECT — 年龄>25 且 score>80",
            ),
            self._seed(
                "SELECT id FROM t_events WHERE event_type = 'login' "
                "INTERSECT SELECT id FROM t_events WHERE user_id IS NOT NULL "
                "ORDER BY id",
                tags=["intersect", "intersect_login_known_user"],
                desc="INTERSECT — login 事件且有 user_id",
            ),
            self._seed(
                "SELECT from_user FROM t_transactions WHERE tx_type = 'transfer' "
                "INTERSECT SELECT to_user FROM t_transactions WHERE tx_type = 'payment' "
                "ORDER BY from_user",
                tags=["intersect", "intersect_transfer_payer"],
                desc="INTERSECT — 转账发起者且是支付接收者",
            ),
            self._seed(
                "SELECT id FROM t_departments WHERE budget IS NOT NULL "
                "INTERSECT SELECT id FROM t_departments WHERE location IS NOT NULL "
                "ORDER BY id",
                tags=["intersect", "intersect_dept_budget_loc"],
                desc="INTERSECT — 有预算且有位置的部门",
            ),
        ]

    # ── UNION / UNION ALL (~12) ───────────────────────────
    def _union_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username AS name, 'user' AS source FROM t_users "
                "UNION ALL SELECT id, name, 'employee' AS source FROM t_employees "
                "ORDER BY id",
                tags=["union_all", "union_users_employees"],
                desc="UNION ALL — 用户和员工合并",
            ),
            self._seed(
                "SELECT id, tag AS label, 'tag' AS source FROM t_tags "
                "UNION ALL SELECT id, event_type AS label, 'event' AS source FROM t_events "
                "ORDER BY id",
                tags=["union_all", "union_tags_events"],
                desc="UNION ALL — 标签和事件类型合并",
            ),
            self._seed(
                "SELECT id, name, 'product' AS type FROM t_products WHERE category IS NULL "
                "UNION ALL SELECT id, name, 'dept' AS type FROM t_departments WHERE budget IS NULL "
                "ORDER BY id",
                tags=["union_all", "union_null_cat_budget"],
                desc="UNION ALL — NULL 分类产品和 NULL 预算部门",
            ),
            self._seed(
                "SELECT id, username AS name FROM t_users WHERE score IS NULL "
                "UNION SELECT id, name FROM t_employees WHERE salary IS NULL "
                "ORDER BY id",
                tags=["union", "union_distinct_null"],
                desc="UNION — score 或 salary 为 NULL 的人（去重）",
            ),
            self._seed(
                "SELECT category FROM t_products WHERE category IS NOT NULL "
                "UNION SELECT status FROM t_orders WHERE status IS NOT NULL "
                "ORDER BY category",
                tags=["union", "union_categories_statuses"],
                desc="UNION — 产品分类和订单状态（去重）",
            ),
            self._seed(
                "SELECT id, amount AS val, 'transaction' AS src FROM t_transactions WHERE amount > 0 "
                "UNION ALL SELECT id, salary AS val, 'salary' AS src FROM t_employees WHERE salary > 0 "
                "ORDER BY id",
                tags=["union_all", "union_positive_amounts"],
                desc="UNION ALL — 正金额和正薪资",
            ),
            self._seed(
                "SELECT id, username AS name, score AS val FROM t_users WHERE score > 90 "
                "UNION ALL SELECT id, name, salary / 1000 AS val FROM t_employees WHERE salary > 100000 "
                "ORDER BY id",
                tags=["union_all", "union_top_scores_salaries"],
                desc="UNION ALL — 高分用户和高薪员工",
            ),
            self._seed(
                "SELECT 'active' AS status, COUNT(*) AS cnt FROM t_employees WHERE status = 'active' "
                "UNION ALL SELECT 'inactive' AS status, COUNT(*) AS cnt FROM t_employees WHERE status = 'inactive' "
                "UNION ALL SELECT 'other' AS status, COUNT(*) AS cnt FROM t_employees "
                "WHERE status NOT IN ('active', 'inactive') OR status IS NULL "
                "ORDER BY status",
                tags=["union_all", "union_status_counts"],
                desc="UNION ALL — 多状态计数",
            ),
            self._seed(
                "SELECT id, name, 'product' AS entity FROM t_products WHERE price < 50 "
                "UNION SELECT id, username AS name, 'user' AS entity FROM t_users WHERE age < 25 "
                "ORDER BY id",
                tags=["union", "union_cheap_young"],
                desc="UNION — 便宜产品和年轻用户（去重）",
            ),
            self._seed(
                "SELECT DISTINCT event_type FROM t_events "
                "UNION SELECT DISTINCT tx_type FROM t_transactions WHERE tx_type IS NOT NULL "
                "ORDER BY event_type",
                tags=["union", "union_event_tx_types"],
                desc="UNION — 所有事件和交易类型",
            ),
            self._seed(
                "SELECT id, 'order' AS src, user_id AS ref_id FROM t_orders "
                "UNION ALL SELECT id, 'event' AS src, user_id AS ref_id FROM t_events WHERE user_id IS NOT NULL "
                "ORDER BY ref_id, id",
                tags=["union_all", "union_orders_events"],
                desc="UNION ALL — 订单和事件按用户 ID 合并",
            ),
            self._seed(
                "SELECT d.name AS dept_name, COUNT(e.id) AS emp_count "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name "
                "UNION ALL SELECT 'TOTAL' AS dept_name, COUNT(*) AS emp_count FROM t_employees "
                "ORDER BY dept_name",
                tags=["union_all", "union_dept_summary_total"],
                desc="UNION ALL — 部门统计 + 总计行",
            ),
        ]

    # ── 复合集合操作 (~10) ────────────────────────────────
    def _compound_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "(SELECT id, username FROM t_users WHERE score > 80) "
                "EXCEPT (SELECT id, username FROM t_users WHERE age < 20) "
                "ORDER BY id",
                tags=["compound", "compound_except"],
                desc="括号 EXCEPT — 高分但非年轻用户",
            ),
            self._seed(
                "(SELECT id, name FROM t_products WHERE price > 100) "
                "INTERSECT (SELECT id, name FROM t_products WHERE stock IS NOT NULL) "
                "EXCEPT (SELECT id, name FROM t_products WHERE category IS NULL) "
                "ORDER BY id",
                tags=["compound", "compound_three_ops"],
                desc="三重集合操作 — 昂贵且有库存且有分类",
            ),
            self._seed(
                "SELECT id, username FROM t_users WHERE email IS NOT NULL "
                "UNION ALL SELECT id, name FROM t_employees WHERE bio IS NOT NULL "
                "EXCEPT SELECT id, username FROM t_users WHERE score IS NULL "
                "ORDER BY id",
                tags=["compound", "compound_union_except"],
                desc="UNION ALL + EXCEPT 组合",
            ),
            self._seed(
                "SELECT id FROM t_users WHERE score IS NOT NULL "
                "INTERSECT SELECT id FROM t_users WHERE email IS NOT NULL "
                "UNION SELECT id FROM t_users WHERE age IS NOT NULL "
                "ORDER BY id",
                tags=["compound", "compound_intersect_union"],
                desc="INTERSECT + UNION 组合",
            ),
            self._seed(
                "(SELECT user_id FROM t_orders) "
                "EXCEPT (SELECT id FROM t_users WHERE score IS NULL) "
                "ORDER BY user_id",
                tags=["compound", "compound_orders_active_users"],
                desc="EXCEPT — 下过订单且 score 不为 NULL 的用户",
            ),
            self._seed(
                "SELECT id, event_type FROM t_events WHERE user_id IN "
                "(SELECT id FROM t_users "
                "INTERSECT SELECT id FROM t_users WHERE age IS NOT NULL) "
                "ORDER BY id",
                tags=["compound", "compound_subquery_intersect"],
                desc="子查询含 INTERSECT",
            ),
            self._seed(
                "SELECT DISTINCT tx_type FROM t_transactions "
                "UNION SELECT DISTINCT event_type FROM t_events "
                "EXCEPT SELECT 'error' AS val "
                "ORDER BY tx_type",
                tags=["compound", "compound_types_except_error"],
                desc="UNION + EXCEPT — 所有类型去掉 error",
            ),
            self._seed(
                "(SELECT id, username, score FROM t_users WHERE score >= 90) "
                "UNION ALL (SELECT id, name, salary / 1000 FROM t_employees WHERE salary >= 100000) "
                "ORDER BY score",
                tags=["compound", "compound_top_users_employees"],
                desc="UNION ALL — 顶级用户和员工按分数排序",
            ),
            self._seed(
                "SELECT category FROM t_products WHERE category IS NOT NULL "
                "INTERSECT SELECT name FROM t_departments WHERE budget > 100000 "
                "ORDER BY category",
                tags=["compound", "compound_cat_dept_intersect"],
                desc="INTERSECT — 产品分类与部门名交集",
            ),
            self._seed(
                "SELECT id, status FROM t_orders WHERE status = 'completed' "
                "UNION ALL SELECT id, status FROM t_transactions WHERE status = 'completed' "
                "ORDER BY id",
                tags=["compound", "compound_all_completed"],
                desc="UNION ALL — 所有已完成订单和交易",
            ),
        ]
