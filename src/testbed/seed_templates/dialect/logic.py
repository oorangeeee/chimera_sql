"""逻辑运算+三值逻辑方言差异模板 — 测试 SQLite/Oracle 三值逻辑差异。

覆盖差异点：
- AND/OR/NOT 与 NULL 交互
- 三值逻辑（TRUE/FALSE/UNKNOWN）
- NULL 在逻辑运算中的传播
- De Morgan 定律验证
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class LogicTemplate(SeedTemplate):
    """逻辑运算+三值逻辑方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "logic"

    @property
    def description(self) -> str:
        return "逻辑运算+三值逻辑方言差异测试（AND/OR/NOT/NULL 传播）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._and_null())
        seeds.extend(self._or_null())
        seeds.extend(self._not_null())
        seeds.extend(self._three_valued())
        return seeds

    # ── AND + NULL (~8) ──────────────────────────────────
    def _and_null(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE score IS NOT NULL AND score > 80 ORDER BY id",
                tags=["and_null", "and_not_null_gt"],
                desc="AND — 先检查 NOT NULL 再比较",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE email IS NOT NULL AND age IS NOT NULL ORDER BY id",
                tags=["and_null", "and_both_not_null"],
                desc="AND — 两列均 NOT NULL",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE price > 50 AND stock > 0 AND category IS NOT NULL ORDER BY id",
                tags=["and_null", "and_three_cond"],
                desc="AND 三条件 — 含 NULL 检查",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE dept_id IS NOT NULL AND salary IS NOT NULL "
                "AND hire_date IS NOT NULL ORDER BY id",
                tags=["and_null", "and_full_profile"],
                desc="AND 多列 NOT NULL — 完整档案",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE salary > 80000 AND dept_id IS NOT NULL AND manager_id IS NOT NULL "
                "ORDER BY id",
                tags=["and_null", "and_senior_managed"],
                desc="AND — 高薪有部门有经理",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE score > 0 AND email LIKE '%@%' ORDER BY id",
                tags=["and_null", "and_score_email"],
                desc="AND — score > 0 且 email 含 @",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE (category IS NULL OR category = 'misc') AND price < 100 ORDER BY id",
                tags=["and_null", "and_or_combo"],
                desc="AND + OR 组合 — NULL 或特定分类且低价",
            ),
            self._seed(
                "SELECT id, from_user, to_user FROM t_transactions "
                "WHERE from_user IS NOT NULL AND to_user IS NOT NULL "
                "AND amount > 0 ORDER BY id",
                tags=["and_null", "and_complete_tx"],
                desc="AND — 完整交易（双方非空且正金额）",
            ),
        ]

    # ── OR + NULL (~8) ──────────────────────────────────
    def _or_null(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE score > 90 OR age < 20 ORDER BY id",
                tags=["or_null", "or_score_age"],
                desc="OR — 高分或年轻",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE price < 10 OR stock IS NULL ORDER BY id",
                tags=["or_null", "or_cheap_or_no_stock"],
                desc="OR — 便宜或无库存信息",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE salary > 120000 OR dept_id IS NULL ORDER BY id",
                tags=["or_null", "or_high_or_no_dept"],
                desc="OR — 高薪或无部门",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE email IS NULL OR score IS NULL ORDER BY id",
                tags=["or_null", "or_missing_data"],
                desc="OR — 缺少 email 或 score",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE manager_id IS NULL OR hire_date IS NULL ORDER BY id",
                tags=["or_null", "or_missing_manager_hire"],
                desc="OR — 无经理或无入职日期",
            ),
            self._seed(
                "SELECT id, event_type FROM t_events "
                "WHERE user_id IS NULL OR event_date IS NULL ORDER BY id",
                tags=["or_null", "or_anonymous_or_no_date"],
                desc="OR — 匿名或无日期事件",
            ),
            self._seed(
                "SELECT id, status FROM t_transactions "
                "WHERE from_user IS NULL OR to_user IS NULL OR amount IS NULL "
                "ORDER BY id",
                tags=["or_null", "or_incomplete_tx"],
                desc="OR 三条件 — 不完整交易",
            ),
            self._seed(
                "SELECT id, name FROM t_departments "
                "WHERE budget IS NULL OR location IS NULL ORDER BY id",
                tags=["or_null", "or_dept_missing"],
                desc="OR — 缺少预算或位置",
            ),
        ]

    # ── NOT + NULL (~7) ──────────────────────────────────
    def _not_null(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE NOT score > 80 OR score IS NULL ORDER BY id",
                tags=["not_null", "not_score_or_null"],
                desc="NOT + OR NULL — 低分或无分数",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE NOT (price > 100 AND stock > 0) ORDER BY id",
                tags=["not_null", "not_expensive_instock"],
                desc="NOT (AND) — De Morgan 定律验证",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE NOT dept_id = 1 AND dept_id IS NOT NULL ORDER BY id",
                tags=["not_null", "not_dept_1"],
                desc="NOT 等于 + IS NOT NULL 保护",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE NOT email IS NULL ORDER BY id",
                tags=["not_null", "not_is_null"],
                desc="NOT IS NULL — 等价于 IS NOT NULL",
            ),
            self._seed(
                "SELECT id, name FROM t_products "
                "WHERE NOT category IN ('electronics', 'clothing') "
                "OR category IS NULL ORDER BY id",
                tags=["not_null", "not_in_or_null"],
                desc="NOT IN + OR NULL — 排除特定分类含 NULL",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE NOT (salary > 100000 AND status = 'active') "
                "ORDER BY id",
                tags=["not_null", "not_active_high"],
                desc="NOT (AND) — 非高薪活跃员工",
            ),
            self._seed(
                "SELECT id, status FROM t_orders "
                "WHERE NOT status = 'completed' AND status IS NOT NULL ORDER BY id",
                tags=["not_null", "not_completed"],
                desc="NOT 等于 + IS NOT NULL — 非完成订单",
            ),
        ]

    # ── 三值逻辑 (~7) ──────────────────────────────────
    def _three_valued(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, "
                "CASE WHEN score > 80 THEN 'high' "
                "WHEN score <= 80 THEN 'low' "
                "ELSE 'unknown' END AS score_class "
                "FROM t_users ORDER BY id",
                tags=["three_val", "case_null_branch"],
                desc="CASE 三值逻辑 — NULL 走 ELSE",
            ),
            self._seed(
                "SELECT id, name, "
                "CASE WHEN price > 100 THEN 'expensive' "
                "WHEN NOT price > 100 THEN 'affordable' "
                "ELSE 'unknown' END AS price_class "
                "FROM t_products ORDER BY id",
                tags=["three_val", "not_in_case"],
                desc="CASE NOT 条件 — NULL 不满足任何分支",
            ),
            self._seed(
                "SELECT id, username FROM t_users "
                "WHERE (score > 80 OR age < 25) IS NOT DISTINCT FROM TRUE ORDER BY id",
                tags=["three_val", "is_distinct"],
                desc="IS NOT DISTINCT FROM — 三值逻辑明确比较",
            ),
            self._seed(
                "SELECT id, name FROM t_employees "
                "WHERE (salary > 100000 AND dept_id = 1) "
                "OR (salary IS NULL) ORDER BY id",
                tags=["three_val", "null_as_match"],
                desc="NULL 作为 OR 条件匹配 — 三值逻辑",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN score IS NULL AND age IS NULL THEN 'both_null' "
                "WHEN score IS NULL OR age IS NULL THEN 'one_null' "
                "WHEN score > 80 AND age > 25 THEN 'qualified' "
                "ELSE 'not_qualified' END AS assessment "
                "FROM t_users ORDER BY id",
                tags=["three_val", "multi_null_assess"],
                desc="多层 NULL 检查 + 条件 — 三值逻辑综合",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "CASE WHEN salary IS NOT NULL AND dept_id IS NOT NULL THEN 'complete' "
                "WHEN salary IS NULL AND dept_id IS NOT NULL THEN 'no_salary' "
                "WHEN salary IS NOT NULL AND dept_id IS NULL THEN 'no_dept' "
                "ELSE 'minimal' END AS profile_type "
                "FROM t_employees ORDER BY id",
                tags=["three_val", "four_way_null"],
                desc="四路 NULL 分支 — 三值逻辑矩阵",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN (score > 70 OR age IS NULL) AND email IS NOT NULL "
                "THEN 'eligible' ELSE 'review' END AS status "
                "FROM t_users ORDER BY id",
                tags=["three_val", "complex_logic"],
                desc="复杂嵌套逻辑 — OR/AND/NULL 交互",
            ),
        ]
