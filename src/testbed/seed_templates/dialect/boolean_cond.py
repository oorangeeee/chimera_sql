"""布尔/条件表达式方言差异模板 — 测试 SQLite/Oracle 布尔语义差异。

覆盖差异点：
- SQLite: 布尔用 1/0 表示
- Oracle: 无原生 BOOLEAN 类型（PL/SQL 有但 SQL 没有）
- CASE 表达式返回类型推导差异
- 条件表达式中的隐式类型转换
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class BooleanCondTemplate(SeedTemplate):
    """布尔/条件表达式方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "boolean_cond"

    @property
    def description(self) -> str:
        return "布尔/条件表达式方言差异测试（SQLite 1/0 vs Oracle 无 BOOLEAN）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._boolean_expressions())
        seeds.extend(self._case_boolean())
        seeds.extend(self._conditional_logic())
        return seeds

    # ── 布尔表达式 (~10) ────────────────────────────────
    def _boolean_expressions(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, CASE WHEN score > 80 THEN 1 ELSE 0 END AS is_high_scorer "
                "FROM t_users ORDER BY id",
                tags=["bool_expr", "case_as_bool"],
                desc="CASE 返回 1/0 模拟布尔",
            ),
            self._seed(
                "SELECT id, name, CASE WHEN stock > 0 THEN 1 ELSE 0 END AS in_stock "
                "FROM t_products ORDER BY id",
                tags=["bool_expr", "stock_bool"],
                desc="库存检查 → 1/0",
            ),
            self._seed(
                "SELECT id, name, CASE WHEN salary >= 100000 THEN 1 ELSE 0 END AS is_high_earner "
                "FROM t_employees ORDER BY id",
                tags=["bool_expr", "salary_bool"],
                desc="薪资检查 → 1/0",
            ),
            self._seed(
                "SELECT id, CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END AS has_email, "
                "CASE WHEN score IS NOT NULL THEN 1 ELSE 0 END AS has_score "
                "FROM t_users ORDER BY id",
                tags=["bool_expr", "multi_bool"],
                desc="多列布尔检查",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN score IS NOT NULL AND score > 80 THEN 'high' "
                "WHEN score IS NOT NULL AND score > 50 THEN 'medium' "
                "ELSE 'low_or_null' END AS score_level "
                "FROM t_users ORDER BY id",
                tags=["bool_expr", "score_level"],
                desc="CASE 多条件分级",
            ),
            self._seed(
                "SELECT id, name, "
                "CASE WHEN price > 100 AND stock > 0 THEN 'premium_available' "
                "WHEN price > 100 THEN 'premium_no_stock' "
                "WHEN stock > 0 THEN 'budget_available' "
                "ELSE 'other' END AS product_class "
                "FROM t_products ORDER BY id",
                tags=["bool_expr", "product_class"],
                desc="CASE 复合条件分类",
            ),
            self._seed(
                "SELECT id, name, status, "
                "CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active "
                "FROM t_employees ORDER BY id",
                tags=["bool_expr", "active_bool"],
                desc="状态检查 → 1/0",
            ),
            self._seed(
                "SELECT id, "
                "CASE WHEN amount > 0 THEN 1 ELSE 0 END AS is_positive, "
                "CASE WHEN amount < 0 THEN 1 ELSE 0 END AS is_negative "
                "FROM t_transactions ORDER BY id",
                tags=["bool_expr", "amount_sign"],
                desc="金额正负检查",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN age IS NULL OR score IS NULL THEN 0 ELSE 1 END AS profile_complete "
                "FROM t_users ORDER BY id",
                tags=["bool_expr", "profile_complete"],
                desc="OR 条件 → 布尔",
            ),
            self._seed(
                "SELECT id, name, "
                "CASE WHEN dept_id IS NOT NULL AND salary IS NOT NULL AND hire_date IS NOT NULL "
                "THEN 1 ELSE 0 END AS fully_assigned "
                "FROM t_employees ORDER BY id",
                tags=["bool_expr", "fully_assigned"],
                desc="AND 多条件 → 布尔",
            ),
        ]

    # ── CASE 布尔逻辑 (~10) ──────────────────────────────
    def _case_boolean(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, "
                "CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' "
                "WHEN score >= 70 THEN 'C' WHEN score >= 60 THEN 'D' "
                "ELSE 'F' END AS grade "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["case_bool", "grade_case"],
                desc="CASE 分级 — 成绩等级",
            ),
            self._seed(
                "SELECT id, username, score, "
                "CASE WHEN score >= 90 THEN 'excellent' "
                "WHEN score >= 70 THEN 'good' "
                "WHEN score >= 50 THEN 'average' "
                "WHEN score IS NOT NULL THEN 'below_avg' "
                "ELSE 'no_score' END AS performance "
                "FROM t_users ORDER BY id",
                tags=["case_bool", "performance_case"],
                desc="CASE 含 NULL 分支 — 绩效",
            ),
            self._seed(
                "SELECT id, name, "
                "CASE price WHEN 0 THEN 'free' WHEN 0.01 THEN 'minimal' "
                "ELSE 'priced' END AS price_type "
                "FROM t_products ORDER BY id",
                tags=["case_bool", "simple_case_price"],
                desc="简单 CASE — 价格分类",
            ),
            self._seed(
                "SELECT id, name, salary, "
                "CASE WHEN salary IS NULL THEN 'unknown' "
                "WHEN salary < 50000 THEN 'entry' "
                "WHEN salary < 80000 THEN 'mid' "
                "WHEN salary < 120000 THEN 'senior' "
                "ELSE 'executive' END AS salary_band "
                "FROM t_employees ORDER BY id",
                tags=["case_bool", "salary_band"],
                desc="CASE 分级 — 薪资带",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN email IS NOT NULL AND score IS NOT NULL THEN 'complete' "
                "WHEN email IS NOT NULL THEN 'email_only' "
                "WHEN score IS NOT NULL THEN 'score_only' "
                "ELSE 'minimal' END AS data_quality "
                "FROM t_users ORDER BY id",
                tags=["case_bool", "data_quality"],
                desc="CASE 多列 NULL 检查 — 数据质量",
            ),
            self._seed(
                "SELECT id, name, dept_id, salary, "
                "CASE WHEN dept_id IS NULL THEN salary "
                "ELSE salary * 1.1 END AS adjusted_salary "
                "FROM t_employees ORDER BY id",
                tags=["case_bool", "adjusted_salary"],
                desc="CASE 条件计算 — 调整薪资",
            ),
            self._seed(
                "SELECT id, amount, "
                "CASE WHEN amount > 500 THEN 'large' "
                "WHEN amount > 100 THEN 'medium' "
                "WHEN amount > 0 THEN 'small' "
                "WHEN amount = 0 THEN 'zero' "
                "ELSE 'negative' END AS amount_category "
                "FROM t_transactions ORDER BY id",
                tags=["case_bool", "amount_category"],
                desc="CASE 金额分类",
            ),
            self._seed(
                "SELECT id, event_type, "
                "CASE WHEN event_type IN ('login', 'logout') THEN 'auth' "
                "WHEN event_type IN ('purchase', 'payment') THEN 'commerce' "
                "WHEN event_type = 'error' THEN 'system' "
                "ELSE 'other' END AS event_category "
                "FROM t_events ORDER BY id",
                tags=["case_bool", "event_category"],
                desc="CASE + IN 子句分类",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN (score IS NOT NULL AND score > 80) OR (age IS NOT NULL AND age < 25) "
                "THEN 'priority' ELSE 'normal' END AS priority "
                "FROM t_users ORDER BY id",
                tags=["case_bool", "priority_case"],
                desc="CASE 含 OR + AND 组合",
            ),
            self._seed(
                "SELECT id, name, "
                "CASE WHEN dept_id IS NOT NULL AND salary IS NOT NULL THEN "
                "CASE WHEN salary > 100000 THEN 'dept_senior' ELSE 'dept_junior' END "
                "ELSE 'unassigned' END AS emp_category "
                "FROM t_employees ORDER BY id",
                tags=["case_bool", "nested_case"],
                desc="嵌套 CASE — 条件分类",
            ),
        ]

    # ── 条件逻辑 (~10) ──────────────────────────────────
    def _conditional_logic(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, "
                "CASE WHEN NOT (score IS NULL OR age IS NULL) THEN 'complete' "
                "ELSE 'incomplete' END AS info_status "
                "FROM t_users ORDER BY id",
                tags=["cond_logic", "not_condition"],
                desc="NOT 条件 — 非不完全则完整",
            ),
            self._seed(
                "SELECT id, name, price, stock, "
                "CASE WHEN price IS NOT NULL AND stock IS NOT NULL THEN price * stock "
                "ELSE 0 END AS inventory_value "
                "FROM t_products ORDER BY id",
                tags=["cond_logic", "inventory_value"],
                desc="条件计算 — 库存价值",
            ),
            self._seed(
                "SELECT id, name, "
                "CASE WHEN salary > 0 THEN salary "
                "WHEN salary = 0 THEN 0 "
                "WHEN salary < 0 THEN ABS(salary) "
                "ELSE 0 END AS abs_salary "
                "FROM t_employees ORDER BY id",
                tags=["cond_logic", "abs_salary"],
                desc="CASE 多分支含负数处理",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN score BETWEEN 80 AND 100 THEN 'top' "
                "WHEN score BETWEEN 50 AND 79 THEN 'mid' "
                "WHEN score IS NOT NULL THEN 'low' "
                "ELSE 'N/A' END AS score_band "
                "FROM t_users ORDER BY id",
                tags=["cond_logic", "between_case"],
                desc="CASE + BETWEEN 条件",
            ),
            self._seed(
                "SELECT id, name, "
                "CASE WHEN hire_date IS NULL THEN 'not_hired' "
                "WHEN hire_date >= DATE('2023-01-01') THEN 'new_hire' "
                "WHEN hire_date >= DATE('2020-01-01') THEN 'recent' "
                "ELSE 'veteran' END AS tenure "
                "FROM t_employees ORDER BY id",
                tags=["cond_logic", "tenure_case"],
                desc="CASE + 日期条件 — 入职时长",
            ),
            self._seed(
                "SELECT id, username, score, "
                "CASE WHEN score IS NULL THEN NULL "
                "WHEN score > 90 THEN score + 5 "
                "WHEN score > 70 THEN score + 3 "
                "ELSE score END AS bonus_score "
                "FROM t_users ORDER BY id",
                tags=["cond_logic", "bonus_score"],
                desc="CASE 保留 NULL — 加分逻辑",
            ),
            self._seed(
                "SELECT id, name, dept_id, manager_id, "
                "CASE WHEN manager_id IS NULL THEN 'top_level' "
                "WHEN dept_id IS NULL THEN 'unassigned' "
                "ELSE 'regular' END AS hierarchy "
                "FROM t_employees ORDER BY id",
                tags=["cond_logic", "hierarchy"],
                desc="CASE 层级判断",
            ),
            self._seed(
                "SELECT id, from_user, to_user, amount, "
                "CASE WHEN from_user IS NULL OR to_user IS NULL THEN 'incomplete' "
                "WHEN amount < 0 THEN 'refund' "
                "WHEN amount = 0 THEN 'zero' "
                "ELSE 'normal' END AS tx_class "
                "FROM t_transactions ORDER BY id",
                tags=["cond_logic", "tx_class"],
                desc="CASE 交易分类",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN score IS NULL AND age IS NULL THEN 'no_data' "
                "WHEN score IS NULL OR age IS NULL THEN 'partial' "
                "WHEN score > 80 AND age < 30 THEN 'young_talent' "
                "ELSE 'standard' END AS user_segment "
                "FROM t_users ORDER BY id",
                tags=["cond_logic", "user_segment"],
                desc="CASE 多条件 — 用户分群",
            ),
            self._seed(
                "SELECT id, name, budget, location, "
                "CASE WHEN budget IS NULL THEN 'no_budget' "
                "WHEN budget <= 0 THEN 'zero_budget' "
                "WHEN budget > 200000 THEN 'well_funded' "
                "ELSE 'normal_budget' END AS budget_status, "
                "CASE WHEN location IS NULL THEN 'remote' ELSE 'onsite' END AS work_mode "
                "FROM t_departments ORDER BY id",
                tags=["cond_logic", "dept_status"],
                desc="多列 CASE — 部门状态",
            ),
        ]
