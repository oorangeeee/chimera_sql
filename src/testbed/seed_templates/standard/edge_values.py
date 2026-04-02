"""边界值标准模板 — 验证标准 SQL 边界值正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class StandardEdgeValuesTemplate(SeedTemplate):

    @property
    def domain(self) -> str:
        return "edge_values"

    @property
    def description(self) -> str:
        return "标准SQL边界值测试（0/负数/空结果集/多列NULL）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._zero_neg())
        seeds.extend(self._empty_results())
        seeds.extend(self._multi_null())
        return seeds

    def _zero_neg(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, name, salary FROM t_employees WHERE salary = 0 ORDER BY id", tags=["zero_neg", "salary_zero"], desc="salary = 0"),
            self._seed("SELECT id, name, budget FROM t_departments WHERE budget <= 0 ORDER BY id", tags=["zero_neg", "budget_le_zero"], desc="budget <= 0"),
            self._seed("SELECT id, amount FROM t_transactions WHERE amount < 0 ORDER BY id", tags=["zero_neg", "negative_amount"], desc="负数金额"),
            self._seed("SELECT id, name, salary, ABS(salary) AS abs_sal FROM t_employees WHERE salary < 0 ORDER BY id", tags=["zero_neg", "neg_salary_abs"], desc="负数薪资 + ABS"),
            self._seed("SELECT id, name, budget FROM t_departments WHERE budget = 0 OR budget IS NULL ORDER BY id", tags=["zero_neg", "zero_null_budget"], desc="零或 NULL 预算"),
            self._seed("SELECT id, amount FROM t_transactions WHERE amount = 0 OR amount IS NULL ORDER BY id", tags=["zero_neg", "zero_null_amount"], desc="零或 NULL 金额"),
            self._seed("SELECT id, name, stock FROM t_products WHERE stock = 0 ORDER BY id", tags=["zero_neg", "zero_stock"], desc="零库存"),
            self._seed("SELECT id, name, salary FROM t_employees WHERE salary > 0 ORDER BY salary ASC, id", tags=["zero_neg", "positive_salary"], desc="正数薪资排序"),
            self._seed("SELECT id, amount, ABS(amount) AS abs_amt, amount * -1 AS negated FROM t_transactions WHERE amount IS NOT NULL ORDER BY id", tags=["zero_neg", "abs_negate"], desc="ABS + 取反"),
        ]

    def _empty_results(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username FROM t_users WHERE score > 999 ORDER BY id", tags=["empty", "impossible_score"], desc="不可能条件 — 空结果"),
            self._seed("SELECT id, name FROM t_products WHERE price < 0 AND price > 100 ORDER BY id", tags=["empty", "contradict"], desc="矛盾条件 — 空结果"),
            self._seed("SELECT id, name FROM t_employees WHERE dept_id = -1 ORDER BY id", tags=["empty", "no_dept"], desc="不存在的部门 ID"),
            self._seed("SELECT id, username FROM t_users WHERE username = 'NONEXISTENT_USER_12345' ORDER BY id", tags=["empty", "no_user"], desc="不存在的用户"),
            self._seed("SELECT COUNT(*) AS cnt FROM t_users WHERE score > 999", tags=["empty", "count_empty"], desc="空结果 COUNT — 应为 0"),
            self._seed("SELECT id, name FROM t_departments WHERE budget < -99999 ORDER BY id", tags=["empty", "no_budget"], desc="不可能的预算"),
            self._seed("SELECT id, event_type FROM t_events WHERE event_type = 'NONEXISTENT_EVENT' ORDER BY id", tags=["empty", "no_event"], desc="不存在的类型"),
            self._seed("SELECT id, amount FROM t_transactions WHERE amount > 999999 ORDER BY id", tags=["empty", "no_amount"], desc="不可能的金额"),
        ]

    def _multi_null(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username FROM t_users WHERE score IS NULL AND age IS NULL ORDER BY id", tags=["multi_null", "both_null"], desc="两列同时 NULL"),
            self._seed("SELECT id, username FROM t_users WHERE email IS NULL OR score IS NULL OR age IS NULL ORDER BY id", tags=["multi_null", "any_null"], desc="任一列 NULL"),
            self._seed("SELECT id, name FROM t_employees WHERE salary IS NULL AND dept_id IS NULL ORDER BY id", tags=["multi_null", "emp_both_null"], desc="员工两列 NULL"),
            self._seed("SELECT id, name FROM t_employees WHERE salary IS NOT NULL AND dept_id IS NOT NULL AND hire_date IS NOT NULL ORDER BY id", tags=["multi_null", "emp_all_not_null"], desc="全部非 NULL"),
            self._seed("SELECT id, from_user, to_user FROM t_transactions WHERE from_user IS NULL OR to_user IS NULL ORDER BY id", tags=["multi_null", "tx_null_user"], desc="交易缺少用户"),
            self._seed("SELECT id, name, budget, location FROM t_departments WHERE budget IS NULL AND location IS NULL ORDER BY id", tags=["multi_null", "dept_both_null"], desc="部门两列 NULL"),
            self._seed("SELECT id, username, COALESCE(email, 'N/A') AS email_val, COALESCE(score, 0) AS score_val, COALESCE(age, 0) AS age_val FROM t_users ORDER BY id", tags=["multi_null", "multi_coalesce"], desc="多列 COALESCE"),
            self._seed("SELECT id, name, COALESCE(salary, 0) AS salary_val, COALESCE(dept_id, 0) AS dept_val, COALESCE(hire_date, DATE('2000-01-01')) AS hire_val FROM t_employees ORDER BY id", tags=["multi_null", "emp_coalesce"], desc="员工多列 COALESCE"),
        ]
