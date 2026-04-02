"""CASE 表达式标准模板 — 验证标准 CASE SQL 正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class StandardCaseExprTemplate(SeedTemplate):

    @property
    def domain(self) -> str:
        return "case_expr"

    @property
    def description(self) -> str:
        return "标准SQL CASE表达式测试（简单/搜索/嵌套/聚合中CASE）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._simple_case())
        seeds.extend(self._searched_case())
        seeds.extend(self._nested_case())
        seeds.extend(self._case_in_agg())
        return seeds

    def _simple_case(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, CASE status WHEN 'active' THEN 'Y' ELSE 'N' END AS is_active FROM (SELECT id, username, CASE WHEN score IS NOT NULL THEN 'active' ELSE 'inactive' END AS status FROM t_users) ORDER BY id", tags=["simple_case", "user_status"], desc="简单 CASE"),
            self._seed("SELECT id, name, CASE dept_id WHEN 1 THEN 'Engineering' WHEN 2 THEN 'Sales' WHEN 3 THEN 'HR' ELSE 'Other' END AS dept_name FROM t_employees WHERE dept_id IS NOT NULL ORDER BY id", tags=["simple_case", "emp_dept_name"], desc="简单 CASE — 部门映射"),
            self._seed("SELECT id, event_type, CASE event_type WHEN 'login' THEN 'auth' WHEN 'purchase' THEN 'buy' WHEN 'error' THEN 'sys' ELSE 'other' END AS cat FROM t_events ORDER BY id", tags=["simple_case", "event_cat"], desc="简单 CASE — 事件分类"),
            self._seed("SELECT id, CASE tx_type WHEN 'transfer' THEN 'xfer' WHEN 'payment' THEN 'pay' WHEN 'refund' THEN 'ref' ELSE tx_type END AS short_type FROM t_transactions WHERE tx_type IS NOT NULL ORDER BY id", tags=["simple_case", "tx_short"], desc="简单 CASE — 交易缩写"),
            self._seed("SELECT id, name, CASE WHEN budget > 200000 THEN 'large' WHEN budget > 100000 THEN 'medium' ELSE 'small' END AS size FROM t_departments WHERE budget IS NOT NULL ORDER BY id", tags=["simple_case", "dept_size"], desc="搜索 CASE — 部门大小"),
        ]

    def _searched_case(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' WHEN score >= 60 THEN 'D' ELSE 'F' END AS grade FROM t_users WHERE score IS NOT NULL ORDER BY id", tags=["searched_case", "grade"], desc="搜索 CASE — 成绩"),
            self._seed("SELECT id, name, CASE WHEN salary >= 120000 THEN 'senior' WHEN salary >= 80000 THEN 'mid' WHEN salary >= 50000 THEN 'junior' ELSE 'entry' END AS level FROM t_employees WHERE salary IS NOT NULL ORDER BY id", tags=["searched_case", "salary_level"], desc="搜索 CASE — 薪资等级"),
            self._seed("SELECT id, name, CASE WHEN price > 100 THEN 'expensive' WHEN price > 50 THEN 'moderate' ELSE 'cheap' END AS price_tier FROM t_products WHERE price IS NOT NULL ORDER BY id", tags=["searched_case", "price_tier"], desc="搜索 CASE — 价格档"),
            self._seed("SELECT id, name, CASE WHEN email IS NOT NULL AND score IS NOT NULL THEN 'complete' WHEN email IS NOT NULL OR score IS NOT NULL THEN 'partial' ELSE 'empty' END AS profile FROM t_users ORDER BY id", tags=["searched_case", "profile"], desc="搜索 CASE — 资料完整性"),
            self._seed("SELECT id, name, CASE WHEN dept_id IS NOT NULL AND salary IS NOT NULL AND hire_date IS NOT NULL THEN 'full' WHEN dept_id IS NOT NULL THEN 'partial' ELSE 'minimal' END AS info FROM t_employees ORDER BY id", tags=["searched_case", "emp_info"], desc="搜索 CASE — 员工信息完整度"),
            self._seed("SELECT id, amount, CASE WHEN amount > 500 THEN 'large' WHEN amount > 100 THEN 'medium' WHEN amount > 0 THEN 'small' WHEN amount = 0 THEN 'zero' ELSE 'negative' END AS size FROM t_transactions ORDER BY id", tags=["searched_case", "amount_size"], desc="搜索 CASE — 金额大小"),
            self._seed("SELECT id, name, CASE WHEN budget IS NULL THEN 'no_budget' WHEN budget = 0 THEN 'zero' WHEN budget < 100000 THEN 'small' ELSE 'large' END AS budget_type FROM t_departments ORDER BY id", tags=["searched_case", "budget_type"], desc="搜索 CASE — 预算类型"),
            self._seed("SELECT id, username, CASE WHEN score IS NULL THEN 'N/A' WHEN score >= 80 THEN 'good' ELSE 'needs_improvement' END AS assessment FROM t_users ORDER BY id", tags=["searched_case", "assessment"], desc="搜索 CASE — 含 NULL"),
        ]

    def _nested_case(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, CASE WHEN score IS NOT NULL THEN CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END ELSE 'no_score' END AS detailed_grade FROM t_users ORDER BY id", tags=["nested_case", "grade_nested"], desc="嵌套 CASE — 含 NULL 检查"),
            self._seed("SELECT id, name, CASE WHEN dept_id IS NOT NULL THEN CASE WHEN salary > 100000 THEN 'dept_senior' WHEN salary > 50000 THEN 'dept_mid' ELSE 'dept_junior' END ELSE 'unassigned' END AS category FROM t_employees ORDER BY id", tags=["nested_case", "emp_category"], desc="嵌套 CASE — 部门+薪资"),
            self._seed("SELECT id, name, CASE WHEN category IS NOT NULL THEN CASE WHEN price > 100 THEN 'premium' ELSE 'standard' END ELSE 'uncategorized' END AS product_class FROM t_products ORDER BY id", tags=["nested_case", "product_class"], desc="嵌套 CASE — 分类+价格"),
            self._seed("SELECT id, amount, CASE WHEN amount > 0 THEN CASE WHEN tx_type = 'transfer' THEN 'transfer_out' WHEN tx_type = 'payment' THEN 'payment_out' ELSE 'other_out' END ELSE 'non_positive' END AS tx_detail FROM t_transactions WHERE tx_type IS NOT NULL ORDER BY id", tags=["nested_case", "tx_detail"], desc="嵌套 CASE — 金额+类型"),
            self._seed("SELECT id, name, CASE WHEN manager_id IS NOT NULL THEN CASE WHEN dept_id IS NOT NULL THEN 'managed_assigned' ELSE 'managed_unassigned' END ELSE 'unmanaged' END AS emp_type FROM t_employees ORDER BY id", tags=["nested_case", "emp_type"], desc="嵌套 CASE — 经理+部门"),
            self._seed("SELECT id, event_type, CASE WHEN user_id IS NOT NULL THEN CASE WHEN event_type = 'login' THEN 'known_login' WHEN event_type = 'purchase' THEN 'known_purchase' ELSE 'known_other' END ELSE 'anonymous' END AS event_class FROM t_events ORDER BY id", tags=["nested_case", "event_class"], desc="嵌套 CASE — 用户+事件类型"),
        ]

    def _case_in_agg(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT dept_id, SUM(CASE WHEN salary > 100000 THEN 1 ELSE 0 END) AS senior_count, COUNT(*) AS total FROM t_employees WHERE dept_id IS NOT NULL GROUP BY dept_id ORDER BY dept_id", tags=["case_agg", "dept_senior"], desc="CASE in SUM — 高薪人数"),
            self._seed("SELECT COUNT(CASE WHEN score > 80 THEN 1 END) AS high, COUNT(CASE WHEN score <= 80 AND score IS NOT NULL THEN 1 END) AS low FROM t_users", tags=["case_agg", "score_dist"], desc="CASE in COUNT — 分数分布"),
            self._seed("SELECT status, SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS positive_total, SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS refund_total FROM t_transactions WHERE status IS NOT NULL GROUP BY status ORDER BY status", tags=["case_agg", "tx_by_status"], desc="CASE in SUM — 正负金额"),
            self._seed("SELECT AVG(CASE WHEN salary > 80000 THEN salary END) AS avg_high, AVG(CASE WHEN salary <= 80000 AND salary IS NOT NULL THEN salary END) AS avg_low FROM t_employees", tags=["case_agg", "salary_split_avg"], desc="CASE in AVG — 高低薪平均"),
            self._seed("SELECT d.name, SUM(CASE WHEN e.status = 'active' THEN 1 ELSE 0 END) AS active, SUM(CASE WHEN e.status != 'active' OR e.status IS NULL THEN 1 ELSE 0 END) AS other FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id GROUP BY d.id, d.name ORDER BY d.name", tags=["case_agg", "dept_active"], desc="CASE in SUM + LEFT JOIN"),
            self._seed("SELECT category, SUM(CASE WHEN stock > 0 THEN 1 ELSE 0 END) AS in_stock, COUNT(*) AS total FROM t_products WHERE category IS NOT NULL GROUP BY category ORDER BY category", tags=["case_agg", "cat_in_stock"], desc="CASE in SUM — 库存状态"),
            self._seed("SELECT event_type, COUNT(CASE WHEN user_id IS NOT NULL THEN 1 END) AS known, COUNT(CASE WHEN user_id IS NULL THEN 1 END) AS anon FROM t_events GROUP BY event_type ORDER BY event_type", tags=["case_agg", "event_user_type"], desc="CASE in COUNT — 事件用户类型"),
            self._seed("SELECT CASE WHEN score >= 80 THEN 'high' WHEN score >= 50 THEN 'mid' WHEN score IS NOT NULL THEN 'low' ELSE 'null' END AS tier, COUNT(*) AS cnt FROM t_users GROUP BY tier ORDER BY tier", tags=["case_agg", "score_tier_group"], desc="CASE in GROUP BY — 分数分段"),
            self._seed("SELECT CASE WHEN salary >= 100000 THEN '100k+' WHEN salary >= 50000 THEN '50k-100k' WHEN salary IS NOT NULL THEN '<50k' ELSE 'null' END AS band, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM t_employees GROUP BY band ORDER BY band", tags=["case_agg", "salary_band_group"], desc="CASE in GROUP BY — 薪资带"),
            self._seed("SELECT tx_type, COUNT(*) AS cnt, SUM(CASE WHEN amount > 500 THEN 1 ELSE 0 END) AS large, SUM(CASE WHEN amount <= 500 THEN 1 ELSE 0 END) AS small FROM t_transactions WHERE tx_type IS NOT NULL GROUP BY tx_type ORDER BY tx_type", tags=["case_agg", "tx_size_by_type"], desc="CASE in SUM — 交易大小分布"),
        ]
