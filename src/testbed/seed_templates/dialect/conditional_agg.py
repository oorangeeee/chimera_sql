"""条件聚合方言差异模板 — 测试 SQLite/Oracle 条件聚合差异。

覆盖差异点：
- COUNT(CASE WHEN...) 条件计数
- SUM(CASE WHEN...) 条件求和
- AVG(CASE WHEN...) 条件平均
- 多条件聚合
- CASE 在 GROUP BY 聚合中的行为
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class ConditionalAggTemplate(SeedTemplate):
    """条件聚合方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "conditional_agg"

    @property
    def description(self) -> str:
        return "条件聚合方言差异测试（COUNT/SUM/AVG + CASE WHEN）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._count_case())
        seeds.extend(self._sum_case())
        seeds.extend(self._avg_case())
        seeds.extend(self._multi_cond_agg())
        return seeds

    # ── COUNT + CASE (~8) ───────────────────────────────
    def _count_case(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT COUNT(CASE WHEN score > 80 THEN 1 END) AS high_scorers, "
                "COUNT(CASE WHEN score <= 80 AND score IS NOT NULL THEN 1 END) AS low_scorers, "
                "COUNT(CASE WHEN score IS NULL THEN 1 END) AS no_score "
                "FROM t_users",
                tags=["count_case", "score_buckets"],
                desc="COUNT + CASE — 分数分段计数",
            ),
            self._seed(
                "SELECT dept_id, "
                "COUNT(CASE WHEN salary > 100000 THEN 1 END) AS high_earners, "
                "COUNT(CASE WHEN salary <= 100000 AND salary IS NOT NULL THEN 1 END) AS regular, "
                "COUNT(CASE WHEN salary IS NULL THEN 1 END) AS no_salary "
                "FROM t_employees WHERE dept_id IS NOT NULL "
                "GROUP BY dept_id ORDER BY dept_id",
                tags=["count_case", "dept_salary_buckets"],
                desc="COUNT CASE + GROUP BY — 部门薪资分布",
            ),
            self._seed(
                "SELECT status, "
                "COUNT(CASE WHEN amount > 500 THEN 1 END) AS large_tx, "
                "COUNT(CASE WHEN amount <= 500 AND amount IS NOT NULL THEN 1 END) AS small_tx "
                "FROM t_transactions WHERE status IS NOT NULL "
                "GROUP BY status ORDER BY status",
                tags=["count_case", "tx_amount_by_status"],
                desc="COUNT CASE — 按状态分大额小额",
            ),
            self._seed(
                "SELECT COUNT(CASE WHEN email IS NOT NULL THEN 1 END) AS with_email, "
                "COUNT(CASE WHEN score IS NOT NULL THEN 1 END) AS with_score, "
                "COUNT(CASE WHEN age IS NOT NULL THEN 1 END) AS with_age "
                "FROM t_users",
                tags=["count_case", "field_completeness"],
                desc="COUNT CASE — 字段完整性统计",
            ),
            self._seed(
                "SELECT d.name AS dept, "
                "COUNT(CASE WHEN e.status = 'active' THEN 1 END) AS active, "
                "COUNT(CASE WHEN e.status = 'inactive' THEN 1 END) AS inactive, "
                "COUNT(CASE WHEN e.status = 'on_leave' THEN 1 END) AS on_leave "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name ORDER BY d.name",
                tags=["count_case", "dept_status_dist"],
                desc="COUNT CASE + LEFT JOIN — 部门状态分布",
            ),
            self._seed(
                "SELECT event_type, "
                "COUNT(CASE WHEN user_id IS NOT NULL THEN 1 END) AS known_user, "
                "COUNT(CASE WHEN user_id IS NULL THEN 1 END) AS anonymous "
                "FROM t_events GROUP BY event_type ORDER BY event_type",
                tags=["count_case", "event_user_type"],
                desc="COUNT CASE — 事件按用户身份分布",
            ),
            self._seed(
                "SELECT COUNT(CASE WHEN price > 0 THEN 1 END) AS priced, "
                "COUNT(CASE WHEN stock > 0 THEN 1 END) AS in_stock, "
                "COUNT(CASE WHEN category IS NOT NULL THEN 1 END) AS categorized "
                "FROM t_products",
                tags=["count_case", "product_fields"],
                desc="COUNT CASE — 产品字段统计",
            ),
            self._seed(
                "SELECT COUNT(CASE WHEN amount > 0 THEN 1 END) AS positive, "
                "COUNT(CASE WHEN amount = 0 THEN 1 END) AS zero, "
                "COUNT(CASE WHEN amount < 0 THEN 1 END) AS negative, "
                "COUNT(CASE WHEN amount IS NULL THEN 1 END) AS null_amt "
                "FROM t_transactions",
                tags=["count_case", "amount_sign_count"],
                desc="COUNT CASE — 金额正负零 NULL 分布",
            ),
        ]

    # ── SUM + CASE (~8) ─────────────────────────────────
    def _sum_case(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT dept_id, "
                "SUM(CASE WHEN salary > 100000 THEN salary ELSE 0 END) AS high_salary_total, "
                "SUM(CASE WHEN salary <= 100000 THEN salary ELSE 0 END) AS low_salary_total "
                "FROM t_employees WHERE dept_id IS NOT NULL AND salary IS NOT NULL "
                "GROUP BY dept_id ORDER BY dept_id",
                tags=["sum_case", "dept_salary_split"],
                desc="SUM CASE — 部门高薪/低薪总额",
            ),
            self._seed(
                "SELECT status, "
                "SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS positive_total, "
                "SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS refund_total "
                "FROM t_transactions WHERE status IS NOT NULL "
                "GROUP BY status ORDER BY status",
                tags=["sum_case", "tx_pos_neg"],
                desc="SUM CASE — 正负金额分别统计",
            ),
            self._seed(
                "SELECT SUM(CASE WHEN category = 'electronics' THEN price ELSE 0 END) AS elec_total, "
                "SUM(CASE WHEN category IS NULL THEN price ELSE 0 END) AS no_cat_total "
                "FROM t_products",
                tags=["sum_case", "category_price"],
                desc="SUM CASE — 按分类求价格总额",
            ),
            self._seed(
                "SELECT from_user, "
                "SUM(CASE WHEN tx_type = 'transfer' THEN amount ELSE 0 END) AS transfers, "
                "SUM(CASE WHEN tx_type = 'payment' THEN amount ELSE 0 END) AS payments "
                "FROM t_transactions WHERE from_user IS NOT NULL "
                "GROUP BY from_user ORDER BY from_user",
                tags=["sum_case", "user_tx_type_sum"],
                desc="SUM CASE — 用户按类型统计金额",
            ),
            self._seed(
                "SELECT d.name AS dept, "
                "SUM(CASE WHEN e.hire_date >= DATE('2022-01-01') THEN 1 ELSE 0 END) AS recent_hires, "
                "SUM(CASE WHEN e.hire_date < DATE('2022-01-01') OR e.hire_date IS NULL THEN 1 ELSE 0 END) AS tenured "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name ORDER BY d.name",
                tags=["sum_case", "dept_hire_tenure"],
                desc="SUM CASE — 部门新老员工统计",
            ),
            self._seed(
                "SELECT SUM(CASE WHEN score BETWEEN 90 AND 100 THEN 1 ELSE 0 END) AS grade_a, "
                "SUM(CASE WHEN score BETWEEN 80 AND 89 THEN 1 ELSE 0 END) AS grade_b, "
                "SUM(CASE WHEN score BETWEEN 70 AND 79 THEN 1 ELSE 0 END) AS grade_c, "
                "SUM(CASE WHEN score < 70 AND score IS NOT NULL THEN 1 ELSE 0 END) AS grade_d "
                "FROM t_users",
                tags=["sum_case", "grade_distribution"],
                desc="SUM CASE — 成绩等级分布",
            ),
            self._seed(
                "SELECT event_type, "
                "SUM(CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END) AS known_events, "
                "SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS anon_events "
                "FROM t_events GROUP BY event_type ORDER BY event_type",
                tags=["sum_case", "event_known_anon"],
                desc="SUM CASE — 事件已知/匿名统计",
            ),
            self._seed(
                "SELECT dept_id, "
                "SUM(CASE WHEN status = 'active' THEN salary ELSE 0 END) AS active_salary, "
                "SUM(CASE WHEN status != 'active' OR status IS NULL THEN COALESCE(salary, 0) ELSE 0 END) AS other_salary "
                "FROM t_employees WHERE dept_id IS NOT NULL "
                "GROUP BY dept_id ORDER BY dept_id",
                tags=["sum_case", "active_other_salary"],
                desc="SUM CASE — 活跃/其他员工薪资",
            ),
        ]

    # ── AVG + CASE (~7) ─────────────────────────────────
    def _avg_case(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT dept_id, "
                "AVG(CASE WHEN salary > 80000 THEN salary END) AS avg_high, "
                "AVG(CASE WHEN salary <= 80000 THEN salary END) AS avg_low "
                "FROM t_employees WHERE dept_id IS NOT NULL "
                "GROUP BY dept_id ORDER BY dept_id",
                tags=["avg_case", "dept_avg_split"],
                desc="AVG CASE — 高/低薪平均值（NULL 跳过）",
            ),
            self._seed(
                "SELECT AVG(CASE WHEN score > 80 THEN score END) AS avg_top, "
                "AVG(CASE WHEN score <= 80 AND score IS NOT NULL THEN score END) AS avg_rest "
                "FROM t_users",
                tags=["avg_case", "score_avg_split"],
                desc="AVG CASE — 高/低分段平均（NULL 处理差异）",
            ),
            self._seed(
                "SELECT category, "
                "AVG(CASE WHEN stock > 0 THEN price END) AS avg_in_stock_price "
                "FROM t_products WHERE category IS NOT NULL "
                "GROUP BY category ORDER BY category",
                tags=["avg_case", "avg_price_in_stock"],
                desc="AVG CASE — 仅计算有库存产品的平均价",
            ),
            self._seed(
                "SELECT status, "
                "AVG(CASE WHEN amount > 0 THEN amount END) AS avg_positive "
                "FROM t_transactions WHERE status IS NOT NULL "
                "GROUP BY status ORDER BY status",
                tags=["avg_case", "avg_positive_tx"],
                desc="AVG CASE — 仅计算正金额平均",
            ),
            self._seed(
                "SELECT dept_id, "
                "AVG(CASE WHEN hire_date >= DATE('2022-01-01') THEN salary END) AS avg_recent, "
                "AVG(CASE WHEN hire_date < DATE('2022-01-01') THEN salary END) AS avg_veteran "
                "FROM t_employees WHERE dept_id IS NOT NULL "
                "GROUP BY dept_id ORDER BY dept_id",
                tags=["avg_case", "avg_salary_by_tenure"],
                desc="AVG CASE — 新老员工薪资平均",
            ),
            self._seed(
                "SELECT ROUND(AVG(CASE WHEN score IS NOT NULL THEN score END), 2) AS avg_score, "
                "ROUND(AVG(CASE WHEN age IS NOT NULL THEN age END), 2) AS avg_age "
                "FROM t_users",
                tags=["avg_case", "avg_score_age"],
                desc="AVG CASE + ROUND — 含 NULL 处理",
            ),
            self._seed(
                "SELECT d.name AS dept, "
                "AVG(CASE WHEN e.status = 'active' THEN e.salary END) AS avg_active_sal "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name ORDER BY d.name",
                tags=["avg_case", "dept_active_avg"],
                desc="AVG CASE + LEFT JOIN — 活跃员工平均薪资",
            ),
        ]

    # ── 多条件聚合 (~7) ─────────────────────────────────
    def _multi_cond_agg(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT d.name AS dept, "
                "COUNT(*) AS total, "
                "COUNT(CASE WHEN e.salary > 100000 THEN 1 END) AS high_earners, "
                "SUM(CASE WHEN e.salary > 100000 THEN e.salary ELSE 0 END) AS high_total, "
                "AVG(e.salary) AS avg_salary "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name ORDER BY d.name",
                tags=["multi_agg", "dept_comprehensive"],
                desc="综合聚合 — 部门完整报告",
            ),
            self._seed(
                "SELECT u.username, "
                "COUNT(o.id) AS total_orders, "
                "SUM(CASE WHEN o.status = 'completed' THEN o.total_amount ELSE 0 END) AS completed_total, "
                "AVG(o.total_amount) AS avg_order "
                "FROM t_users u LEFT JOIN t_orders o ON u.id = o.user_id "
                "GROUP BY u.id, u.username ORDER BY u.id",
                tags=["multi_agg", "user_order_report"],
                desc="综合聚合 — 用户订单报告",
            ),
            self._seed(
                "SELECT event_type, "
                "COUNT(*) AS total, "
                "COUNT(DISTINCT user_id) AS unique_users, "
                "SUM(CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END) AS known_user_events, "
                "SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS anon_events "
                "FROM t_events GROUP BY event_type ORDER BY event_type",
                tags=["multi_agg", "event_comprehensive"],
                desc="综合聚合 — 事件完整报告",
            ),
            self._seed(
                "SELECT d.name AS dept, "
                "COUNT(CASE WHEN e.salary >= 100000 THEN 1 END) AS senior_count, "
                "COUNT(CASE WHEN e.salary BETWEEN 60000 AND 99999 THEN 1 END) AS mid_count, "
                "COUNT(CASE WHEN e.salary < 60000 AND e.salary IS NOT NULL THEN 1 END) AS junior_count, "
                "COUNT(CASE WHEN e.salary IS NULL THEN 1 END) AS unknown "
                "FROM t_departments d LEFT JOIN t_employees e ON d.id = e.dept_id "
                "GROUP BY d.id, d.name ORDER BY d.name",
                tags=["multi_agg", "dept_salary_matrix"],
                desc="四路矩阵聚合 — 部门薪资分布矩阵",
            ),
            self._seed(
                "SELECT status, tx_type, "
                "COUNT(*) AS cnt, "
                "SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS total_positive, "
                "SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS total_negative "
                "FROM t_transactions "
                "WHERE status IS NOT NULL AND tx_type IS NOT NULL "
                "GROUP BY status, tx_type ORDER BY status, tx_type",
                tags=["multi_agg", "tx_comprehensive"],
                desc="多列 GROUP BY + CASE SUM",
            ),
            self._seed(
                "SELECT "
                "COUNT(CASE WHEN score >= 90 THEN 1 END) AS A, "
                "COUNT(CASE WHEN score >= 80 AND score < 90 THEN 1 END) AS B, "
                "COUNT(CASE WHEN score >= 70 AND score < 80 THEN 1 END) AS C, "
                "COUNT(CASE WHEN score < 70 AND score IS NOT NULL THEN 1 END) AS D, "
                "COUNT(CASE WHEN score IS NULL THEN 1 END) AS missing, "
                "ROUND(AVG(score), 2) AS overall_avg "
                "FROM t_users",
                tags=["multi_agg", "full_grade_report"],
                desc="五级评分 + 总平均 — 完整报告",
            ),
            self._seed(
                "SELECT from_user, tx_type, "
                "COUNT(*) AS tx_count, "
                "SUM(amount) AS total_amount, "
                "AVG(amount) AS avg_amount, "
                "MAX(amount) AS max_amount, "
                "MIN(amount) AS min_amount "
                "FROM t_transactions WHERE from_user IS NOT NULL AND amount IS NOT NULL "
                "GROUP BY from_user, tx_type "
                "HAVING COUNT(*) > 0 "
                "ORDER BY from_user, tx_type",
                tags=["multi_agg", "user_type_stats"],
                desc="六聚合函数 + GROUP BY + HAVING",
            ),
        ]
