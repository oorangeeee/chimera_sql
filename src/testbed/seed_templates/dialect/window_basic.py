"""窗口函数基础差异种子模板 — ROW_NUMBER / RANK / NTILE / SUM AVG OVER / COUNT OVER。"""

from __future__ import annotations

from typing import List

from src.testbed.seed_templates.base import SchemaMetadata, SeedSQL, SeedTemplate
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WindowBasicTemplate(SeedTemplate):
    """基础窗口函数方言差异模板。"""

    @property
    def domain(self) -> str:
        return "window_basic"

    @property
    def description(self) -> str:
        return "基础窗口函数方言差异: ROW_NUMBER, RANK, DENSE_RANK, NTILE, SUM/AVG OVER, COUNT OVER"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []

        # ── 1. ROW_NUMBER (~8) ──────────────────────────────────
        seeds.extend(self._row_number())

        # ── 2. RANK / DENSE_RANK (~10) ──────────────────────────
        seeds.extend(self._rank_dense_rank())

        # ── 3. NTILE (~6) ───────────────────────────────────────
        seeds.extend(self._ntile())

        # ── 4. SUM / AVG OVER (~8) ──────────────────────────────
        seeds.extend(self._sum_avg_over())

        # ── 5. COUNT OVER (~8) ──────────────────────────────────
        seeds.extend(self._count_over())

        logger.info("WindowBasicTemplate 生成 %d 条种子 SQL", len(seeds))
        return seeds

    # ────────────────────────────────────────────────────────────
    # 1. ROW_NUMBER
    # ────────────────────────────────────────────────────────────
    def _row_number(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score, "
                "ROW_NUMBER() OVER (ORDER BY score DESC) AS rn "
                "FROM t_users ORDER BY rn",
                tags=["row_number", "order_by"],
                desc="ROW_NUMBER 按 score 降序排列",
            ),
            self._seed(
                "SELECT id, username, age, "
                "ROW_NUMBER() OVER (ORDER BY age) AS rn "
                "FROM t_users ORDER BY rn",
                tags=["row_number", "order_by"],
                desc="ROW_NUMBER 按 age 升序排列",
            ),
            self._seed(
                "SELECT id, username, age, score, "
                "ROW_NUMBER() OVER (PARTITION BY age ORDER BY score DESC) AS rn "
                "FROM t_users ORDER BY age, rn",
                tags=["row_number", "partition_by"],
                desc="ROW_NUMBER PARTITION BY age 再按 score 排序",
            ),
            self._seed(
                "SELECT id, user_id, total_price, status, "
                "ROW_NUMBER() OVER (PARTITION BY status ORDER BY total_price DESC) AS rn "
                "FROM t_orders ORDER BY status, rn",
                tags=["row_number", "partition_by"],
                desc="ROW_NUMBER PARTITION BY status 再按 total_price 排序",
            ),
            self._seed(
                "SELECT id, name, dept_id, salary, "
                "ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rn "
                "FROM t_employees ORDER BY dept_id, rn",
                tags=["row_number", "partition_by"],
                desc="ROW_NUMBER PARTITION BY dept_id 再按 salary 排序",
            ),
            self._seed(
                "SELECT id, user_id, metric_name, metric_value, "
                "ROW_NUMBER() OVER (PARTITION BY metric_name ORDER BY metric_value DESC) AS rn "
                "FROM t_metrics ORDER BY metric_name, rn",
                tags=["row_number", "partition_by"],
                desc="ROW_NUMBER PARTITION BY metric_name 再按 metric_value 排序",
            ),
            self._seed(
                "SELECT id, from_user, to_user, amount, "
                "ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn "
                "FROM t_transactions ORDER BY rn",
                tags=["row_number", "order_by"],
                desc="ROW_NUMBER 按 amount 降序排列",
            ),
            self._seed(
                "SELECT id, user_id, total_price, status, "
                "ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY total_price DESC) AS rn "
                "FROM t_orders ORDER BY user_id, rn",
                tags=["row_number", "partition_by"],
                desc="ROW_NUMBER PARTITION BY user_id 再按 total_price 排序",
            ),
        ]

    # ────────────────────────────────────────────────────────────
    # 2. RANK / DENSE_RANK
    # ────────────────────────────────────────────────────────────
    def _rank_dense_rank(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score, "
                "RANK() OVER (ORDER BY score DESC) AS rnk "
                "FROM t_users ORDER BY rnk",
                tags=["rank", "order_by"],
                desc="RANK 按 score 降序排列",
            ),
            self._seed(
                "SELECT id, username, score, "
                "DENSE_RANK() OVER (ORDER BY score DESC) AS drnk "
                "FROM t_users ORDER BY drnk",
                tags=["dense_rank", "order_by"],
                desc="DENSE_RANK 按 score 降序排列",
            ),
            self._seed(
                "SELECT id, username, score, "
                "RANK() OVER (ORDER BY score DESC) AS rnk, "
                "DENSE_RANK() OVER (ORDER BY score DESC) AS drnk "
                "FROM t_users ORDER BY rnk",
                tags=["rank", "dense_rank", "order_by"],
                desc="RANK 与 DENSE_RANK 对比",
            ),
            self._seed(
                "SELECT id, name, price, "
                "RANK() OVER (ORDER BY price DESC) AS rnk "
                "FROM t_products ORDER BY rnk",
                tags=["rank", "order_by"],
                desc="RANK 按 price 降序排列 (t_products)",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rnk "
                "FROM t_employees ORDER BY dept_id, rnk",
                tags=["rank", "partition_by"],
                desc="RANK PARTITION BY dept_id 再按 salary 排序",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS drnk "
                "FROM t_employees ORDER BY dept_id, drnk",
                tags=["dense_rank", "partition_by"],
                desc="DENSE_RANK PARTITION BY dept_id 再按 salary 排序",
            ),
            self._seed(
                "SELECT id, user_id, metric_name, metric_value, "
                "RANK() OVER (PARTITION BY user_id ORDER BY metric_value DESC) AS rnk "
                "FROM t_metrics ORDER BY user_id, rnk",
                tags=["rank", "partition_by"],
                desc="RANK PARTITION BY user_id 再按 metric_value 排序",
            ),
            self._seed(
                "SELECT id, from_user, amount, tx_type, "
                "RANK() OVER (PARTITION BY tx_type ORDER BY amount DESC) AS rnk "
                "FROM t_transactions ORDER BY tx_type, rnk",
                tags=["rank", "partition_by"],
                desc="RANK PARTITION BY tx_type 再按 amount 排序",
            ),
            self._seed(
                "SELECT id, from_user, amount, tx_type, "
                "DENSE_RANK() OVER (PARTITION BY tx_type ORDER BY amount DESC) AS drnk "
                "FROM t_transactions ORDER BY tx_type, drnk",
                tags=["dense_rank", "partition_by"],
                desc="DENSE_RANK PARTITION BY tx_type 再按 amount 排序",
            ),
            self._seed(
                "SELECT id, user_id, total_price, status, "
                "RANK() OVER (PARTITION BY status ORDER BY total_price DESC) AS rnk "
                "FROM t_orders ORDER BY status, rnk",
                tags=["rank", "partition_by"],
                desc="RANK PARTITION BY status 再按 total_price 排序",
            ),
        ]

    # ────────────────────────────────────────────────────────────
    # 3. NTILE
    # ────────────────────────────────────────────────────────────
    def _ntile(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score, "
                "NTILE(2) OVER (ORDER BY score DESC) AS bucket "
                "FROM t_users ORDER BY score DESC",
                tags=["ntile"],
                desc="NTILE(2) 按 score 分两桶",
            ),
            self._seed(
                "SELECT id, username, score, "
                "NTILE(3) OVER (ORDER BY score DESC) AS bucket "
                "FROM t_users ORDER BY score DESC",
                tags=["ntile"],
                desc="NTILE(3) 按 score 分三桶",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "NTILE(4) OVER (PARTITION BY dept_id ORDER BY salary DESC) AS bucket "
                "FROM t_employees ORDER BY dept_id, bucket",
                tags=["ntile", "partition_by"],
                desc="NTILE(4) PARTITION BY dept_id 再按 salary 分桶",
            ),
            self._seed(
                "SELECT id, user_id, total_price, "
                "NTILE(2) OVER (ORDER BY total_price DESC) AS bucket "
                "FROM t_orders ORDER BY bucket",
                tags=["ntile"],
                desc="NTILE(2) 按 total_price 分两桶",
            ),
            self._seed(
                "SELECT id, user_id, metric_value, "
                "NTILE(3) OVER (ORDER BY metric_value DESC) AS bucket "
                "FROM t_metrics ORDER BY bucket",
                tags=["ntile"],
                desc="NTILE(3) 按 metric_value 分三桶",
            ),
            self._seed(
                "SELECT id, from_user, amount, "
                "NTILE(4) OVER (ORDER BY amount DESC) AS bucket "
                "FROM t_transactions ORDER BY bucket",
                tags=["ntile"],
                desc="NTILE(4) 按 amount 分四桶",
            ),
        ]

    # ────────────────────────────────────────────────────────────
    # 4. SUM / AVG OVER
    # ────────────────────────────────────────────────────────────
    def _sum_avg_over(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score, "
                "SUM(score) OVER () AS total_score "
                "FROM t_users ORDER BY id",
                tags=["sum", "over_empty"],
                desc="SUM(score) OVER () 全表汇总",
            ),
            self._seed(
                "SELECT id, username, score, "
                "AVG(score) OVER () AS avg_score "
                "FROM t_users ORDER BY id",
                tags=["avg", "over_empty"],
                desc="AVG(score) OVER () 全表平均",
            ),
            self._seed(
                "SELECT id, username, age, score, "
                "SUM(score) OVER (PARTITION BY age) AS age_total "
                "FROM t_users ORDER BY age, id",
                tags=["sum", "partition_by"],
                desc="SUM(score) PARTITION BY age 分组汇总",
            ),
            self._seed(
                "SELECT id, username, age, score, "
                "AVG(score) OVER (PARTITION BY age) AS age_avg "
                "FROM t_users ORDER BY age, id",
                tags=["avg", "partition_by"],
                desc="AVG(score) PARTITION BY age 分组平均",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "SUM(salary) OVER (PARTITION BY dept_id) AS dept_total "
                "FROM t_employees ORDER BY dept_id, id",
                tags=["sum", "partition_by"],
                desc="SUM(salary) PARTITION BY dept_id 部门薪资总额",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "AVG(salary) OVER (PARTITION BY dept_id) AS dept_avg "
                "FROM t_employees ORDER BY dept_id, id",
                tags=["avg", "partition_by"],
                desc="AVG(salary) PARTITION BY dept_id 部门平均薪资",
            ),
            self._seed(
                "SELECT id, user_id, total_price, "
                "SUM(total_price) OVER (ORDER BY id) AS running_total "
                "FROM t_orders ORDER BY id",
                tags=["sum", "running"],
                desc="SUM(total_price) ORDER BY id 累计汇总",
            ),
            self._seed(
                "SELECT id, from_user, amount, "
                "SUM(amount) OVER (PARTITION BY tx_type) AS type_total "
                "FROM t_transactions ORDER BY tx_type, id",
                tags=["sum", "partition_by"],
                desc="SUM(amount) PARTITION BY tx_type 按交易类型汇总",
            ),
        ]

    # ────────────────────────────────────────────────────────────
    # 5. COUNT OVER
    # ────────────────────────────────────────────────────────────
    def _count_over(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score, "
                "COUNT(*) OVER () AS total_count "
                "FROM t_users ORDER BY id",
                tags=["count", "over_empty"],
                desc="COUNT(*) OVER () 全表计数",
            ),
            self._seed(
                "SELECT id, username, age, "
                "COUNT(*) OVER (PARTITION BY age) AS age_count "
                "FROM t_users ORDER BY age, id",
                tags=["count", "partition_by"],
                desc="COUNT(*) PARTITION BY age 按年龄计数",
            ),
            self._seed(
                "SELECT id, user_id, total_price, status, "
                "COUNT(*) OVER (PARTITION BY status) AS status_count "
                "FROM t_orders ORDER BY status, id",
                tags=["count", "partition_by"],
                desc="COUNT(*) PARTITION BY status 按订单状态计数",
            ),
            self._seed(
                "SELECT id, name, dept_id, "
                "COUNT(*) OVER (PARTITION BY dept_id) AS dept_count "
                "FROM t_employees ORDER BY dept_id, id",
                tags=["count", "partition_by"],
                desc="COUNT(*) PARTITION BY dept_id 按部门计数",
            ),
            self._seed(
                "SELECT id, user_id, metric_name, "
                "COUNT(metric_value) OVER (PARTITION BY metric_name) AS name_count "
                "FROM t_metrics ORDER BY metric_name, id",
                tags=["count", "partition_by"],
                desc="COUNT(metric_value) PARTITION BY metric_name 按指标名计数(排除NULL)",
            ),
            self._seed(
                "SELECT id, from_user, amount, tx_type, "
                "COUNT(*) OVER (PARTITION BY tx_type) AS type_count "
                "FROM t_transactions ORDER BY tx_type, id",
                tags=["count", "partition_by"],
                desc="COUNT(*) PARTITION BY tx_type 按交易类型计数",
            ),
            self._seed(
                "SELECT id, username, score, "
                "COUNT(score) OVER () AS non_null_score_count "
                "FROM t_users ORDER BY id",
                tags=["count", "over_empty"],
                desc="COUNT(score) OVER () 非 NULL score 计数",
            ),
            self._seed(
                "SELECT id, user_id, total_price, status, "
                "COUNT(*) OVER (PARTITION BY user_id) AS user_order_count "
                "FROM t_orders ORDER BY user_id, id",
                tags=["count", "partition_by"],
                desc="COUNT(*) PARTITION BY user_id 按用户统计订单数",
            ),
        ]
