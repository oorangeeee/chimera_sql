"""窗口函数高级差异种子模板 — LEAD/LAG, FIRST_VALUE/LAST_VALUE, Frame, PERCENT_RANK, NULL, CUME_DIST。"""

from __future__ import annotations

from typing import List

from src.testbed.seed_templates.base import SchemaMetadata, SeedSQL, SeedTemplate
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WindowAdvancedTemplate(SeedTemplate):
    """高级窗口函数方言差异模板。"""

    @property
    def domain(self) -> str:
        return "window_advanced"

    @property
    def description(self) -> str:
        return (
            "高级窗口函数方言差异: LEAD/LAG, FIRST_VALUE/LAST_VALUE, "
            "Frame 子句, PERCENT_RANK, NULL 排序, CUME_DIST"
        )

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []

        # ── 1. LEAD / LAG (~10) ─────────────────────────────────
        seeds.extend(self._lead_lag())

        # ── 2. FIRST_VALUE / LAST_VALUE (~6) ────────────────────
        seeds.extend(self._first_last_value())

        # ── 3. Frame clauses (~8) ───────────────────────────────
        seeds.extend(self._frame_clauses())

        # ── 4. PERCENT_RANK (~4) ────────────────────────────────
        seeds.extend(self._percent_rank())

        # ── 5. NULL in windows (~6) ─────────────────────────────
        seeds.extend(self._null_in_windows())

        # ── 6. CUME_DIST (~6) ───────────────────────────────────
        seeds.extend(self._cume_dist())

        logger.info("WindowAdvancedTemplate 生成 %d 条种子 SQL", len(seeds))
        return seeds

    # ────────────────────────────────────────────────────────────
    # 1. LEAD / LAG
    # ────────────────────────────────────────────────────────────
    def _lead_lag(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, user_id, total_price, "
                "LEAD(total_price, 1) OVER (PARTITION BY user_id ORDER BY id) AS next_price "
                "FROM t_orders ORDER BY user_id, id",
                tags=["lead", "partition_by"],
                desc="LEAD(total_price, 1) PARTITION BY user_id 查看下一笔订单金额",
            ),
            self._seed(
                "SELECT id, user_id, total_price, "
                "LAG(total_price, 1) OVER (PARTITION BY user_id ORDER BY id) AS prev_price "
                "FROM t_orders ORDER BY user_id, id",
                tags=["lag", "partition_by"],
                desc="LAG(total_price, 1) PARTITION BY user_id 查看上一笔订单金额",
            ),
            self._seed(
                "SELECT id, user_id, metric_value, "
                "LEAD(metric_value, 1) OVER (PARTITION BY user_id ORDER BY id) AS next_val "
                "FROM t_metrics ORDER BY user_id, id",
                tags=["lead", "partition_by"],
                desc="LEAD(metric_value, 1) PARTITION BY user_id 查看下一条指标值",
            ),
            self._seed(
                "SELECT id, user_id, metric_value, "
                "LAG(metric_value, 1) OVER (PARTITION BY user_id ORDER BY id) AS prev_val "
                "FROM t_metrics ORDER BY user_id, id",
                tags=["lag", "partition_by"],
                desc="LAG(metric_value, 1) PARTITION BY user_id 查看上一条指标值",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "LEAD(salary, 1) OVER (PARTITION BY dept_id ORDER BY salary DESC) AS next_salary "
                "FROM t_employees ORDER BY dept_id, salary DESC",
                tags=["lead", "partition_by"],
                desc="LEAD(salary, 1) PARTITION BY dept_id 按薪资查看下一位",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "LAG(salary, 1) OVER (PARTITION BY dept_id ORDER BY salary DESC) AS prev_salary "
                "FROM t_employees ORDER BY dept_id, salary DESC",
                tags=["lag", "partition_by"],
                desc="LAG(salary, 1) PARTITION BY dept_id 按薪资查看上一位",
            ),
            self._seed(
                "SELECT id, from_user, amount, "
                "LEAD(amount, 2) OVER (ORDER BY id) AS amount_ahead_2 "
                "FROM t_transactions ORDER BY id",
                tags=["lead", "offset_2"],
                desc="LEAD(amount, 2) 查看往后第二笔交易金额",
            ),
            self._seed(
                "SELECT id, from_user, amount, "
                "LAG(amount, 2) OVER (ORDER BY id) AS amount_behind_2 "
                "FROM t_transactions ORDER BY id",
                tags=["lag", "offset_2"],
                desc="LAG(amount, 2) 查看往前第二笔交易金额",
            ),
            self._seed(
                "SELECT id, user_id, metric_value, "
                "LEAD(metric_value, 2) OVER (PARTITION BY metric_name ORDER BY id) AS val_ahead_2 "
                "FROM t_metrics ORDER BY metric_name, id",
                tags=["lead", "partition_by", "offset_2"],
                desc="LEAD(metric_value, 2) PARTITION BY metric_name 查看后第二条指标值",
            ),
            self._seed(
                "SELECT id, from_user, amount, tx_type, "
                "LAG(amount, 1) OVER (PARTITION BY tx_type ORDER BY id) AS prev_type_amount "
                "FROM t_transactions ORDER BY tx_type, id",
                tags=["lag", "partition_by"],
                desc="LAG(amount, 1) PARTITION BY tx_type 按交易类型查看上笔金额",
            ),
        ]

    # ────────────────────────────────────────────────────────────
    # 2. FIRST_VALUE / LAST_VALUE
    # ────────────────────────────────────────────────────────────
    def _first_last_value(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "FIRST_VALUE(salary) OVER (PARTITION BY dept_id ORDER BY salary DESC) AS max_salary "
                "FROM t_employees ORDER BY dept_id, id",
                tags=["first_value", "partition_by"],
                desc="FIRST_VALUE(salary) PARTITION BY dept_id 取部门最高薪",
            ),
            self._seed(
                "SELECT id, username, score, "
                "FIRST_VALUE(score) OVER (ORDER BY score DESC) AS top_score "
                "FROM t_users ORDER BY score DESC",
                tags=["first_value", "order_by"],
                desc="FIRST_VALUE(score) ORDER BY score DESC 取最高分",
            ),
            self._seed(
                "SELECT id, user_id, total_price, "
                "LAST_VALUE(total_price) OVER (PARTITION BY user_id ORDER BY id "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_price "
                "FROM t_orders ORDER BY user_id, id",
                tags=["last_value", "rows_between", "partition_by"],
                desc="LAST_VALUE(total_price) 带 ROWS BETWEEN 取用户最后订单金额",
            ),
            self._seed(
                "SELECT id, user_id, metric_value, "
                "FIRST_VALUE(metric_value) OVER (PARTITION BY user_id ORDER BY id) AS first_val "
                "FROM t_metrics ORDER BY user_id, id",
                tags=["first_value", "partition_by"],
                desc="FIRST_VALUE(metric_value) PARTITION BY user_id 取用户首条指标值",
            ),
            self._seed(
                "SELECT id, from_user, amount, tx_type, "
                "LAST_VALUE(amount) OVER (PARTITION BY tx_type ORDER BY id "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amount "
                "FROM t_transactions ORDER BY tx_type, id",
                tags=["last_value", "rows_between", "partition_by"],
                desc="LAST_VALUE(amount) 带 ROWS BETWEEN 按交易类型取最后一笔金额",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "FIRST_VALUE(salary) OVER (PARTITION BY dept_id ORDER BY salary) AS min_salary "
                "FROM t_employees ORDER BY dept_id, id",
                tags=["first_value", "partition_by"],
                desc="FIRST_VALUE(salary) PARTITION BY dept_id ORDER BY salary ASC 取部门最低薪",
            ),
        ]

    # ────────────────────────────────────────────────────────────
    # 3. Frame clauses
    # ────────────────────────────────────────────────────────────
    def _frame_clauses(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score, "
                "SUM(score) OVER (ORDER BY id "
                "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS neighbor_sum "
                "FROM t_users ORDER BY id",
                tags=["sum", "rows_between", "running"],
                desc="SUM(score) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING 邻近求和",
            ),
            self._seed(
                "SELECT id, username, score, "
                "AVG(score) OVER (ORDER BY id "
                "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS neighbor_avg "
                "FROM t_users ORDER BY id",
                tags=["avg", "rows_between"],
                desc="AVG(score) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING 邻近均值",
            ),
            self._seed(
                "SELECT id, name, salary, "
                "SUM(salary) OVER (ORDER BY id "
                "ROWS UNBOUNDED PRECEDING) AS running_total "
                "FROM t_employees ORDER BY id",
                tags=["sum", "rows_unbounded", "running"],
                desc="SUM(salary) ROWS UNBOUNDED PRECEDING 累计求和",
            ),
            self._seed(
                "SELECT id, user_id, total_price, "
                "SUM(total_price) OVER (ORDER BY id "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total "
                "FROM t_orders ORDER BY id",
                tags=["sum", "rows_between", "running"],
                desc="SUM(total_price) ROWS UNBOUNDED PRECEDING AND CURRENT ROW 累计",
            ),
            self._seed(
                "SELECT id, user_id, metric_value, "
                "AVG(metric_value) OVER (ORDER BY id "
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg "
                "FROM t_metrics ORDER BY id",
                tags=["avg", "rows_between", "moving"],
                desc="AVG(metric_value) ROWS BETWEEN 2 PRECEDING AND CURRENT ROW 移动平均",
            ),
            self._seed(
                "SELECT id, from_user, amount, "
                "SUM(amount) OVER (ORDER BY id "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS grand_total "
                "FROM t_transactions ORDER BY id",
                tags=["sum", "rows_between", "full_frame"],
                desc="SUM(amount) ROWS 全范围窗口，每行均为总和",
            ),
            self._seed(
                "SELECT id, username, score, "
                "SUM(score) OVER (ORDER BY id "
                "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_sum "
                "FROM t_users ORDER BY id",
                tags=["sum", "range_between", "running"],
                desc="SUM(score) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
            ),
            self._seed(
                "SELECT id, name, salary, "
                "AVG(salary) OVER (ORDER BY salary "
                "RANGE BETWEEN 1000 PRECEDING AND 1000 FOLLOWING) AS range_avg "
                "FROM t_employees ORDER BY salary",
                tags=["avg", "range_between"],
                desc="AVG(salary) RANGE BETWEEN 1000 PRECEDING AND 1000 FOLLOWING 范围均值",
            ),
        ]

    # ────────────────────────────────────────────────────────────
    # 4. PERCENT_RANK
    # ────────────────────────────────────────────────────────────
    def _percent_rank(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score, "
                "PERCENT_RANK() OVER (ORDER BY score DESC) AS pct_rank "
                "FROM t_users ORDER BY pct_rank",
                tags=["percent_rank", "order_by"],
                desc="PERCENT_RANK 按 score 降序计算百分位排名",
            ),
            self._seed(
                "SELECT id, name, price, "
                "PERCENT_RANK() OVER (ORDER BY price) AS pct_rank "
                "FROM t_products ORDER BY pct_rank",
                tags=["percent_rank", "order_by"],
                desc="PERCENT_RANK 按 price 升序计算百分位排名",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "PERCENT_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS pct_rank "
                "FROM t_employees ORDER BY dept_id, pct_rank",
                tags=["percent_rank", "partition_by"],
                desc="PERCENT_RANK PARTITION BY dept_id 部门内薪资百分位排名",
            ),
            self._seed(
                "SELECT id, from_user, amount, "
                "PERCENT_RANK() OVER (ORDER BY amount) AS pct_rank "
                "FROM t_transactions ORDER BY pct_rank",
                tags=["percent_rank", "order_by"],
                desc="PERCENT_RANK 按 amount 升序计算百分位排名",
            ),
        ]

    # ────────────────────────────────────────────────────────────
    # 5. NULL in windows
    # ────────────────────────────────────────────────────────────
    def _null_in_windows(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score, "
                "ROW_NUMBER() OVER (ORDER BY score) AS rn "
                "FROM t_users ORDER BY rn",
                tags=["row_number", "nullable_order"],
                desc="ROW_NUMBER ORDER BY nullable score (NULL 排序行为差异)",
            ),
            self._seed(
                "SELECT id, name, dept_id, salary, "
                "RANK() OVER (ORDER BY salary) AS rnk "
                "FROM t_employees ORDER BY rnk",
                tags=["rank", "nullable_order"],
                desc="RANK ORDER BY nullable salary (NULL 排序行为差异)",
            ),
            self._seed(
                "SELECT id, user_id, metric_value, "
                "DENSE_RANK() OVER (ORDER BY metric_value DESC) AS drnk "
                "FROM t_metrics ORDER BY drnk",
                tags=["dense_rank", "nullable_order"],
                desc="DENSE_RANK ORDER BY nullable metric_value (NULL 排序行为差异)",
            ),
            self._seed(
                "SELECT id, user_id, total_price, status, "
                "ROW_NUMBER() OVER (PARTITION BY status ORDER BY total_price) AS rn "
                "FROM t_orders ORDER BY rn",
                tags=["row_number", "nullable_order", "partition_by"],
                desc="ROW_NUMBER ORDER BY nullable total_price, PARTITION BY nullable status",
            ),
            self._seed(
                "SELECT id, from_user, amount, "
                "SUM(amount) OVER (ORDER BY amount "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum "
                "FROM t_transactions ORDER BY running_sum",
                tags=["sum", "rows_between", "nullable_order"],
                desc="SUM OVER ORDER BY nullable amount 累计 (NULL 排序行为差异)",
            ),
            self._seed(
                "SELECT id, user_id, metric_name, metric_value, "
                "LEAD(metric_value, 1) OVER (PARTITION BY metric_name ORDER BY id) AS next_val "
                "FROM t_metrics ORDER BY metric_name, id",
                tags=["lead", "nullable_order", "partition_by"],
                desc="LEAD(metric_value, 1) ORDER BY id, metric_value 可能为 NULL",
            ),
        ]

    # ────────────────────────────────────────────────────────────
    # 6. CUME_DIST
    # ────────────────────────────────────────────────────────────
    def _cume_dist(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score, "
                "CUME_DIST() OVER (ORDER BY score DESC) AS cd "
                "FROM t_users ORDER BY cd",
                tags=["cume_dist", "order_by"],
                desc="CUME_DIST 按 score 降序计算累积分布",
            ),
            self._seed(
                "SELECT id, name, salary, dept_id, "
                "CUME_DIST() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS cd "
                "FROM t_employees ORDER BY dept_id, cd",
                tags=["cume_dist", "partition_by"],
                desc="CUME_DIST PARTITION BY dept_id 部门内薪资累积分布",
            ),
            self._seed(
                "SELECT id, user_id, metric_value, "
                "CUME_DIST() OVER (ORDER BY metric_value) AS cd "
                "FROM t_metrics ORDER BY cd",
                tags=["cume_dist", "order_by"],
                desc="CUME_DIST 按 metric_value 升序计算累积分布",
            ),
            self._seed(
                "SELECT id, from_user, amount, "
                "CUME_DIST() OVER (ORDER BY amount DESC) AS cd "
                "FROM t_transactions ORDER BY cd",
                tags=["cume_dist", "order_by"],
                desc="CUME_DIST 按 amount 降序计算累积分布",
            ),
            self._seed(
                "SELECT id, name, salary, "
                "CUME_DIST() OVER (ORDER BY salary) AS cd, "
                "PERCENT_RANK() OVER (ORDER BY salary) AS pct "
                "FROM t_employees ORDER BY cd",
                tags=["cume_dist", "percent_rank", "order_by"],
                desc="CUME_DIST 与 PERCENT_RANK 对比按 salary 升序",
            ),
            self._seed(
                "SELECT id, username, score, "
                "CUME_DIST() OVER (ORDER BY score) AS cd, "
                "PERCENT_RANK() OVER (ORDER BY score) AS pct "
                "FROM t_users ORDER BY cd",
                tags=["cume_dist", "percent_rank", "order_by"],
                desc="CUME_DIST 与 PERCENT_RANK 对比按 score 升序",
            ),
        ]
