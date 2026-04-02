"""数值函数方言差异模板 — 测试 SQLite/Oracle 数值函数行为差异。

覆盖差异点：
- ROUND: SQLite 使用 banker's rounding（round-half-even），Oracle 使用标准舍入
- TRUNC: 两者截断行为基本一致，但参数处理有差异
- MOD vs REMAINDER: Oracle 同时支持 MOD 和 REMAINDER，后者遵循 IEEE 754
- CEIL/FLOOR: 对负数的行为可暴露方言差异
- LOG/LN: SQLite 用 LN()，Oracle 也支持 LOG() 但参数含义不同
- 类型提升: 整数与浮点混合运算的结果类型差异
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class NumericFuncsTemplate(SeedTemplate):
    """数值函数方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "numeric_funcs"

    @property
    def description(self) -> str:
        return "数值函数方言差异测试（ROUND/TRUNC/MOD/ABS/CEIL/FLOOR/POWER/SQRT/类型提升）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._round_queries())
        seeds.extend(self._trunc_queries())
        seeds.extend(self._mod_queries())
        seeds.extend(self._abs_queries())
        seeds.extend(self._ceil_floor_queries())
        seeds.extend(self._power_sqrt_queries())
        seeds.extend(self._type_promotion_queries())
        return seeds

    # ── ROUND (~12) ─────────────────────────────────────
    def _round_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, name, ROUND(price) AS rounded_price "
                "FROM t_products ORDER BY id",
                tags=["round", "round_no_precision"],
                desc="ROUND 无精度参数 — banker's vs standard rounding",
            ),
            self._seed(
                "SELECT id, name, ROUND(price, 1) AS price_1dp "
                "FROM t_products ORDER BY id",
                tags=["round", "round_1dp"],
                desc="ROUND 保留 1 位小数",
            ),
            self._seed(
                "SELECT id, name, ROUND(price, 2) AS price_2dp "
                "FROM t_products ORDER BY id",
                tags=["round", "round_2dp"],
                desc="ROUND 保留 2 位小数",
            ),
            self._seed(
                "SELECT id, name, ROUND(price, -1) AS price_tens "
                "FROM t_products ORDER BY id",
                tags=["round", "round_negative_precision"],
                desc="ROUND 负精度 — 截断到十位",
            ),
            self._seed(
                "SELECT id, username, ROUND(score) AS rounded_score "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["round", "round_score"],
                desc="ROUND 对 score 列",
            ),
            self._seed(
                "SELECT id, username, ROUND(score, 1) AS score_1dp "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["round", "round_score_1dp"],
                desc="ROUND(score, 1) 保留 1 位小数",
            ),
            self._seed(
                "SELECT id, username, ROUND(score, 2) AS score_2dp "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["round", "round_score_2dp"],
                desc="ROUND(score, 2) 保留 2 位小数",
            ),
            self._seed(
                "SELECT id, username, ROUND(score, -1) AS score_tens "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["round", "round_score_neg"],
                desc="ROUND 负精度对 score",
            ),
            self._seed(
                "SELECT id, name, ROUND(salary) AS rounded_salary "
                "FROM t_employees WHERE salary IS NOT NULL ORDER BY id",
                tags=["round", "round_salary"],
                desc="ROUND 对 salary",
            ),
            self._seed(
                "SELECT id, ROUND(amount) AS rounded_amount "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY id",
                tags=["round", "round_amount"],
                desc="ROUND 对 amount",
            ),
            self._seed(
                "SELECT id, metric_name, ROUND(metric_value) AS rounded_val "
                "FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id",
                tags=["round", "round_metric_value"],
                desc="ROUND 对 metric_value",
            ),
            self._seed(
                "SELECT id, metric_name, ROUND(metric_value, 2) AS val_2dp "
                "FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id",
                tags=["round", "round_metric_2dp"],
                desc="ROUND(metric_value, 2) 高精度列",
            ),
        ]

    # ── TRUNC (~8) ──────────────────────────────────────
    def _trunc_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, TRUNC(score) AS score_truncated "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["trunc", "trunc_no_precision"],
                desc="TRUNC 无精度参数",
            ),
            self._seed(
                "SELECT id, name, TRUNC(price) AS price_truncated "
                "FROM t_products ORDER BY id",
                tags=["trunc", "trunc_price"],
                desc="TRUNC 对 price",
            ),
            self._seed(
                "SELECT id, username, TRUNC(score, 1) AS score_1dp "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["trunc", "trunc_1dp"],
                desc="TRUNC 保留 1 位小数",
            ),
            self._seed(
                "SELECT id, name, TRUNC(price, 1) AS price_1dp "
                "FROM t_products ORDER BY id",
                tags=["trunc", "trunc_price_1dp"],
                desc="TRUNC(price, 1)",
            ),
            self._seed(
                "SELECT id, username, TRUNC(score, -1) AS score_tens "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["trunc", "trunc_negative"],
                desc="TRUNC 负精度",
            ),
            self._seed(
                "SELECT id, name, TRUNC(price, -1) AS price_tens "
                "FROM t_products ORDER BY id",
                tags=["trunc", "trunc_price_neg"],
                desc="TRUNC(price, -1) 截断到十位",
            ),
            self._seed(
                "SELECT id, ROUND(amount) AS round_amt, TRUNC(amount) AS trunc_amt "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY id",
                tags=["trunc", "round_vs_trunc"],
                desc="ROUND vs TRUNC 对比 — banker's 差异可见",
            ),
            self._seed(
                "SELECT id, metric_name, TRUNC(metric_value, 3) AS val_trunc_3dp "
                "FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id",
                tags=["trunc", "trunc_high_precision"],
                desc="TRUNC 高精度列截断",
            ),
        ]

    # ── MOD (~8) ────────────────────────────────────────
    def _mod_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, age, MOD(age, 10) AS age_mod10 "
                "FROM t_users WHERE age IS NOT NULL ORDER BY id",
                tags=["mod", "mod_age_10"],
                desc="MOD(age, 10)",
            ),
            self._seed(
                "SELECT id, username, age, MOD(age, 3) AS age_mod3 "
                "FROM t_users WHERE age IS NOT NULL ORDER BY id",
                tags=["mod", "mod_age_3"],
                desc="MOD(age, 3)",
            ),
            self._seed(
                "SELECT id, username, age, MOD(age, 2) AS age_mod2 "
                "FROM t_users WHERE age IS NOT NULL ORDER BY id",
                tags=["mod", "mod_age_2"],
                desc="MOD(age, 2) — 奇偶判断",
            ),
            self._seed(
                "SELECT id, user_id, quantity, MOD(quantity, 10) AS qty_mod10 "
                "FROM t_orders ORDER BY id",
                tags=["mod", "mod_quantity_10"],
                desc="MOD(quantity, 10)",
            ),
            self._seed(
                "SELECT id, name, stock, MOD(stock, 3) AS stock_mod3 "
                "FROM t_products WHERE stock IS NOT NULL ORDER BY id",
                tags=["mod", "mod_stock_3"],
                desc="MOD(stock, 3)",
            ),
            self._seed(
                "SELECT id, name, stock, MOD(stock, 2) AS stock_mod2 "
                "FROM t_products WHERE stock IS NOT NULL ORDER BY id",
                tags=["mod", "mod_stock_2"],
                desc="MOD(stock, 2) — 奇偶判断",
            ),
            self._seed(
                "SELECT id, name, price, MOD(price, 10) AS price_mod10 "
                "FROM t_products ORDER BY id",
                tags=["mod", "mod_price_10"],
                desc="MOD 对 DECIMAL 列 — Oracle REMAINDER vs MOD 差异",
            ),
            self._seed(
                "SELECT id, amount, MOD(amount, 100) AS amt_mod100 "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY id",
                tags=["mod", "mod_amount_100"],
                desc="MOD 对 DECIMAL amount",
            ),
        ]

    # ── ABS (~6) ────────────────────────────────────────
    def _abs_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, metric_name, metric_value, ABS(metric_value) AS abs_val "
                "FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id",
                tags=["abs", "abs_metric_value"],
                desc="ABS 对 metric_value（含负数）",
            ),
            self._seed(
                "SELECT id, amount, ABS(amount) AS abs_amount "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY id",
                tags=["abs", "abs_amount"],
                desc="ABS 对 amount（含负数）",
            ),
            self._seed(
                "SELECT id, score, ABS(score) AS abs_score "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["abs", "abs_score"],
                desc="ABS 对 score",
            ),
            self._seed(
                "SELECT id, name, price, ABS(price - 100) AS price_diff "
                "FROM t_products ORDER BY id",
                tags=["abs", "abs_difference"],
                desc="ABS 计算差值 — 类型提升差异",
            ),
            self._seed(
                "SELECT id, salary, ABS(salary - 5000) AS salary_gap "
                "FROM t_employees WHERE salary IS NOT NULL ORDER BY id",
                tags=["abs", "abs_salary_gap"],
                desc="ABS 计算薪资差距",
            ),
            self._seed(
                "SELECT id, metric_value, ABS(metric_value), ROUND(ABS(metric_value), 2) AS rounded_abs "
                "FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id",
                tags=["abs", "abs_nested_round"],
                desc="ABS + ROUND 嵌套 — 类型提升链",
            ),
        ]

    # ── CEIL / FLOOR (~6) ───────────────────────────────
    def _ceil_floor_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, score, CEIL(score) AS score_ceil, FLOOR(score) AS score_floor "
                "FROM t_users WHERE score IS NOT NULL ORDER BY id",
                tags=["ceil_floor", "ceil_floor_score"],
                desc="CEIL/FLOOR 对 score — 正数",
            ),
            self._seed(
                "SELECT id, name, price, CEIL(price) AS price_ceil, FLOOR(price) AS price_floor "
                "FROM t_products ORDER BY id",
                tags=["ceil_floor", "ceil_floor_price"],
                desc="CEIL/FLOOR 对 price",
            ),
            self._seed(
                "SELECT id, name, weight_kg, CEIL(weight_kg) AS weight_ceil, FLOOR(weight_kg) AS weight_floor "
                "FROM t_products WHERE weight_kg IS NOT NULL ORDER BY id",
                tags=["ceil_floor", "ceil_floor_weight"],
                desc="CEIL/FLOOR 对 FLOAT 列",
            ),
            self._seed(
                "SELECT id, metric_name, metric_value, "
                "CEIL(metric_value) AS val_ceil, FLOOR(metric_value) AS val_floor "
                "FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id",
                tags=["ceil_floor", "ceil_floor_metric"],
                desc="CEIL/FLOOR 对 metric_value（含负数测试）",
            ),
            self._seed(
                "SELECT id, amount, "
                "CEIL(amount) AS amt_ceil, FLOOR(amount) AS amt_floor "
                "FROM t_transactions WHERE amount IS NOT NULL ORDER BY id",
                tags=["ceil_floor", "ceil_floor_amount"],
                desc="CEIL/FLOOR 对 amount（含负数）",
            ),
            self._seed(
                "SELECT id, salary, "
                "CEIL(salary / 1000) AS salary_k_ceil, FLOOR(salary / 1000) AS salary_k_floor "
                "FROM t_employees WHERE salary IS NOT NULL ORDER BY id",
                tags=["ceil_floor", "ceil_floor_expression"],
                desc="CEIL/FLOOR 对除法表达式 — 类型提升差异",
            ),
        ]

    # ── POWER / SQRT (~4) ───────────────────────────────
    def _power_sqrt_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, name, price, ROUND(POWER(price, 2), 4) AS price_squared "
                "FROM t_products WHERE price IS NOT NULL ORDER BY id",
                tags=["power", "power_price_2"],
                desc="POWER(price, 2) 平方",
            ),
            self._seed(
                "SELECT id, name, price, ROUND(SQRT(price), 4) AS price_sqrt "
                "FROM t_products WHERE price >= 0 ORDER BY id",
                tags=["sqrt", "sqrt_price"],
                desc="SQRT(price) 平方根",
            ),
            self._seed(
                "SELECT id, metric_name, metric_value, "
                "ROUND(POWER(metric_value, 2), 4) AS val_squared "
                "FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id",
                tags=["power", "power_metric"],
                desc="POWER 对 metric_value — 负数平方结果一致",
            ),
            self._seed(
                "SELECT id, metric_name, metric_value, "
                "ROUND(SQRT(ABS(metric_value)), 4) AS sqrt_abs_val "
                "FROM t_metrics WHERE metric_value IS NOT NULL ORDER BY id",
                tags=["sqrt", "sqrt_abs_metric"],
                desc="SQRT(ABS(x)) 嵌套 — 保证非负输入",
            ),
        ]

    # ── 类型提升 (~6) ───────────────────────────────────
    def _type_promotion_queries(self) -> List[SeedSQL]:
        return [
            self._seed(
                "SELECT id, username, age, score, "
                "age + score AS int_plus_decimal "
                "FROM t_users WHERE age IS NOT NULL AND score IS NOT NULL ORDER BY id",
                tags=["type_promotion", "int_plus_decimal"],
                desc="INTEGER + DECIMAL 混合运算",
            ),
            self._seed(
                "SELECT id, name, price, stock, "
                "price * stock AS total_value "
                "FROM t_products WHERE stock IS NOT NULL ORDER BY id",
                tags=["type_promotion", "decimal_times_int"],
                desc="DECIMAL * INTEGER 混合运算",
            ),
            self._seed(
                "SELECT id, username, height, "
                "CAST(height AS INTEGER) AS height_int, "
                "height + 0.5 AS height_plus_half "
                "FROM t_users WHERE height IS NOT NULL ORDER BY id",
                tags=["type_promotion", "float_cast_arithmetic"],
                desc="FLOAT CAST 和混合运算",
            ),
            self._seed(
                "SELECT id, name, weight_kg, "
                "CAST(weight_kg AS INTEGER) AS weight_int, "
                "weight_kg + 0.01 AS weight_plus "
                "FROM t_products WHERE weight_kg IS NOT NULL ORDER BY id",
                tags=["type_promotion", "float_cast_and_add"],
                desc="FLOAT 列 CAST 和加法",
            ),
            self._seed(
                "SELECT id, user_id, total_price, quantity, "
                "CAST(total_price AS INTEGER) AS price_int, "
                "total_price / quantity AS unit_price "
                "FROM t_orders WHERE quantity > 0 ORDER BY id",
                tags=["type_promotion", "decimal_division"],
                desc="DECIMAL 除法 — 整数除法 vs 浮点除法差异",
            ),
            self._seed(
                "SELECT id, username, age, score, "
                "CAST(age AS REAL) AS age_real, "
                "CAST(score AS INTEGER) AS score_int, "
                "CAST(age AS REAL) + score AS mixed_result "
                "FROM t_users WHERE age IS NOT NULL AND score IS NOT NULL ORDER BY id",
                tags=["type_promotion", "explicit_cast_arithmetic"],
                desc="显式 CAST 混合运算 — REAL/INTEGER 交叉",
            ),
        ]
