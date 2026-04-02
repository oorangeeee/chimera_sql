"""表达式/运算标准模板 — 验证标准 SQL 表达式正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class StandardExpressionsTemplate(SeedTemplate):

    @property
    def domain(self) -> str:
        return "expressions"

    @property
    def description(self) -> str:
        return "标准SQL表达式测试（算术/字符串/嵌套函数/逻辑组合）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._arithmetic())
        seeds.extend(self._string_ops())
        seeds.extend(self._logical())
        return seeds

    def _arithmetic(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, score * 2 AS doubled FROM t_users WHERE score IS NOT NULL ORDER BY id", tags=["arith", "double_score"], desc="乘法"),
            self._seed("SELECT id, name, price + 10 AS with_fee FROM t_products WHERE price IS NOT NULL ORDER BY id", tags=["arith", "price_plus_fee"], desc="加法"),
            self._seed("SELECT id, name, price * COALESCE(stock, 0) AS value FROM t_products ORDER BY id", tags=["arith", "inventory_value"], desc="乘法 + COALESCE"),
            self._seed("SELECT id, name, salary / 12 AS monthly FROM t_employees WHERE salary IS NOT NULL ORDER BY id", tags=["arith", "monthly_salary"], desc="除法"),
            self._seed("SELECT id, name, salary - 5000 AS after_tax FROM t_employees WHERE salary IS NOT NULL ORDER BY id", tags=["arith", "after_tax"], desc="减法"),
            self._seed("SELECT id, username, score + age AS combined FROM t_users WHERE score IS NOT NULL AND age IS NOT NULL ORDER BY id", tags=["arith", "combined"], desc="列相加"),
            self._seed("SELECT id, name, ROUND(price * 1.1, 2) AS with_tax FROM t_products WHERE price IS NOT NULL ORDER BY id", tags=["arith", "with_tax"], desc="ROUND + 乘法"),
            self._seed("SELECT id, amount, ABS(amount) AS abs_amt FROM t_transactions WHERE amount IS NOT NULL ORDER BY id", tags=["arith", "abs_amount"], desc="ABS"),
            self._seed("SELECT id, username, ROUND(score, 0) AS rounded FROM t_users WHERE score IS NOT NULL ORDER BY id", tags=["arith", "round_score"], desc="ROUND"),
            self._seed("SELECT id, name, salary, FLOOR(salary / 10000) * 10000 AS salary_band FROM t_employees WHERE salary IS NOT NULL ORDER BY id", tags=["arith", "salary_band"], desc="FLOOR 分段"),
            self._seed("SELECT id, amount, CEIL(amount) AS ceil_amt FROM t_transactions WHERE amount IS NOT NULL ORDER BY id", tags=["arith", "ceil"], desc="CEIL"),
            self._seed("SELECT id, name, price, MOD(CAST(price AS INTEGER), 100) AS remainder FROM t_products WHERE price IS NOT NULL ORDER BY id", tags=["arith", "mod"], desc="MOD"),
            self._seed("SELECT id, username, score, POWER(score, 2) AS squared FROM t_users WHERE score IS NOT NULL ORDER BY id", tags=["arith", "power"], desc="POWER"),
        ]

    def _string_ops(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username, UPPER(username) AS upper_name FROM t_users ORDER BY id", tags=["string", "upper"], desc="UPPER"),
            self._seed("SELECT id, name, LOWER(name) AS lower_name FROM t_employees ORDER BY id", tags=["string", "lower"], desc="LOWER"),
            self._seed("SELECT id, username, LENGTH(username) AS name_len FROM t_users ORDER BY id", tags=["string", "length"], desc="LENGTH"),
            self._seed("SELECT id, name, SUBSTR(name, 1, 3) AS short FROM t_employees ORDER BY id", tags=["string", "substr"], desc="SUBSTR"),
            self._seed("SELECT id, tag, TRIM(tag) AS clean_tag FROM t_tags ORDER BY id", tags=["string", "trim"], desc="TRIM"),
            self._seed("SELECT id, username, username || '@example.com' AS email_guess FROM t_users ORDER BY id", tags=["string", "concat"], desc="字符串拼接"),
            self._seed("SELECT id, name, REPLACE(name, ' ', '_') AS normalized FROM t_departments ORDER BY id", tags=["string", "replace"], desc="REPLACE"),
            self._seed("SELECT id, username, COALESCE(email, 'N/A') AS email_display FROM t_users ORDER BY id", tags=["string", "coalesce"], desc="COALESCE 字符串"),
            self._seed("SELECT id, name, UPPER(SUBSTR(name, 1, 1)) || LOWER(SUBSTR(name, 2)) AS title_case FROM t_employees ORDER BY id", tags=["string", "title_case"], desc="嵌套字符串函数"),
            self._seed("SELECT id, tag, LENGTH(tag) AS tag_len FROM t_tags WHERE LENGTH(tag) > 5 ORDER BY id", tags=["string", "long_tags"], desc="LENGTH 过滤"),
            self._seed("SELECT id, username, 'User: ' || username || ' (ID: ' || CAST(id AS VARCHAR(10)) || ')' AS label FROM t_users ORDER BY id", tags=["string", "complex_concat"], desc="复杂拼接"),
            self._seed("SELECT id, name, INSTR(name, 'a') AS pos FROM t_employees WHERE INSTR(name, 'a') > 0 ORDER BY id", tags=["string", "instr"], desc="INSTR"),
            self._seed("SELECT id, name, LPAD(CAST(id AS VARCHAR(5)), 5, '0') AS padded FROM t_departments ORDER BY id", tags=["string", "lpad"], desc="LPAD"),
        ]

    def _logical(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, username FROM t_users WHERE score > 80 AND age > 25 ORDER BY id", tags=["logic", "and"], desc="AND 逻辑"),
            self._seed("SELECT id, username FROM t_users WHERE score > 80 OR age < 20 ORDER BY id", tags=["logic", "or"], desc="OR 逻辑"),
            self._seed("SELECT id, name FROM t_products WHERE NOT category = 'electronics' ORDER BY id", tags=["logic", "not"], desc="NOT 逻辑"),
            self._seed("SELECT id, name FROM t_employees WHERE salary > 80000 AND dept_id IS NOT NULL AND status = 'active' ORDER BY id", tags=["logic", "multi_and"], desc="多 AND"),
            self._seed("SELECT id, username FROM t_users WHERE (score > 80 OR age < 25) AND email IS NOT NULL ORDER BY id", tags=["logic", "combo"], desc="AND + OR 组合"),
            self._seed("SELECT id, name FROM t_products WHERE (price < 10 OR price > 200) AND stock IS NOT NULL ORDER BY id", tags=["logic", "price_range_logic"], desc="OR + AND"),
            self._seed("SELECT id, name FROM t_employees WHERE NOT (salary < 50000 OR dept_id IS NULL) ORDER BY id", tags=["logic", "not_or"], desc="NOT(OR)"),
            self._seed("SELECT id, username FROM t_users WHERE score BETWEEN 70 AND 90 AND email IS NOT NULL ORDER BY id", tags=["logic", "between_and"], desc="BETWEEN + AND"),
            self._seed("SELECT id, name FROM t_employees WHERE status IN ('active', 'probation') AND salary > 0 ORDER BY id", tags=["logic", "in_and"], desc="IN + AND"),
            self._seed("SELECT id, name FROM t_departments WHERE budget IS NOT NULL AND location IS NOT NULL ORDER BY id", tags=["logic", "not_null_and"], desc="IS NOT NULL + AND"),
            self._seed("SELECT id, username FROM t_users WHERE email LIKE '%@%' AND score IS NOT NULL ORDER BY id", tags=["logic", "like_and"], desc="LIKE + AND"),
            self._seed("SELECT id, amount FROM t_transactions WHERE amount != 0 AND status IS NOT NULL ORDER BY id", tags=["logic", "ne_and"], desc="!= + AND"),
            self._seed("SELECT id, name FROM t_employees WHERE hire_date IS NOT NULL AND salary IS NOT NULL AND dept_id IS NOT NULL ORDER BY id", tags=["logic", "multi_not_null"], desc="多 IS NOT NULL"),
            self._seed("SELECT id, username FROM t_users WHERE (score IS NULL OR age IS NULL) AND email IS NOT NULL ORDER BY id", tags=["logic", "null_or"], desc="NULL 检查 + OR"),
        ]
