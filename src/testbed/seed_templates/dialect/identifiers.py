"""标识符/引号方言差异模板 — 测试 SQLite/Oracle 标识符引用差异。

覆盖差异点：
- SQLite: 反引号 `` 或双引号 "" 包围标识符
- Oracle: 双引号 "" 包围标识符（大小写敏感）
- 字符串用单引号
- 标识符大小写规则不同
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class IdentifiersTemplate(SeedTemplate):
    """标识符/引号方言差异种子模板。"""

    @property
    def domain(self) -> str:
        return "identifiers"

    @property
    def description(self) -> str:
        return "标识符/引号方言差异测试（引号风格、大小写规则）"

    @property
    def category_prefix(self) -> str:
        return "dialect"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._quoted_identifiers())
        seeds.extend(self._alias_quoting())
        seeds.extend(self._mixed_quoting())
        seeds.extend(self._reserved_words())
        return seeds

    # ── 引号标识符 (~5) ─────────────────────────────────
    def _quoted_identifiers(self) -> List[SeedSQL]:
        return [
            self._seed(
                'SELECT "id", "username", "score" FROM "t_users" ORDER BY "id"',
                tags=["quoted_id", "double_quote"],
                desc="双引号包围所有标识符",
            ),
            self._seed(
                'SELECT "id", "name", "price" FROM "t_products" ORDER BY "id"',
                tags=["quoted_id", "quote_products"],
                desc="双引号 — products 表",
            ),
            self._seed(
                'SELECT "id", "name", "salary" FROM "t_employees" ORDER BY "id"',
                tags=["quoted_id", "quote_employees"],
                desc="双引号 — employees 表",
            ),
            self._seed(
                'SELECT "id", "event_type" AS "type", "event_date" AS "date" '
                'FROM "t_events" ORDER BY "id"',
                tags=["quoted_id", "quote_events"],
                desc="双引号 — events 含别名",
            ),
            self._seed(
                'SELECT "id", "amount", "tx_type" FROM "t_transactions" ORDER BY "id"',
                tags=["quoted_id", "quote_transactions"],
                desc="双引号 — transactions 表",
            ),
        ]

    # ── 别名引号 (~5) ────────────────────────────────────
    def _alias_quoting(self) -> List[SeedSQL]:
        return [
            self._seed(
                'SELECT id AS "user_id", username AS "user_name", '
                'score AS "user_score" FROM t_users ORDER BY id',
                tags=["alias_quote", "alias_double_quote"],
                desc="别名使用双引号",
            ),
            self._seed(
                'SELECT id AS "编号", name AS "名称", salary AS "薪资" '
                'FROM t_employees ORDER BY id',
                tags=["alias_quote", "alias_chinese"],
                desc="别名含中文字符",
            ),
            self._seed(
                'SELECT COUNT(*) AS "total count", AVG(score) AS "avg score" '
                'FROM t_users',
                tags=["alias_quote", "alias_space"],
                desc="别名含空格（需引号）",
            ),
            self._seed(
                'SELECT e.id AS "emp_id", d.name AS "dept_name", '
                'e.salary AS "emp_salary" '
                'FROM t_employees e JOIN t_departments d ON e.dept_id = d.id '
                'ORDER BY e.id',
                tags=["alias_quote", "alias_join"],
                desc="JOIN 查询双引号别名",
            ),
            self._seed(
                'SELECT id, amount AS "金额", tx_type AS "类型", '
                'status AS "状态" FROM t_transactions ORDER BY id',
                tags=["alias_quote", "alias_chinese_tx"],
                desc="交易表中文别名",
            ),
        ]

    # ── 混合引号 (~5) ────────────────────────────────────
    def _mixed_quoting(self) -> List[SeedSQL]:
        return [
            self._seed(
                'SELECT "id", username, \'active\' AS status_label FROM t_users ORDER BY "id"',
                tags=["mixed_quote", "mixed_select"],
                desc="混合引号 — 标识符双引号、字符串单引号",
            ),
            self._seed(
                'SELECT "id", name, \'test_\' || name AS labeled FROM t_products ORDER BY "id"',
                tags=["mixed_quote", "mixed_concat"],
                desc="引号标识符 + 字符串拼接",
            ),
            self._seed(
                'SELECT "id", "name", CASE WHEN "salary" IS NULL THEN \'N/A\' '
                'ELSE CAST("salary" AS VARCHAR(20)) END AS salary_display '
                'FROM t_employees ORDER BY "id"',
                tags=["mixed_quote", "mixed_case"],
                desc="引号标识符 + CASE + CAST",
            ),
            self._seed(
                'SELECT "id", "username" AS "name", '
                '"email" AS "contact" FROM "t_users" '
                'WHERE "email" IS NOT NULL ORDER BY "id"',
                tags=["mixed_quote", "all_quoted"],
                desc="全引号查询 — 所有标识符加引号",
            ),
            self._seed(
                'SELECT "id", "amount" AS "val", '
                'CASE WHEN "amount" > 0 THEN \'positive\' ELSE \'non_positive\' END AS "sign" '
                'FROM "t_transactions" ORDER BY "id"',
                tags=["mixed_quote", "quoted_case_string"],
                desc="引号标识符 + CASE 字符串",
            ),
        ]

    # ── 保留字/特殊列名 (~5) ──────────────────────────────
    def _reserved_words(self) -> List[SeedSQL]:
        return [
            self._seed(
                'SELECT id, username, score AS "value" FROM t_users ORDER BY id',
                tags=["reserved", "as_value"],
                desc="别名使用 value（可能是保留字）",
            ),
            self._seed(
                'SELECT id, name, "number" AS "key" FROM t_products ORDER BY id',
                tags=["reserved", "as_key"],
                desc="别名使用 key（可能是保留字）",
            ),
            self._seed(
                'SELECT id, "order" AS "order_num" FROM t_orders ORDER BY id',
                tags=["reserved", "order_alias"],
                desc="别名使用 order（SQL 保留字）",
            ),
            self._seed(
                'SELECT id, COUNT(*) AS "count", AVG(score) AS "average" '
                'FROM t_users GROUP BY id ORDER BY id',
                tags=["reserved", "count_alias"],
                desc="聚合别名 count（保留字）",
            ),
            self._seed(
                'SELECT e.id, e.name AS "name", d.name AS "group" '
                'FROM t_employees e JOIN t_departments d ON e.dept_id = d.id '
                'ORDER BY e.id',
                tags=["reserved", "group_alias"],
                desc="别名使用 group（SQL 保留字）",
            ),
        ]
