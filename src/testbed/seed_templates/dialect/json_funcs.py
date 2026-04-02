"""JSON 函数方言差异模板 — 生成 ~70 条种子 SQL，覆盖 SQLite/Oracle JSON 函数差异。

关键差异点：
- json_extract 返回原生类型（int/float/null） vs JSON_VALUE 返回 VARCHAR2
- json_array / json_arrayagg vs JSON_ARRAY / JSON_ARRAYAGG
- NULL JSON 列 / NULL 路径的处理
- 嵌套路径 '$.a.b' vs '$.a.b'（Oracle JSON_VALUE 可能差异）
- 数组索引 '$.items[0]' 的行为差异

测试表及 JSON 列：
- t_users(profile)
- t_products(metadata)
- t_events(payload)
- t_transactions(metadata_json)
"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class JsonFuncsTemplate(SeedTemplate):
    """生成测试 SQLite/Oracle JSON 函数差异的种子 SQL。"""

    category_prefix = "dialect"
    domain = "json_funcs"
    description = "JSON函数方言差异测试（json_extract vs JSON_VALUE、类型返回、NULL处理）"

    # ------------------------------------------------------------------
    # 公开入口
    # ------------------------------------------------------------------
    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        """生成 ~70 条 JSON 函数方言差异种子 SQL。"""
        seeds: List[SeedSQL] = []
        seeds.extend(self._json_extract_basic())
        seeds.extend(self._json_extract_where())
        seeds.extend(self._json_extract_aggregation())
        seeds.extend(self._json_extract_nested())
        seeds.extend(self._json_extract_coalesce())
        seeds.extend(self._json_extract_cast())
        seeds.extend(self._null_json_handling())
        seeds.extend(self._json_extract_case())
        return seeds

    # ==================================================================
    # 1. json_extract basic (~15)
    # ==================================================================
    def _json_extract_basic(self) -> List[SeedSQL]:
        """json_extract 基本用法 — 测试各表 JSON 列的基础路径提取。"""
        return [
            self._seed(
                "SELECT id, username, json_extract(profile, '$.theme') AS theme "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "basic", "profile"],
                desc="json_extract 从 profile 提取 theme 字段",
            ),
            self._seed(
                "SELECT id, username, json_extract(profile, '$.lang') AS lang "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "basic", "profile"],
                desc="json_extract 从 profile 提取 lang 字段",
            ),
            self._seed(
                "SELECT id, username, json_extract(profile, '$.theme') AS theme, "
                "json_extract(profile, '$.lang') AS lang "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "basic", "profile", "multi_field"],
                desc="json_extract 同时提取 profile 中多个字段",
            ),
            self._seed(
                "SELECT id, name, json_extract(metadata, '$.color') AS color "
                "FROM t_products ORDER BY id",
                tags=["json_extract", "basic", "metadata"],
                desc="json_extract 从 metadata 提取 color 字段",
            ),
            self._seed(
                "SELECT id, name, json_extract(metadata, '$.weight') AS weight "
                "FROM t_products ORDER BY id",
                tags=["json_extract", "basic", "metadata"],
                desc="json_extract 从 metadata 提取 weight 字段",
            ),
            self._seed(
                "SELECT id, name, json_extract(metadata, '$.color') AS color, "
                "json_extract(metadata, '$.weight') AS weight "
                "FROM t_products ORDER BY id",
                tags=["json_extract", "basic", "metadata", "multi_field"],
                desc="json_extract 同时提取 metadata 中多个字段",
            ),
            self._seed(
                "SELECT id, event_type, json_extract(payload, '$.ip') AS ip "
                "FROM t_events ORDER BY id",
                tags=["json_extract", "basic", "payload"],
                desc="json_extract 从 payload 提取 ip 字段",
            ),
            self._seed(
                "SELECT id, event_type, json_extract(payload, '$.code') AS code "
                "FROM t_events ORDER BY id",
                tags=["json_extract", "basic", "payload"],
                desc="json_extract 从 payload 提取 code 字段",
            ),
            self._seed(
                "SELECT id, event_type, json_extract(payload, '$.ip') AS ip, "
                "json_extract(payload, '$.code') AS code "
                "FROM t_events ORDER BY id",
                tags=["json_extract", "basic", "payload", "multi_field"],
                desc="json_extract 同时提取 payload 中多个字段",
            ),
            self._seed(
                "SELECT id, tx_type, json_extract(metadata_json, '$.note') AS note "
                "FROM t_transactions ORDER BY id",
                tags=["json_extract", "basic", "metadata_json"],
                desc="json_extract 从 metadata_json 提取 note 字段",
            ),
            self._seed(
                "SELECT id, tx_type, json_extract(metadata_json, '$.source') AS source "
                "FROM t_transactions ORDER BY id",
                tags=["json_extract", "basic", "metadata_json"],
                desc="json_extract 从 metadata_json 提取 source 字段",
            ),
            self._seed(
                "SELECT id, tx_type, json_extract(metadata_json, '$.priority') AS priority "
                "FROM t_transactions ORDER BY id",
                tags=["json_extract", "basic", "metadata_json"],
                desc="json_extract 从 metadata_json 提取 priority 字段",
            ),
            self._seed(
                "SELECT id, username, json_extract(profile, '$.notifications') AS notifications "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "basic", "profile", "boolean_field"],
                desc="json_extract 提取布尔类型字段 — SQLite 返回 0/1, Oracle 返回字符串",
            ),
            self._seed(
                "SELECT id, name, json_extract(metadata, '$.rating') AS rating "
                "FROM t_products ORDER BY id",
                tags=["json_extract", "basic", "metadata", "numeric_field"],
                desc="json_extract 提取数值字段 — SQLite 返回原生数值, Oracle 返回字符串",
            ),
            self._seed(
                "SELECT id, event_type, json_extract(payload, '$.count') AS ev_count "
                "FROM t_events ORDER BY id",
                tags=["json_extract", "basic", "payload", "numeric_field"],
                desc="json_extract 提取 count 数值字段",
            ),
        ]

    # ==================================================================
    # 2. json_extract with WHERE (~10)
    # ==================================================================
    def _json_extract_where(self) -> List[SeedSQL]:
        """json_extract 在 WHERE 子句中的使用 — 类型比较差异。"""
        return [
            self._seed(
                "SELECT id, username, json_extract(profile, '$.theme') AS theme "
                "FROM t_users "
                "WHERE json_extract(profile, '$.theme') = 'dark' "
                "ORDER BY id",
                tags=["json_extract", "where", "string_comparison"],
                desc="WHERE json_extract 字符串比较 'dark'",
            ),
            self._seed(
                "SELECT id, username, json_extract(profile, '$.lang') AS lang "
                "FROM t_users "
                "WHERE json_extract(profile, '$.lang') = 'en' "
                "ORDER BY id",
                tags=["json_extract", "where", "string_comparison"],
                desc="WHERE json_extract 字符串比较 'en'",
            ),
            self._seed(
                "SELECT id, name, json_extract(metadata, '$.color') AS color "
                "FROM t_products "
                "WHERE json_extract(metadata, '$.color') = 'red' "
                "ORDER BY id",
                tags=["json_extract", "where", "string_comparison"],
                desc="WHERE json_extract metadata color = 'red'",
            ),
            self._seed(
                "SELECT id, event_type, json_extract(payload, '$.code') AS code "
                "FROM t_events "
                "WHERE json_extract(payload, '$.code') IS NOT NULL "
                "ORDER BY id",
                tags=["json_extract", "where", "is_not_null"],
                desc="WHERE json_extract IS NOT NULL — 过滤缺失字段",
            ),
            self._seed(
                "SELECT id, event_type, json_extract(payload, '$.ip') AS ip "
                "FROM t_events "
                "WHERE json_extract(payload, '$.ip') IS NOT NULL "
                "ORDER BY id",
                tags=["json_extract", "where", "is_not_null"],
                desc="WHERE json_extract ip IS NOT NULL",
            ),
            self._seed(
                "SELECT id, tx_type, json_extract(metadata_json, '$.note') AS note "
                "FROM t_transactions "
                "WHERE json_extract(metadata_json, '$.note') IS NOT NULL "
                "ORDER BY id",
                tags=["json_extract", "where", "is_not_null", "metadata_json"],
                desc="WHERE json_extract metadata_json note IS NOT NULL",
            ),
            self._seed(
                "SELECT id, username, json_extract(profile, '$.theme') AS theme "
                "FROM t_users "
                "WHERE json_extract(profile, '$.theme') != 'light' "
                "ORDER BY id",
                tags=["json_extract", "where", "not_equal"],
                desc="WHERE json_extract 不等于 'light'",
            ),
            self._seed(
                "SELECT id, name, json_extract(metadata, '$.rating') AS rating "
                "FROM t_products "
                "WHERE json_extract(metadata, '$.rating') > 4 "
                "ORDER BY id",
                tags=["json_extract", "where", "numeric_comparison"],
                desc="WHERE json_extract 数值比较 > 4 — SQLite 原生数值 vs Oracle 字符串比较",
            ),
            self._seed(
                "SELECT id, event_type, json_extract(payload, '$.count') AS ev_count "
                "FROM t_events "
                "WHERE json_extract(payload, '$.count') >= 1 "
                "ORDER BY id",
                tags=["json_extract", "where", "numeric_comparison"],
                desc="WHERE json_extract count >= 1 — 数值字段过滤",
            ),
            self._seed(
                "SELECT id, username "
                "FROM t_users "
                "WHERE json_extract(profile, '$.notifications') = 1 "
                "ORDER BY id",
                tags=["json_extract", "where", "boolean_comparison"],
                desc="WHERE json_extract 布尔字段 = 1 — SQLite 整数 vs Oracle 字符串",
            ),
        ]

    # ==================================================================
    # 3. json_extract with aggregation (~10)
    # ==================================================================
    def _json_extract_aggregation(self) -> List[SeedSQL]:
        """json_extract 在聚合查询中的使用。"""
        return [
            self._seed(
                "SELECT json_extract(profile, '$.theme') AS theme, "
                "COUNT(*) AS cnt "
                "FROM t_users "
                "WHERE profile IS NOT NULL "
                "GROUP BY json_extract(profile, '$.theme') "
                "ORDER BY theme",
                tags=["json_extract", "group_by", "count"],
                desc="GROUP BY json_extract theme 并计数",
            ),
            self._seed(
                "SELECT json_extract(profile, '$.lang') AS lang, "
                "COUNT(*) AS cnt "
                "FROM t_users "
                "WHERE profile IS NOT NULL "
                "GROUP BY json_extract(profile, '$.lang') "
                "ORDER BY lang",
                tags=["json_extract", "group_by", "count"],
                desc="GROUP BY json_extract lang 并计数",
            ),
            self._seed(
                "SELECT json_extract(metadata, '$.color') AS color, "
                "COUNT(*) AS cnt, AVG(price) AS avg_price "
                "FROM t_products "
                "WHERE metadata IS NOT NULL "
                "GROUP BY json_extract(metadata, '$.color') "
                "ORDER BY color",
                tags=["json_extract", "group_by", "avg"],
                desc="GROUP BY json_extract color 加 AVG(price)",
            ),
            self._seed(
                "SELECT json_extract(payload, '$.code') AS code, "
                "COUNT(*) AS cnt "
                "FROM t_events "
                "WHERE payload IS NOT NULL "
                "GROUP BY json_extract(payload, '$.code') "
                "ORDER BY code",
                tags=["json_extract", "group_by", "count", "payload"],
                desc="GROUP BY json_extract payload code",
            ),
            self._seed(
                "SELECT json_extract(metadata_json, '$.source') AS source, "
                "COUNT(*) AS cnt, SUM(amount) AS total_amount "
                "FROM t_transactions "
                "WHERE metadata_json IS NOT NULL "
                "GROUP BY json_extract(metadata_json, '$.source') "
                "ORDER BY source",
                tags=["json_extract", "group_by", "sum", "metadata_json"],
                desc="GROUP BY json_extract source 加 SUM(amount)",
            ),
            self._seed(
                "SELECT json_extract(metadata_json, '$.priority') AS priority, "
                "COUNT(*) AS cnt "
                "FROM t_transactions "
                "WHERE metadata_json IS NOT NULL "
                "GROUP BY json_extract(metadata_json, '$.priority') "
                "ORDER BY priority",
                tags=["json_extract", "group_by", "count", "metadata_json"],
                desc="GROUP BY json_extract priority",
            ),
            self._seed(
                "SELECT COUNT(json_extract(profile, '$.theme')) AS non_null_themes, "
                "COUNT(*) AS total "
                "FROM t_users ORDER BY non_null_themes",
                tags=["json_extract", "aggregate", "count"],
                desc="COUNT(json_extract(...)) 统计非 NULL 的 JSON 字段数",
            ),
            self._seed(
                "SELECT json_extract(profile, '$.theme') AS theme, "
                "json_extract(profile, '$.lang') AS lang, "
                "COUNT(*) AS cnt "
                "FROM t_users "
                "WHERE profile IS NOT NULL "
                "GROUP BY json_extract(profile, '$.theme'), "
                "json_extract(profile, '$.lang') "
                "ORDER BY theme, lang",
                tags=["json_extract", "group_by", "composite"],
                desc="GROUP BY 多个 json_extract 字段",
            ),
            self._seed(
                "SELECT json_extract(profile, '$.theme') AS theme, "
                "AVG(score) AS avg_score "
                "FROM t_users "
                "WHERE profile IS NOT NULL AND score IS NOT NULL "
                "GROUP BY json_extract(profile, '$.theme') "
                "HAVING COUNT(*) > 0 "
                "ORDER BY avg_score",
                tags=["json_extract", "group_by", "having", "avg"],
                desc="GROUP BY json_extract theme with HAVING and AVG(score)",
            ),
            self._seed(
                "SELECT json_extract(metadata_json, '$.priority') AS priority, "
                "MIN(amount) AS min_amount, MAX(amount) AS max_amount "
                "FROM t_transactions "
                "WHERE metadata_json IS NOT NULL AND amount IS NOT NULL "
                "GROUP BY json_extract(metadata_json, '$.priority') "
                "ORDER BY priority",
                tags=["json_extract", "group_by", "min_max"],
                desc="GROUP BY json_extract priority 加 MIN/MAX(amount)",
            ),
        ]

    # ==================================================================
    # 4. json_extract nested path (~8)
    # ==================================================================
    def _json_extract_nested(self) -> List[SeedSQL]:
        """json_extract 嵌套路径和数组索引 — Oracle JSON_VALUE 可能差异。"""
        return [
            self._seed(
                "SELECT id, username, json_extract(profile, '$.settings.theme') AS deep_theme "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "nested", "profile"],
                desc="json_extract 两层嵌套路径 $.settings.theme",
            ),
            self._seed(
                "SELECT id, username, json_extract(profile, '$.settings.lang') AS deep_lang "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "nested", "profile"],
                desc="json_extract 两层嵌套路径 $.settings.lang",
            ),
            self._seed(
                "SELECT id, name, json_extract(metadata, '$.details.origin') AS origin "
                "FROM t_products ORDER BY id",
                tags=["json_extract", "nested", "metadata"],
                desc="json_extract 嵌套路径 $.details.origin",
            ),
            self._seed(
                "SELECT id, name, json_extract(metadata, '$.details.warehouse') AS warehouse "
                "FROM t_products ORDER BY id",
                tags=["json_extract", "nested", "metadata"],
                desc="json_extract 嵌套路径 $.details.warehouse",
            ),
            self._seed(
                "SELECT id, event_type, json_extract(payload, '$.items[0]') AS first_item "
                "FROM t_events ORDER BY id",
                tags=["json_extract", "nested", "array_index", "payload"],
                desc="json_extract 数组索引 $.items[0] — Oracle JSON_VALUE 数组语法差异",
            ),
            self._seed(
                "SELECT id, event_type, json_extract(payload, '$.items[1]') AS second_item "
                "FROM t_events ORDER BY id",
                tags=["json_extract", "nested", "array_index", "payload"],
                desc="json_extract 数组索引 $.items[1]",
            ),
            self._seed(
                "SELECT id, tx_type, "
                "json_extract(metadata_json, '$.tags[0]') AS first_tag "
                "FROM t_transactions ORDER BY id",
                tags=["json_extract", "nested", "array_index", "metadata_json"],
                desc="json_extract 数组索引 $.tags[0] 从 metadata_json",
            ),
            self._seed(
                "SELECT id, username, "
                "json_extract(profile, '$.settings.nested.key') AS deep_nested "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "nested", "three_level", "profile"],
                desc="json_extract 三层嵌套路径 $.settings.nested.key",
            ),
        ]

    # ==================================================================
    # 5. json_extract + COALESCE (~8)
    # ==================================================================
    def _json_extract_coalesce(self) -> List[SeedSQL]:
        """json_extract 与 COALESCE 组合 — 处理 JSON 字段缺失的默认值。"""
        return [
            self._seed(
                "SELECT id, username, "
                "COALESCE(json_extract(profile, '$.theme'), 'default') AS theme "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "coalesce", "default_value"],
                desc="COALESCE(json_extract, 'default') — 为缺失 JSON 字段提供默认值",
            ),
            self._seed(
                "SELECT id, username, "
                "COALESCE(json_extract(profile, '$.lang'), 'unknown') AS lang "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "coalesce", "default_value"],
                desc="COALESCE(json_extract lang, 'unknown')",
            ),
            self._seed(
                "SELECT id, name, "
                "COALESCE(json_extract(metadata, '$.color'), 'N/A') AS color "
                "FROM t_products ORDER BY id",
                tags=["json_extract", "coalesce", "metadata"],
                desc="COALESCE(json_extract color, 'N/A')",
            ),
            self._seed(
                "SELECT id, name, "
                "COALESCE(json_extract(metadata, '$.rating'), 0) AS rating "
                "FROM t_products ORDER BY id",
                tags=["json_extract", "coalesce", "numeric_default"],
                desc="COALESCE(json_extract rating, 0) — 数值默认值",
            ),
            self._seed(
                "SELECT id, event_type, "
                "COALESCE(json_extract(payload, '$.ip'), '0.0.0.0') AS ip "
                "FROM t_events ORDER BY id",
                tags=["json_extract", "coalesce", "payload"],
                desc="COALESCE(json_extract ip, '0.0.0.0')",
            ),
            self._seed(
                "SELECT id, event_type, "
                "COALESCE(json_extract(payload, '$.code'), 'NONE') AS code "
                "FROM t_events ORDER BY id",
                tags=["json_extract", "coalesce", "payload"],
                desc="COALESCE(json_extract code, 'NONE')",
            ),
            self._seed(
                "SELECT id, tx_type, "
                "COALESCE(json_extract(metadata_json, '$.note'), 'no note') AS note "
                "FROM t_transactions ORDER BY id",
                tags=["json_extract", "coalesce", "metadata_json"],
                desc="COALESCE(json_extract note, 'no note')",
            ),
            self._seed(
                "SELECT id, tx_type, "
                "COALESCE(json_extract(metadata_json, '$.priority'), 0) AS priority "
                "FROM t_transactions ORDER BY id",
                tags=["json_extract", "coalesce", "metadata_json", "numeric_default"],
                desc="COALESCE(json_extract priority, 0) — 数值默认值",
            ),
        ]

    # ==================================================================
    # 6. json_extract + CAST (~8)
    # ==================================================================
    def _json_extract_cast(self) -> List[SeedSQL]:
        """json_extract 与 CAST 组合 — 类型转换差异。"""
        return [
            self._seed(
                "SELECT id, username, "
                "CAST(json_extract(profile, '$.theme') AS VARCHAR(50)) AS theme_str "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "cast", "varchar"],
                desc="CAST(json_extract AS VARCHAR) — 强制字符串类型",
            ),
            self._seed(
                "SELECT id, username, "
                "CAST(json_extract(profile, '$.lang') AS VARCHAR(20)) AS lang_str "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "cast", "varchar"],
                desc="CAST(json_extract lang AS VARCHAR)",
            ),
            self._seed(
                "SELECT id, name, "
                "CAST(json_extract(metadata, '$.rating') AS INTEGER) AS rating_int "
                "FROM t_products ORDER BY id",
                tags=["json_extract", "cast", "integer", "numeric_field"],
                desc="CAST(json_extract rating AS INTEGER) — SQLite 原生 vs Oracle 字符串截断",
            ),
            self._seed(
                "SELECT id, name, "
                "CAST(json_extract(metadata, '$.weight') AS REAL) AS weight_real "
                "FROM t_products ORDER BY id",
                tags=["json_extract", "cast", "real"],
                desc="CAST(json_extract weight AS REAL)",
            ),
            self._seed(
                "SELECT id, event_type, "
                "CAST(json_extract(payload, '$.count') AS INTEGER) AS count_int "
                "FROM t_events ORDER BY id",
                tags=["json_extract", "cast", "integer", "payload"],
                desc="CAST(json_extract count AS INTEGER)",
            ),
            self._seed(
                "SELECT id, event_type, "
                "CAST(json_extract(payload, '$.ip') AS VARCHAR(20)) AS ip_str "
                "FROM t_events ORDER BY id",
                tags=["json_extract", "cast", "varchar", "payload"],
                desc="CAST(json_extract ip AS VARCHAR)",
            ),
            self._seed(
                "SELECT id, tx_type, "
                "CAST(json_extract(metadata_json, '$.priority') AS INTEGER) AS priority_int "
                "FROM t_transactions ORDER BY id",
                tags=["json_extract", "cast", "integer", "metadata_json"],
                desc="CAST(json_extract priority AS INTEGER)",
            ),
            self._seed(
                "SELECT id, username, "
                "CAST(COALESCE(json_extract(profile, '$.theme'), 'unknown') AS VARCHAR(30)) AS theme_safe "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "cast", "coalesce", "combined"],
                desc="CAST(COALESCE(json_extract, default) AS VARCHAR) — 组合模式",
            ),
        ]

    # ==================================================================
    # 7. NULL JSON handling (~6)
    # ==================================================================
    def _null_json_handling(self) -> List[SeedSQL]:
        """json_extract 在 NULL JSON 列上的行为 — SQLite 返回 NULL，Oracle JSON_VALUE 报错或返回 NULL。"""
        return [
            self._seed(
                "SELECT id, username, json_extract(profile, '$.theme') AS theme "
                "FROM t_users "
                "WHERE profile IS NULL "
                "ORDER BY id",
                tags=["json_extract", "null_json", "profile"],
                desc="json_extract 对 NULL profile 列 — SQLite 返回 NULL, Oracle 行为差异",
            ),
            self._seed(
                "SELECT id, name, json_extract(metadata, '$.color') AS color "
                "FROM t_products "
                "WHERE metadata IS NULL "
                "ORDER BY id",
                tags=["json_extract", "null_json", "metadata"],
                desc="json_extract 对 NULL metadata 列",
            ),
            self._seed(
                "SELECT id, event_type, json_extract(payload, '$.ip') AS ip "
                "FROM t_events "
                "WHERE payload IS NULL "
                "ORDER BY id",
                tags=["json_extract", "null_json", "payload"],
                desc="json_extract 对 NULL payload 列",
            ),
            self._seed(
                "SELECT id, tx_type, json_extract(metadata_json, '$.note') AS note "
                "FROM t_transactions "
                "WHERE metadata_json IS NULL "
                "ORDER BY id",
                tags=["json_extract", "null_json", "metadata_json"],
                desc="json_extract 对 NULL metadata_json 列",
            ),
            self._seed(
                "SELECT id, username, "
                "json_extract(profile, '$.nonexistent.path') AS missing "
                "FROM t_users "
                "WHERE profile IS NOT NULL "
                "ORDER BY id",
                tags=["json_extract", "null_path", "missing_field"],
                desc="json_extract 不存在的路径 — 两者均应返回 NULL",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN profile IS NULL THEN 'no_profile' "
                "ELSE COALESCE(CAST(json_extract(profile, '$.theme') AS VARCHAR(20)), 'no_theme') "
                "END AS profile_status "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "null_json", "case", "defensive"],
                desc="防御性 JSON 处理: 先检查 NULL 列再 json_extract",
            ),
        ]

    # ==================================================================
    # 8. json_extract in CASE (~5)
    # ==================================================================
    def _json_extract_case(self) -> List[SeedSQL]:
        """json_extract 在 CASE 表达式中的使用。"""
        return [
            self._seed(
                "SELECT id, username, "
                "CASE WHEN json_extract(profile, '$.theme') = 'dark' THEN 'night_owl' "
                "WHEN json_extract(profile, '$.theme') = 'light' THEN 'early_bird' "
                "ELSE 'unknown' END AS user_style "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "case", "classification"],
                desc="CASE 根据 json_extract theme 分类用户风格",
            ),
            self._seed(
                "SELECT id, name, price, "
                "CASE WHEN json_extract(metadata, '$.rating') >= 4 THEN 'highly_rated' "
                "WHEN json_extract(metadata, '$.rating') >= 2 THEN 'average' "
                "ELSE 'low_rated' END AS rating_band "
                "FROM t_products ORDER BY id",
                tags=["json_extract", "case", "numeric_comparison"],
                desc="CASE 根据 json_extract rating 数值分级",
            ),
            self._seed(
                "SELECT id, event_type, "
                "CASE WHEN json_extract(payload, '$.code') IS NOT NULL "
                "THEN 'has_code' ELSE 'no_code' END AS code_status "
                "FROM t_events ORDER BY id",
                tags=["json_extract", "case", "null_check"],
                desc="CASE 检查 json_extract 字段是否存在",
            ),
            self._seed(
                "SELECT id, tx_type, amount, "
                "CASE WHEN json_extract(metadata_json, '$.priority') = 1 THEN 'urgent' "
                "WHEN json_extract(metadata_json, '$.priority') = 2 THEN 'normal' "
                "WHEN json_extract(metadata_json, '$.priority') = 3 THEN 'low' "
                "ELSE 'unspecified' END AS priority_label "
                "FROM t_transactions ORDER BY id",
                tags=["json_extract", "case", "priority", "metadata_json"],
                desc="CASE 根据 json_extract priority 标注优先级",
            ),
            self._seed(
                "SELECT id, username, "
                "CASE WHEN json_extract(profile, '$.notifications') = 1 THEN 'subscribed' "
                "WHEN json_extract(profile, '$.notifications') = 0 THEN 'unsubscribed' "
                "ELSE 'not_set' END AS notif_status "
                "FROM t_users ORDER BY id",
                tags=["json_extract", "case", "boolean_field"],
                desc="CASE 根据 json_extract 布尔字段判断订阅状态",
            ),
        ]
