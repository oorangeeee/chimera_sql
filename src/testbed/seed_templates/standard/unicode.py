"""Unicode 标准模板 — 验证标准 SQL 含 Unicode 正常转译。"""

from __future__ import annotations

from typing import List

from ..base import SchemaMetadata, SeedSQL, SeedTemplate


class StandardUnicodeTemplate(SeedTemplate):

    @property
    def domain(self) -> str:
        return "unicode"

    @property
    def description(self) -> str:
        return "标准SQL Unicode测试（中文/日文/拉丁/特殊符号）"

    @property
    def category_prefix(self) -> str:
        return "standard"

    def generate(self, schema: SchemaMetadata) -> List[SeedSQL]:
        seeds: List[SeedSQL] = []
        seeds.extend(self._chinese())
        seeds.extend(self._mixed_lang())
        seeds.extend(self._special_chars())
        return seeds

    def _chinese(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, name FROM t_employees WHERE name LIKE '张%' ORDER BY id", tags=["chinese", "zhang_prefix"], desc="中文 LIKE 张%"),
            self._seed("SELECT id, name FROM t_employees WHERE name LIKE '李%' ORDER BY id", tags=["chinese", "li_prefix"], desc="中文 LIKE 李%"),
            self._seed("SELECT id, name FROM t_employees WHERE name LIKE '%三' ORDER BY id", tags=["chinese", "san_suffix"], desc="中文 LIKE %三"),
            self._seed("SELECT id, tag FROM t_tags WHERE tag LIKE '%测试%' ORDER BY id", tags=["chinese", "test_tag"], desc="中文标签搜索"),
            self._seed("SELECT id, name FROM t_employees WHERE name LIKE '王%' OR name LIKE '赵%' ORDER BY id", tags=["chinese", "multi_prefix"], desc="多中文前缀"),
            self._seed("SELECT id, name FROM t_employees WHERE LENGTH(name) >= 2 AND name LIKE '%十%' ORDER BY id", tags=["chinese", "shi_contains"], desc="中文含'十'"),
            self._seed("SELECT id, tag FROM t_tags WHERE LENGTH(tag) > 2 ORDER BY id", tags=["chinese", "long_tags"], desc="长标签"),
            self._seed("SELECT id, name FROM t_employees WHERE name LIKE '%五' OR name LIKE '%六' OR name LIKE '%七' ORDER BY id", tags=["chinese", "number_suffix"], desc="数字尾中文名"),
            self._seed("SELECT id, name, bio FROM t_employees WHERE bio LIKE '%前端%' OR bio LIKE '%后端%' ORDER BY id", tags=["chinese", "bio_keyword"], desc="中文 bio 搜索"),
        ]

    def _mixed_lang(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, name FROM t_employees WHERE name NOT LIKE '%%' AND name LIKE '%a%' ORDER BY id", tags=["mixed", "has_a"], desc="含英文字符"),
            self._seed("SELECT id, name, bio FROM t_employees WHERE bio IS NOT NULL AND bio LIKE '%React%' ORDER BY id", tags=["mixed", "bio_react"], desc="bio 含英文关键词"),
            self._seed("SELECT id, name FROM t_employees WHERE name GLOB '*[a-zA-Z]*' ORDER BY id", tags=["mixed", "has_english"], desc="含英文字母"),
            self._seed("SELECT id, tag FROM t_tags WHERE tag GLOB '*[^a-zA-Z0-9]*' ORDER BY id", tags=["mixed", "special_tag"], desc="含非 ASCII 标签"),
            self._seed("SELECT id, name, COALESCE(bio, '无简介') AS bio_display FROM t_employees ORDER BY id", tags=["mixed", "chinese_default"], desc="中文默认值"),
            self._seed("SELECT id, tag FROM t_tags WHERE tag = 'café' OR tag LIKE '数据%' ORDER BY id", tags=["mixed", "mixed_tags"], desc="混合语言标签"),
            self._seed("SELECT id, name FROM t_employees WHERE UPPER(name) != name ORDER BY id", tags=["mixed", "has_lower"], desc="含小写字母"),
            self._seed("SELECT id, name FROM t_employees ORDER BY name, id", tags=["mixed", "mixed_sort"], desc="混合排序"),
        ]

    def _special_chars(self) -> List[SeedSQL]:
        return [
            self._seed("SELECT id, tag FROM t_tags WHERE tag LIKE '%-%' ORDER BY id", tags=["special", "dash"], desc="含连字符"),
            self._seed("SELECT id, tag FROM t_tags WHERE tag LIKE '%@%' ORDER BY id", tags=["special", "at_sign"], desc="含 @"),
            self._seed("SELECT id, tag FROM t_tags WHERE tag LIKE '%.%' ORDER BY id", tags=["special", "dot"], desc="含点号"),
            self._seed("SELECT id, name FROM t_employees WHERE bio LIKE '%/%' ORDER BY id", tags=["special", "slash"], desc="含斜杠"),
            self._seed("SELECT id, tag FROM t_tags WHERE INSTR(tag, ' ') > 0 ORDER BY id", tags=["special", "space"], desc="含空格"),
            self._seed("SELECT id, tag FROM t_tags WHERE LENGTH(tag) != LENGTH(CAST(tag AS VARCHAR(100))) ORDER BY id", tags=["special", "unicode_len"], desc="Unicode 长度"),
            self._seed("SELECT id, name, bio FROM t_employees WHERE bio LIKE '%\"%' ORDER BY id", tags=["special", "quote_in_bio"], desc="含引号"),
            self._seed("SELECT id, tag FROM t_tags ORDER BY UPPER(tag), id", tags=["special", "case_order"], desc="大小写无关排序"),
        ]
