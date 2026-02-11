"""JSON 函数双向转换规则。

SQLite 使用 json_extract()，Oracle 使用 JSON_VALUE()。
SQLGlot 原生不处理此差异（仅大写函数名），需要自定义规则补充。
"""

import sqlglot.expressions as exp

from ..rule_base import TranspilationRule


class JsonExtractToJsonValueRule(TranspilationRule):
    """将 SQLite 的 json_extract() 转换为 Oracle 的 JSON_VALUE()。

    示例：json_extract(profile, '$.theme') → JSON_VALUE(profile, '$.theme')
    """

    @property
    def name(self) -> str:
        return "json_extract_to_json_value"

    @property
    def description(self) -> str:
        return "将 json_extract() 转换为 JSON_VALUE()（SQLite→Oracle）"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.JSONExtract):
                return node
            col = node.this
            path = node.expression
            # JSONPath.sql() 返回带引号的路径如 "'$.theme'"，去掉外层引号
            path_str = path.sql().strip("'")
            return exp.Anonymous(
                this="JSON_VALUE",
                expressions=[col.copy(), exp.Literal.string(path_str)],
            )

        return self._transform(tree, _transform)


class JsonValueToJsonExtractRule(TranspilationRule):
    """将 Oracle 的 JSON_VALUE() 转换为 SQLite 的 json_extract()。

    Oracle 解析后 JSON_VALUE 为 Anonymous 节点（非 JSONExtract），
    故通过匹配 Anonymous(this='JSON_VALUE') 来识别。

    注意：不能用 exp.JSONExtract 构建替换节点，否则 SQLite 方言生成器
    会输出箭头语法 (->) 而非函数调用形式。使用 Anonymous('json_extract')
    确保生成 json_extract(...) 的函数调用语法。
    """

    @property
    def name(self) -> str:
        return "json_value_to_json_extract"

    @property
    def description(self) -> str:
        return "将 JSON_VALUE() 转换为 json_extract()（Oracle→SQLite）"

    def apply(self, tree: exp.Expression) -> exp.Expression:
        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Anonymous):
                return node
            if not (isinstance(node.this, str) and node.this.upper() == "JSON_VALUE"):
                return node
            args = node.expressions
            if len(args) < 2:
                return node
            # 保持原参数，仅替换函数名
            return exp.Anonymous(
                this="json_extract",
                expressions=[a.copy() for a in args],
            )

        return self._transform(tree, _transform)
