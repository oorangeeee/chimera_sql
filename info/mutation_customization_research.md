# AST 变异规则定制化调研（DBMS 与版本维度）

## 1. 结论（先给答案）

变异规则**必须定制化**，而且应采用 `DBMS + 版本` 双维度定制，而不是一套通用规则打天下。

原因不是“实现偏好”，而是业内工具和论文反复验证的工程事实：

- 不同数据库的语法、函数、类型系统、优化器语义差异会导致同一变异在不同 DBMS 上从“高价值”变成“无效或误报”。
- 同一数据库不同版本也会改变可用特性和行为边界，若不做版本门控，误报和无效样例显著增加。

## 2. 业内最佳实践证据

### 2.1 SQLancer：按 DBMS 编写专用 provider，并明确“版本相关”

SQLancer 官方文档明确指出：

- 需要为每个 DBMS 编写 DBMS-specific code。
- SQLancer 本身是“specific to a version of the DBMS”，换版本可能出现 false alarms。

这直接说明：变异/生成/判错策略都要绑定 DBMS 与版本，不然质量会下降。

来源：
- https://github.com/sqlancer/sqlancer

### 2.2 SQLsmith（CockroachDB 实践）：同源工具也需要按目标 DB 改造

CockroachDB 团队在 SQLsmith 实践里给出两个关键点：

- SQLsmith 通过 introspect 数据库当前可用对象（operators/functions/types/tables/columns）来生成“可执行且有语义”的查询。
- 原始 SQLsmith 依赖的一些 PostgreSQL 特性在 CockroachDB 上不完全可用，因此他们做了适配修改。

这说明“能力感知 + 目标库适配”是生成高质量 SQL 的必要条件。

来源：
- https://www.cockroachlabs.com/blog/sqlsmith-randomized-sql-testing/

### 2.3 sqllogictest：跨库比较依赖 skipif/onlyif 和差异规避策略

sqllogictest 文档强调：

- 其目标是跨引擎对比正确性。
- 对语法/行为差异使用 `skipif/onlyif`。
- 对无序结果、NULL 排序、LIMIT/OFFSET 支持差异等提供测试编排约束。

这等价于“测试样例和规则需按引擎能力定制”。

来源：
- https://www.sqlite.org/sqllogictest
- https://sqlite.org/sqllogictest/info

### 2.4 SQLRight / DynSQL：高质量变异依赖有效性导向与状态感知

- SQLRight 强调 validity-oriented mutations，以提升语句有效率并发现更多逻辑 bug。
- DynSQL 指出仅靠固定 grammar/静态知识不足，需利用 DBMS state information 动态引导生成。

这说明“变异质量”依赖目标系统能力和运行时状态，不是纯随机替换。

来源：
- https://www.usenix.org/conference/usenixsecurity22/presentation/liang
- https://www.usenix.org/conference/usenixsecurity23/presentation/jiang-zu-ming

### 2.5 Metamorphic 测试（Cockroach/Pebble）：版本与配置也要进入测试维度

Cockroach/Pebble 的 metamorphic 测试实践显示：

- 同一操作序列在不同配置下比对输出可发现深层问题。
- 他们还专门做了跨多版本的 metamorphic 变体来捕获版本兼容问题。

这支持“版本维度必须显式纳入测试策略”。

来源：
- https://www.cockroachlabs.com/blog/metamorphic-testing-the-database/

## 3. 对 ChimeraSQL 的可执行设计建议

### 3.1 规则分层：三层而不是一层

1. 通用语义层（跨库可复用）
- 数值边界、谓词扰动、投影/连接重组等基础变异。

2. DBMS 定制层（按数据库）
- 只启用该库支持的语法与函数变异。
- 维护“已知不一致但非 bug”的差异白名单。

3. 版本定制层（按版本）
- 用 feature gate 控制规则启停。
- 例如：`EXCEPT`、JSON 函数族、窗口函数细节等按版本能力开关。

### 3.2 能力画像（Capability Profile）应成为一等公民

建议在配置中引入 `capability profile`：

- 维度：`dbms`、`version_range`、`features`、`known_differences`、`expected_errors`。
- 变异器在执行前加载 profile，只生成“目标可执行且可比较”的 SQL。

### 3.3 变异前置校验与反馈闭环

- 变异前：基于 profile 做静态门控（不支持的规则不触发）。
- 变异后：执行失败按 expected-error 分类，避免污染 bug 样本。
- 反馈：将高价值样本（新覆盖/新差异）回灌变异队列，参考 SQLRight 的覆盖引导思想。

### 3.4 跨库测试中的“可比性”优先

- 默认保证确定性输出（`ORDER BY` 或客户端排序策略）。
- 对 NULL、空字符串、浮点精度、布尔表示做归一化。
- 对确属标准差异而非缺陷的场景做显式标注，不计入 bug。

## 4. 推荐的落地路线（短期）

1. 定义 `capability profile` 配置格式（先支持 Oracle/SQLite 各 1 个版本档）。
2. 将现有变异规则拆分为：`generic` 与 `dialect/version-gated` 两组。
3. 在变异调度器增加 `can_apply(rule, profile)`。
4. 在差分报告中新增字段：`profile_id`、`gated_rules`、`expected_diff_class`。

## 5. 结论复述

对于“跨数据库 + AST 变异 + 差分”的系统，业界共识是：

- 变异规则需要定制化；
- 定制化应覆盖 DBMS 与版本两个维度；
- 最佳实现形态是“能力画像驱动的规则门控 + 差分归一化 + 反馈闭环”。
