# ChimeraSQL 核心思路（项目总览）

## 1. 核心目标

ChimeraSQL 的核心目标是：以通用 SQL 种子为起点，通过 AST 级别变异生成高质量模糊测试用例，利用方言转译在多数据库执行，并通过差分分析发现逻辑不一致与崩溃缺陷。

## 2. 核心流程

Seed SQL → AST 变异 → 方言转译 → 多库执行 → 差分分析 → 报告

研发早期可单独执行 seeds 回归测试，验证基础设施与连接器是否正常工作；该模式仅用于研发验证，不改变最终主流程。

## 3. 差分到底是什么

差分测试（Differential Testing）是用多个实现互相当“参考答案”，比较同一输入在不同实现上的输出是否一致。

数据库场景的最小流程：

1. 生成一条 SQL
2. 在多个数据库执行（如 Oracle、SQLite）
3. 比较结果是否一致
4. 不一致则标记为潜在逻辑 bug 或语义差异

差分解决的是“没有真值答案时如何判错”的问题。

## 4. 差分是否必要

差分不是“必要条件”，但在跨数据库模糊测试中是“最划算、最稳定的错误检测手段”。

取舍原则：

- 仅做崩溃检测时，单库执行即可
- 需要验证语义一致性时，差分几乎是唯一可自动化的方案

因此对本项目而言，差分是最自然的默认选择，但可以通过配置允许仅做崩溃检测。

## 5. 差分分析器应达到的效果

差分分析器的目标是“在合理的语义对齐下判断一致性”，而不是字节级完全一致。

建议的最小效果标准：

- 结果集一致性：在可比较语义下，两库结果一致，否则判为差异
- 结果归一化：处理跨库已知差异，避免噪声误报
- 解释性输出：明确指出差异 SQL、首个差异位置、两侧值
- 可配置规则：允许调整 NULL/空字符串、布尔、数值精度等比较策略

## 6. 价值链与优先级

核心价值集中在中间三段：

- AST 变异引擎（产生有效且具有攻击性的 SQL）
- 方言转译器（将通用 SQL 转成具体数据库语法）
- 差分分析器（对比结果并输出缺陷报告）

连接器与测试基础设施是支撑层，但不应喧宾夺主。

## 7. 解耦原则

- 变异模块与转译模块必须解耦
- 变异策略不应依赖特定数据库方言
- 转译模块仅负责将通用 SQL 转为目标方言

## 8. 最小可用目标（MVP）

- 2–3 个可用的 AST 变异策略
- 基础 SQLGlot 转译 + 少量规则库修正
- 基本差分分析（结果归一化 + 比较）
- 端到端流程可执行并产生报告

## 9. 方言转译器详细设计思路

### 9.1 为什么需要自定义规则引擎

SQLGlot 是 Python 生态中最成熟的 SQL 方言转换库，通过 `sqlglot.transpile(sql, read="sqlite", write="oracle")` 即可完成约 80% 的方言差异转换（如 `LIMIT/OFFSET` → `FETCH FIRST`、`COALESCE` ↔ `NVL`、大小写函数等）。

但实测发现以下关键差异 SQLGlot **无法自动处理**：

| 差异 | SQLGlot 表现 | 问题 |
|------|-------------|------|
| `json_extract(col, '$.key')` → `JSON_VALUE(col, '$.key')` | 仅大写为 `JSON_EXTRACT()`，不转函数名 | Oracle 无 `JSON_EXTRACT` 函数 |
| `WITH RECURSIVE cte AS (...)` → `WITH cte AS (...)` | 保留 `RECURSIVE` 关键字不变 | Oracle 不识别 `RECURSIVE` 关键字 |

这两类差异正好落在种子 SQL 的 `09_recursive_self_join/`（6 条）和 `11_json_handling/`（6 条）类别中，不处理则 Oracle 端直接报语法错误。因此必须在 SQLGlot 之上叠加一层自定义规则引擎。

### 9.2 两阶段转译管线

设计的核心思想是 “SQLGlot 做重活，规则引擎做精修”：

```
输入 SQL (字符串)
    │
    ▼
sqlglot.parse_one(sql, read=source_dialect)     ← 阶段1: AST 解析
    │
    ▼
[AST 树]
    │
    ▼
rule_1.apply(tree) → rule_2.apply(tree) → ...   ← 阶段2: 自定义规则链变换
    │                                                （补充 SQLGlot 未覆盖的差异）
    ▼
[变换后 AST]
    │
    ▼
tree.sql(dialect=target_dialect)                 ← 阶段3: SQLGlot 目标方言生成
    │                                                （自动处理 LIMIT→FETCH 等）
    ▼
输出 SQL (字符串)
```

阶段 1 和阶段 3 完全委托给 SQLGlot，只在阶段 2 插入自定义规则。这使得：

- SQLGlot 升级修复某个差异时，对应规则可直接移除
- 新发现的差异只需新增规则类，不改动编排逻辑

### 9.3 规则引擎架构（策略模式 + 责任链）

采用 **策略模式** 定义规则接口，结合 **责任链** 按序执行：

```
TranspilationRule (ABC)          ← 策略接口
├── JsonExtractToJsonValueRule   ← json_extract → JSON_VALUE
├── JsonValueToJsonExtractRule   ← JSON_VALUE → json_extract
├── RemoveRecursiveKeywordRule   ← 移除 RECURSIVE
├── AddRecursiveKeywordRule      ← 添加 RECURSIVE（启发式检测）
├── ExceptToMinusRule            ← EXCEPT → MINUS（可选）
└── MinusToExceptRule            ← 占位符

RuleRegistry                     ← 规则注册表
  _rules: {(Dialect, Dialect): [rule1, rule2, ...]}
  按 (源方言, 目标方言) 维护有序规则链

SQLTranspiler                    ← 编排器（门面模式）
  transpile(sql, source, target) → TranspileResult
  transpile_batch(sqls, source, target) → List[TranspileResult]
```

**RuleRegistry 默认注册：**

- `(SQLite, Oracle)`: JsonExtractToJsonValueRule → RemoveRecursiveKeywordRule
- `(Oracle, SQLite)`: JsonValueToJsonExtractRule → AddRecursiveKeywordRule

用户可通过 `registry.register(source, target, rule)` 动态扩展规则链。

### 9.4 各规则的实现细节

#### 9.4.1 JSON 函数转换

SQLite→Oracle（`JsonExtractToJsonValueRule`）：

- SQLGlot 将 `json_extract(profile, '$.theme')` 解析为 `exp.JSONExtract` 节点
- 规则遍历 AST，替换为 `exp.Anonymous(this='JSON_VALUE', expressions=[col, path_literal])`
- `JSONExtract.expression` 是 `JSONPath` 对象，需要通过 `path.sql().strip("'")` 提取路径字符串后重建为 `Literal.string`

Oracle→SQLite（`JsonValueToJsonExtractRule`）：

- SQLGlot 将 Oracle 的 `JSON_VALUE(profile, '$.theme')` 解析为 `exp.Anonymous(this='JSON_VALUE')`
- 通过匹配 `Anonymous` 节点的函数名来识别
- 不能用 `exp.JSONExtract` 替换，否则 SQLite 方言生成器会输出 `->` 箭头语法
- 使用 `exp.Anonymous(this='json_extract')` 确保输出 `json_extract(...)`

#### 9.4.2 递归 CTE 处理

SQLite→Oracle（`RemoveRecursiveKeywordRule`）：

- 找到 `exp.With` 节点
- 若 `recursive=True`，设为 `False`

Oracle→SQLite（`AddRecursiveKeywordRule`）：

- 需要启发式检测 CTE 是否递归
- 条件一：CTE 体包含 `UNION ALL`（`exp.Union`）
- 条件二：CTE 体引用自身别名（`exp.Table` 中找到与 CTE alias 同名的表引用）
- 两条件满足则设置 `recursive=True`

#### 9.4.3 EXCEPT ↔ MINUS（可选规则）

- Oracle 21c 已原生支持 `EXCEPT`，默认不注册此规则
- 若需兼容旧版 Oracle，可手动注册 `ExceptToMinusRule`
- 规则在 AST 上标记 `meta`，由 `SQLTranspiler` 在生成 SQL 后执行文本替换：`EXCEPT` → `MINUS`

### 9.5 容错设计（面向模糊测试场景）

- 单条规则异常不中断：某条规则 `apply()` 失败时记录警告，继续执行后续规则
- 批量转译单条失败不中断：`transpile_batch()` 中单条失败时返回原 SQL + 警告，继续后续 SQL
- 同方言跳过：`source == target` 时直接返回原 SQL
- 严格解析：解析阶段使用 `ErrorLevel.RAISE`，无法解析则抛出异常，由调用方决定处理策略

### 9.6 验证结果

全部 70 条种子 SQL 的 SQLite→Oracle 转译均成功，关键验证点：

- `json_extract(profile, '$.theme')` → `JSON_VALUE(profile, '$.theme')`
- `WITH RECURSIVE hierarchy AS (...)` → `WITH hierarchy AS (...)`
- `SELECT ... LIMIT 5 OFFSET 2` → `SELECT ... OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY`
- `COALESCE(...)` → `COALESCE(...)`（Oracle 12c+ 支持 COALESCE，SQLGlot 保持不变）
- 反向转译 + 往返测试均通过

## 10. 批量转译功能（transpile 子命令）

### 10.1 功能定位

转译器模块实现了核心算法，但需要一个面向用户的编排层将其串联为可用功能。`BatchTranspileRunner`（位于 `src/core/transpiler/batch_runner.py`）即是这个编排层，负责：

1. 校验输入参数（目录存在性、方言不同等）
2. 递归扫描输入目录收集所有 `.sql` 文件
3. 逐条调用 `SQLTranspiler.transpile()` 执行转译
4. 按原始目录层级写入转译结果
5. 调用 `TranspileReport` 生成 Markdown + JSON 双格式报告

CLI 层（`src/cli.py`）负责参数解析与分发，调用 `BatchTranspileRunner().run()` 并输出汇总日志。

### 10.2 输出目录设计

输出目录命名为 `result/{时间戳}_{源方言}_{目标方言}/`，例如 `result/20260211_201214_sqlite_oracle/`。

设计考量：
- 时间戳前缀保证每次运行不覆盖历史结果，便于对比
- 方言对标识转译方向，一目了然
- 保持与输入目录相同的子目录结构，便于对照原始 SQL

### 10.3 报告设计

每次转译生成两份报告：
- `report.md`: 人类可读的 Markdown 格式，包含汇总表格、失败详情、警告详情、全量文件清单
- `report.json`: 机器可读的 JSON 格式，包含完整结构化数据，可供后续流水线消费

### 10.4 容错策略

- 单条 SQL 转译失败不中断批量处理
- 失败的文件仍然写入输出目录（内容为错误注释 + 原始 SQL），保持文件数量一致
- 报告中详细记录每条失败的原因
