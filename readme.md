# ChimeraSQL: Cross-Database Fuzzing Tool

![Python 3.10](https://img.shields.io/badge/Python-3.10%2B-blue.svg)
![Status](https://img.shields.io/badge/Status-Development-orange.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

**ChimeraSQL** 是一个面向关系型数据库（DBMS）的模糊测试（Fuzzing）用例生成与验证工具。

本项目基于 **AST（抽象语法树）变异** 技术，自动生成高覆盖率的测试用例，利用 **SQLGlot** 实现跨数据库方言转换，并通过 **差分测试（Differential Testing）** 机制，在 Oracle 和 SQLite 等异构数据库之间检测逻辑不一致性（Logic Bugs）或系统崩溃（Crash Bugs）。

> **学术背景**: 本项目为本科毕业设计课题《跨数据库模糊测试用例生成工具的设计与实现》的工程实现。

## 🔄 端到端工作流程

ChimeraSQL 的最终形态是一个**模糊测试一站式平台**，完整流水线如下（文本版）：

1. 种子 SQL（Seeds）
   高质量查询作为输入基线。
2. AST 变异引擎（Mutator）
   执行边界值注入、NULL 替换、算子翻转、函数嵌套、条件变异等策略，生成大量变异 SQL。
3. 方言转译器（Transpiler）
   基于 SQLGlot，将通用 SQL 转译为 Oracle 与 SQLite 等目标方言。
4. 数据库执行
   在 Oracle XE 与 SQLite 中分别执行变异 SQL。
5. 差分分析器（Analyzer）
   对结果集进行归一化与对比，发现行为差异。
6. 缺陷报告（Bug Report）
   输出可追踪的差异与崩溃信息。

**核心价值**在于中间三个阶段：AST 变异引擎（生成大量有效的模糊测试用例）、方言转译器（跨数据库兼容）、差分分析器（自动发现行为差异）。

**解耦设计**：变异模块与转译模块严格解耦。默认流程为“先变异、后转译”，但两者可独立运行。

## 🚀 核心特性

- **基于 AST 的智能变异**: 不同于传统的随机字符串生成，本项目深入 SQL 语法树结构，进行边界值注入、逻辑算子翻转、函数嵌套等精准变异，批量生成高质量模糊测试用例。变异策略分为通用（Generic）、结构（Structural）、方言感知（Dialect-Aware）、差分专项（Differential）四大类。
- **能力画像驱动的泛用设计**: 变异引擎与目标数据库严格解耦——每条变异策略声明所需能力，调度器通过"能力画像"（Capability Profile）门控决定是否启用。画像基线由 SQLGlot 内置的 150+ 方言标志自动提取，覆盖 32 种数据库；上叠可选的 YAML 覆盖配置。**新增数据库支持仅需 YAML 配置，无需修改变异逻辑代码。**
- **跨方言自动转译**: 基于 SQLGlot + 自定义规则引擎，将通用 SQL 语法自动转换为 Oracle、SQLite 等目标方言，实现"一次变异，多库测试"。支持批量转译整个目录并生成详细报告。
- **变异/转译解耦**: 允许先执行变异再转译的标准流程，也允许仅运行 seeds 的原始回归测试。
- **通用数据库接口**: 遵循 **Python DB-API 2.0 (PEP 249)** 标准，设计了类 JDBC 的统一抽象接口，支持插件式扩展新的数据库驱动。
- **自动化差分验证**: 自动执行、对比、归一化结果集，精准识别数据库间的逻辑不一致性和崩溃缺陷。

## 📋 使用方式

### 环境准备

```bash
# 激活 Conda 环境
conda activate chimera_sql
```

### 初始化测试基础设施

在 Oracle 和 SQLite 中创建测试表、填充数据、生成种子 SQL：

```bash
python main.py init
```

### 批量方言转译

将指定目录下的所有 SQL 文件从源方言转译为目标方言：

```bash
python main.py transpile <输入目录> -s <源方言> -t <目标方言>
```

**参数说明：**

| 参数 | 说明 |
|------|------|
| `<输入目录>` | 包含 .sql 文件的目录（递归扫描所有子目录） |
| `-s / --source` | 源 SQL 方言（`sqlite` 或 `oracle`） |
| `-t / --target` | 目标 SQL 方言（`sqlite` 或 `oracle`） |

**示例：**

```bash
# 将所有种子 SQL 从 SQLite 方言转译为 Oracle 方言
python main.py transpile data/seeds -s sqlite -t oracle

# 反向：Oracle → SQLite
python main.py transpile data/seeds -s oracle -t sqlite
```

**输出结构：**

```text
result/
└── 20260211_201214_sqlite_oracle/     # {时间戳}_{源}_{目标}
    ├── 01_basic_select/               # 保持原始目录层级
    │   ├── select_all_users.sql       # 转译后的 SQL
    │   └── ...
    ├── 11_json_handling/
    │   └── json_extract_scalar.sql
    ├── report.md                      # Markdown 格式报告
    └── report.json                    # JSON 格式报告（机器可读）
```

### 批量 AST 变异

对指定目录下的种子 SQL 文件执行 AST 级变异，生成模糊测试用例：

```bash
python main.py mutate <输入目录> -d <方言> [-v <版本>] [-n <数量>] [--seed <随机种子>]
```

**参数说明：**

| 参数 | 说明 |
|------|------|
| `<输入目录>` | 包含 .sql 种子文件的目录（递归扫描） |
| `-d / --dialect` | 目标数据库方言（如 `sqlite`, `oracle`, `mysql` 等 SQLGlot 支持的方言） |
| `-v / --version` | 数据库版本（可选，如 `21c`，用于匹配 config.yaml 能力画像） |
| `-n / --count` | 每条种子生成的变异数量（默认从 config.yaml `mutation.policies.balanced_default.max_mutations_per_seed` 读取） |
| `--seed` | 随机种子（可选，用于可复现结果） |

**示例：**

```bash
# 默认变异（每条种子 3 个变异）
python main.py mutate data/seeds -d sqlite

# 指定数量和随机种子（可复现）
python main.py mutate data/seeds -d sqlite -n 5 --seed 42

# 针对 Oracle 21c
python main.py mutate data/seeds -d oracle -v 21c
```

**输出结构：**

```text
result/
└── mutate_20260212_130655_sqlite/     # mutate_{时间戳}_{方言}
    ├── 01_basic_select/               # 保持原始目录层级
    │   ├── where_equality_mut01.sql   # 变异后的 SQL
    │   ├── where_equality_mut02.sql
    │   ├── where_equality_mut03.sql
    │   └── ...
    ├── report.md                      # Markdown 格式报告
    └── report.json                    # JSON 格式报告
```

**内置变异策略（10 个通用策略）：**

| 策略 | 说明 |
|------|------|
| `boundary_injection` | 数字字面量替换为边界值（0/-1/MAX_INT 等） |
| `null_injection` | 列引用或字面量替换为 NULL |
| `predicate_negation` | 比较运算符取反（= ↔ <>, > ↔ <= 等） |
| `logic_tautology` | 注入恒真（OR 1=1）或恒假（AND 1=0）条件 |
| `operand_swap` | 交换二元运算的左右操作数 |
| `aggregate_substitution` | 聚合函数互换（COUNT→SUM 等） |
| `sort_direction_flip` | 排序方向翻转（ASC ↔ DESC） |
| `distinct_toggle` | 切换 SELECT DISTINCT |
| `limit_variation` | 修改 LIMIT/OFFSET 数值 |
| `union_type_variation` | UNION ↔ UNION ALL |

## 🛠️ 技术栈

- **语言**: Python 3.10+
- **核心引擎**: [sqlglot](https://github.com/tobymao/sqlglot) (AST 解析与转译)
- **数据库驱动**: `oracledb` (Oracle), `sqlite3` (内置)
- **配置管理**: PyYAML
- **部署架构**: Docker (Oracle XE), 本地运行 (Python Client)

## 📂 项目结构

```text
ChimeraSQL/
├── config/
│   ├── config.template.yaml    # 配置模板（提交到仓库）
│   └── config.yaml             # 本地配置（通常不提交）
├── data/
│   ├── seeds/                  # 种子 SQL（按类别分子目录，共 11 类 70 条）
│   ├── logs/                   # 运行日志
├── dockers/
│   └── oracle-docker-compose.yml
├── info/                       # 设计说明与调研文档
├── src/
│   ├── cli.py                  # CLI 参数解析 + 子命令分发 + 顶层错误处理
│   ├── analyzer/               # 分析模块（预留）
│   ├── connector/              # 数据库连接层 (工厂模式实现)
│   ├── core/                   # 核心逻辑（仅变异/转译/差分）
│   │   ├── mutator/            # AST 变异引擎 (策略模式 + 能力画像门控)
│   │   │   ├── strategy_base.py     # MutationStrategy ABC + MutationResult
│   │   │   ├── capability.py        # 能力画像（SQLGlot 自动提取 + YAML 覆盖）
│   │   │   ├── gate.py              # 规则门控（策略 × 能力画像 × 节点类型）
│   │   │   ├── engine.py            # MutationEngine 单条 SQL 变异编排
│   │   │   ├── strategy_registry.py # 策略注册表 + 默认注册工厂
│   │   │   ├── batch_runner.py      # 批量变异编排（文件收集/变异/写入）
│   │   │   ├── report.py            # Markdown + JSON 报告生成
│   │   │   └── strategies/          # 10 个通用变异策略
│   │   ├── transpiler/         # 方言转译器 (规则引擎实现)
│   │   │   ├── batch_runner.py    # 批量转译编排（文件收集/转译/写入）
│   │   │   ├── report.py          # Markdown + JSON 报告生成
│   │   │   ├── dialect.py         # Dialect 枚举
│   │   │   ├── rule_base.py       # TranspilationRule ABC + TranspileResult
│   │   │   ├── rule_registry.py   # 规则注册表 + 默认规则工厂
│   │   │   ├── transpiler.py      # SQLTranspiler 编排器
│   │   │   └── rules/             # 具体转译规则
│   │   │       ├── json_rules.py      # json_extract ↔ JSON_VALUE
│   │   │       ├── recursive_rules.py # WITH RECURSIVE 处理
│   │   │       └── set_op_rules.py    # EXCEPT ↔ MINUS（可选）
│   │   └── differential/       # 差分分析与结果归一化
│   ├── testbed/                # 测试基建（schema/data/seed/pipeline）
│   └── utils/                  # 工具类 (ConfigLoader, Logger)
├── result/                     # 变异/转译输出（自动生成，已 gitignore）
├── tests/                      # 测试脚本
├── tech.md                     # 技术方案文档
├── requirements.txt            # 依赖列表
└── main.py                     # 薄入口（仅调用 src.cli.run()）
```
