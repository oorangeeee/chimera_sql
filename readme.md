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

## 🚀 核心特性

- **基于 AST 的智能变异**: 不同于传统的随机字符串生成，本项目深入 SQL 语法树结构，进行边界值注入、逻辑算子翻转、函数嵌套等精准变异，批量生成高质量模糊测试用例。
- **跨方言自动转译**: 基于 SQLGlot 引擎，将通用 SQL 语法的变异用例自动转换为 Oracle、SQLite 等目标方言，实现"一次变异，多库测试"。
- **通用数据库接口**: 遵循 **Python DB-API 2.0 (PEP 249)** 标准，设计了类 JDBC 的统一抽象接口，支持插件式扩展新的数据库驱动。
- **自动化差分验证**: 自动执行、对比、归一化结果集，精准识别数据库间的逻辑不一致性和崩溃缺陷。

## 🛠️ 技术栈

- **语言**: Python 3.10+
- **核心引擎**: [sqlglot](https://github.com/tobymao/sqlglot) (AST 解析与转译)
- **数据库驱动**: `oracledb` (Oracle), `sqlite3` (内置)
- **配置管理**: PyYAML
- **部署架构**: Docker (Oracle XE), 本地运行 (Python Client)

## 📂 项目结构

```text
ChimeraSQL/
├── config/                     # 配置文件 (config.yaml)
├── data/
│   ├── seeds/                  # 种子 SQL（按类别分子目录，共 8 类约 50 个）
│   │   ├── 01_basic_select/
│   │   ├── 02_aggregation/
│   │   ├── 03_join/
│   │   ├── 04_subquery/
│   │   ├── 05_set_operations/
│   │   ├── 06_window_functions/
│   │   ├── 07_null_handling/
│   │   └── 08_expressions/
│   └── logs/                   # 运行日志
├── src/
│   ├── connector/              # 数据库连接层 (工厂模式实现)
│   ├── core/                   # 核心逻辑
│   │   ├── schema_initializer.py  # 测试表 DDL 初始化（5 张表）
│   │   ├── data_populator.py      # 测试数据填充（含边界值/NULL）
│   │   ├── seed_generator.py      # 种子 SQL 文件生成
│   │   ├── mutator/            # AST 变异策略 (策略模式实现)
│   │   └── transpiler/         # 方言转译器
│   ├── analyzer/               # 结果对比与分析
│   └── utils/                  # 工具类 (ConfigLoader, Logger)
├── tests/                      # 单元测试
├── docker-compose.yml          # 数据库环境部署
├── requirements.txt            # 依赖列表
└── main.py                     # 启动入口（三阶段流水线）
```
