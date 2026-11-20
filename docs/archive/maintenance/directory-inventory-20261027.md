# Archive相关目录文件数盘点（2026-10-27）

口径：以 `origin/develop` 共同祖先 `b33eed89a6a424c498d4fb1b03ca2c86eddf4840` 对比当前工作副本；统计每个目录的直接文件，
不将子目录递归累加。生产和测试分开；不计 `.helper`、build输出及无关p2p目录。
“改动文件”指整个Archive分支相对该基线的改动，不只本轮搬迁。
实际路径为 `db2/stateroot`，与 `db2/archive` 同级。

## 专用代码目录中超过10个文件的清单

| 目录（省略Java包前缀org/tron/core） | 拆分前 | 当前 | 当前Java文件 |
| --- | ---: | ---: | ---: |
| `chainbase/src/main/java/db2/stateroot` | 52 | 22 | 22 |
| `framework/src/test/java/db2/stateroot` | 29 | 18 | 18 |
| `chainbase/src/main/java/db2/archive/history/store` | 18 | 17 | 17 |
| `chainbase/src/main/java/db2/archive/history/v5` | 14 | 14 | 14 |
| `framework/src/test/java/db2/archive/history/v5` | 14 | 14 | 14 |
| `framework/src/test/java/db2/archive/serving` | 14 | 12 | 12 |
| `chainbase/src/main/java/db2/archive/serving` | 22 | 11 | 11 |

## 已实施的stateroot分层（含后续流式trie批次）

- `commitment`：8个participant、编码、canonicalization和transition输入合同。
- `runtime`：4个head接口、准入、attachment和异步准备入口。
- `rebuild`：1个原生快照来源；重建coordinator等仍待清理包内依赖。
- `storage`：具体native driver、perf采样、通用原生协议与缓存/预算策略4文件；具体引擎及句柄仍包内。
- `format`：3个物理/通用manifest、engine格式及根元数据声明。
- `trie`：4个文件，节点合同、流式构建入口与两个包内实现。
- `physical`：8个文件，4个包内记录codec、只读记录合同、准入、snapshot head和受限存储合同。
- `metrics`：3个文件，包内采样/指标实现与观测入口。
- 根生产目录52→35→31→26→24→22，总生产Java52→57；新增native协议、流式构建入口、physical只读记录、head存储合同与观测入口。
- 当前根22项仍含trie、physical和layer publication私有协作，下一批继续按这些职责拆分。

10是本次盘点阈值。`serving`、共同checkpoint、旧history Store和V5仍是后续细分候选；
实际拆分要维持所有权和依赖方向，避免通过整体public或仅给大目录改名完成。

## 超过10个文件且被Archive改动触及的宿主/文档目录

这些目录包括既有通用组件。下表同时列出实际改动量，便于判断是否应在Archive切片中处理。

| 目录 | 直接文件 | Java | Archive分支改动文件 |
| --- | ---: | ---: | ---: |
| `framework/src/main/java/org/tron/core/services/http` | 136 | 136 | 1 |
| `framework/src/test/java/org/tron/core/db` | 59 | 59 | 3 |
| `actuator/src/main/java/org/tron/core/actuator` | 42 | 42 | 1 |
| `common/src/main/java/org/tron/core/exception` | 41 | 41 | 1 |
| `chainbase/src/main/java/org/tron/core/store` | 39 | 39 | 4 |
| `common/src/main/java/org/tron/common/utils` | 35 | 35 | 1 |
| `chainbase/src/main/java/org/tron/core/capsule` | 33 | 33 | 1 |
| `.` | 24 | 0 | 1 |
| `framework/src/test/java/org/tron/common/utils` | 23 | 23 | 1 |
| `chainbase/src/main/java/org/tron/core/db` | 19 | 19 | 5 |
| `plugins/src/main/java/common/org/tron/plugins` | 15 | 15 | 3 |
| `actuator/src/main/java/org/tron/core/vm` | 13 | 13 | 3 |
| `common/src/main/java/org/tron/core/config/args` | 13 | 13 | 2 |
| `framework/src/test/java/org/tron/core` | 13 | 13 | 1 |
| `framework/src/test/java/org/tron/core/jsonrpc` | 13 | 13 | 1 |
| `chainbase/src/main/java/org/tron/core/db2/common` | 12 | 12 | 3 |
| `common/src/test/java/org/tron/core/config/args` | 12 | 12 | 1 |
| `framework/src/test/java/org/tron/core/services/jsonrpc` | 11 | 11 | 2 |
| `plugins/src/test/java/org/tron/plugins` | 11 | 11 | 1 |
