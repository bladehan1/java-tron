# 固定区块导出与离线回放实现及验收

本文记录 `feature/block_apply` 的实现过程和验收过程。问题定义、设计与方案权衡
见 [`DESIGN_BLOCK_APPLY.md`](DESIGN_BLOCK_APPLY.md)。

## 1. 实现过程

### 1.1 分支和基线

| 项目 | 值 |
|---|---|
| 工作目录 | `/Users/blade/java/src/awork/java-tron` |
| 分支 | `feature/block_apply` |
| 父分支 | `feat/db_metric` |
| 父分支 HEAD | `1c4f9ae35e docs(metrics): document db benchmark design` |
| 网络处理 | replay 强制禁用 P2P |
| 当前状态 | 实现与本地验收完成 |

用户最初指定 `feat/block_apply`。仓库贡献约定要求功能分支使用 `feature/*`，因此
实际创建 `feature/block_apply`，并从已完成的 DB metrics 分支继续开发，使离线
回放可以直接采集上一阶段新增的数据库指标。

### 1.2 现有能力调查

实现前确认了以下现有入口：

- `block-index`：高度到当前主链 block ID；
- `block`：block ID 到原始 `Protocol.Block` protobuf；
- `BlockCapsule(Block)`：解析区块并计算 block ID；
- `Manager.pushBlock`：执行共识、交易、VM、maintenance 和持久化；
- `TronNetDelegate.processBlock(block, true)`：真实同步区块处理入口；
- Toolkit picocli 命令框架：适合增加导出命令；
- FullNode fat jar：可以通过 classpath 直接运行独立 replay main class。

最初原型直接调用 `Manager.pushBlock`。与已有性能设计复核后，改为
`TronNetDelegate.processBlock(block, true)`，以保留同步入口锁、fresh-block cache
以及 `block_process_latency{sync=true}` 指标。

### 1.3 公共文件格式

新增：

```text
common/src/main/java/org/tron/common/utils/BlockFile.java
```

实现内容：

- `TRONBLK1` magic 和 version 1；
- start、end、count 文件头；
- height、32-byte block ID、length、protobuf、CRC32 记录；
- 64 MiB 单块大小上限；
- 流式 `RecordSource` writer；
- 流式 `Reader`；
- 高度、protobuf 高度、父块、CRC、截断和尾随数据校验；
- 同目录临时文件和原子移动；
- 默认禁止覆盖，失败清理临时文件。

Writer 和 Reader 位于 common，使 Toolkit 和 FullNode 使用同一协议实现。

### 1.4 Toolkit 导出命令

新增：

```text
plugins/src/main/java/common/org/tron/plugins/DbBlock.java
plugins/src/main/java/common/org/tron/plugins/DbBlockExport.java
```

并在 `Db` 根命令注册：

```text
Toolkit.jar db block export
```

实现流程：

1. 规范化 database directory；
2. 在打开数据库前确认 `block`、`block-index` 目录存在；
3. 按 height 从 `block-index` 读取 ID；
4. 按 ID 从 `block` 读取 protobuf；
5. 交给 `BlockFile.write` 做格式和连续性校验；
6. 成功后输出导出范围、数量和目标路径；
7. finally 中关闭两个数据库。

该实现不会遍历 raw block 库猜测主链，也不会在路径错误时静默创建空数据库。

### 1.5 FullNode 离线回放命令

新增：

```text
framework/src/main/java/org/tron/program/BlockReplay.java
```

运行方式：

```bash
java -cp framework/build/libs/FullNode.jar org.tron.program.BlockReplay [options]
```

命令分两种模式：

#### verify-only

不带 `--apply`：

- 只加载 config 和区块文件；
- 流式解析全部记录；
- 重新计算 block ID；
- 不创建 Spring context；
- 不打开或修改 D0。

#### apply

带 `--apply`：

- 要求 `--output-directory` 已存在；
- 向 Args 注入 `--p2p-disable true`；
- 初始化 metrics 和 Spring context；
- 获取 `TronNetDelegate`、`ChainBaseManager`；
- 校验文件 start 和 D0 head；
- 逐块调用 `processBlock(block, true)`；
- 每块后校验 D0 head；
- 最终关闭 context 和数据库。

### 1.6 预热和计时

支持：

| 参数 | 作用 |
|---|---|
| `--warmup-blocks N` | 前 N 块正常应用，但不计入时间 |
| `--max-blocks N` | 最多处理文件前 N 块，用于 preflight |

计时使用 `System.nanoTime()`，只覆盖同步处理入口。输出用 `Locale.ROOT` 格式化，
避免不同系统小数点格式影响自动脚本。

### 1.7 文档

[`README.md`](README.md) 已补充：

- fat jar 构建命令；
- D1 导出命令；
- verify-only 命令；
- D0 apply 命令；
- `database directory` 与 `output directory` 的区别；
- overwrite、D0 head、warmup、max-blocks 和计时边界；
- 每轮必须恢复 D0 新副本的要求。

### 1.8 实现中遇到的问题

#### 分支命名

用户指定 `feat/block_apply`，但 java-tron 本地约定要求 `feature/<description>`。
最终使用 `feature/block_apply`，功能基线仍来自 `feat/db_metric`。

#### Toolkit 模块不依赖 framework

Toolkit 适合直接读取数据库，但不能调用 FullNode 的 Manager。没有为了复用命令
而把 framework 整体引入 plugins；文件协议下沉 common，导出和回放分别位于其
自然模块中。

#### Manager 与同步入口

第一版 replay 直接调用 `Manager.pushBlock`。它能执行核心业务，但没有覆盖
`TronNetDelegate` 的同步锁、fresh block cache 和 block process latency 指标。
最终改为 `processBlock(block, true)`。

#### Checkstyle 任务差异

首次组合验收尝试执行 `:common:checkstyleMain`，但 common 模块没有注册该任务，
Gradle 在测试前停止。这不是源码失败。最终按模块实际能力执行：common 编译和
单测，plugins/framework 执行 Checkstyle。

#### Mockito 对 BlockCapsule 的比较

测试最初按对象实例 verify `Manager.pushBlock`，而文件读取会创建等价但不同实例
的 `BlockCapsule`，导致 Mockito 参数比较失败。测试改用 captor 比较 block ID，
随后又随同步入口调整为验证 `TronNetDelegate.processBlock(block, true)`。

## 2. 验收过程

### 2.1 验收层级

```text
BlockFile 格式单测
  → Toolkit 真实 RocksDB 导出
  → replay 文件校验
  → sync path 调用和 D0 门禁测试
  → Checkstyle
  → Toolkit/FullNode fat jar 构建
  → fat jar 类和 CLI 实际检查
```

### 2.2 编译验收

执行：

```bash
./gradlew -g /private/tmp/java-tron-gradle-home \
  :common:compileJava \
  :plugins:compileJava \
  :framework:compileJava \
  :common:compileTestJava \
  :plugins:compileTestJava \
  :framework:compileTestJava
```

结果：`BUILD SUCCESSFUL`。

### 2.3 BlockFile 测试

测试类：

```text
org.tron.common.utils.BlockFileTest
```

覆盖：

- 连续区块写入和读取；
- header 的 start/end/count；
- block ID 和 protobuf 高度；
- CRC 损坏检测；
- 默认禁止覆盖已有文件。

### 2.4 Toolkit 导出测试

测试类：

```text
org.tron.plugins.DbBlockExportTest
```

测试使用临时真实 RocksDB：

1. 创建 `block-index` 和 `block`；
2. 写入两个连续区块；
3. 关闭数据库；
4. 通过 `new CommandLine(new Toolkit())` 执行完整导出命令；
5. 用 `BlockFile.Reader` 验证文件区间和记录。

结果：通过。

### 2.5 Replay 测试

测试类：

```text
org.tron.program.BlockReplayTest
```

覆盖：

- verify 模式完整读取连续文件；
- 从真实命令行参数进入 verify 模式；
- apply 模式逐块调用 `TronNetDelegate.processBlock(block, true)`；
- 应用后 head 高度和 ID 校验；
- D0 head ID 不匹配时在处理首块前拒绝；
- warmup 块不计入 measured 统计。

结果：4 个测试全部通过。

### 2.6 聚焦测试和 Checkstyle

最终执行：

```bash
./gradlew -g /private/tmp/java-tron-gradle-home \
  :common:test --tests org.tron.common.utils.BlockFileTest \
  :plugins:test --tests org.tron.plugins.DbBlockExportTest \
  :framework:test --tests org.tron.program.BlockReplayTest \
  :plugins:checkstyleMain \
  :plugins:checkstyleTest \
  :framework:checkstyleMain \
  :framework:checkstyleTest \
  :plugins:buildToolkitJar \
  :framework:buildFullNodeJar
```

结果：`BUILD SUCCESSFUL in 1m 21s`。

### 2.7 Fat jar 验收

确认下列类实际进入构建产物：

```text
plugins/build/libs/Toolkit.jar
  org/tron/common/utils/BlockFile*.class
  org/tron/plugins/DbBlock.class
  org/tron/plugins/DbBlockExport.class

framework/build/libs/FullNode.jar
  org/tron/common/utils/BlockFile*.class
  org/tron/program/BlockReplay*.class
```

实际运行以下帮助命令均成功：

```bash
java -jar plugins/build/libs/Toolkit.jar db block export --help

java -cp framework/build/libs/FullNode.jar \
  org.tron.program.BlockReplay --help
```

### 2.8 已通过的验收项

| 验收项 | 状态 | 证据边界 |
|---|---|---|
| 版本化文件格式 | 通过 | common round-trip 测试 |
| 流式处理 | 通过 | writer/reader 不保存完整区间 |
| CRC 损坏检测 | 通过 | 字节篡改测试 |
| 默认不覆盖 | 通过 | existing-file 测试 |
| 主链来源 | 通过 | Toolkit 按 block-index 取 ID |
| 真实 RocksDB 导出 | 通过 | plugins 临时库 CLI 测试 |
| verify-only | 通过 | direct + command-line 测试 |
| computed block ID | 通过 | replay 校验路径 |
| D0 head 防误用 | 通过 | mismatched-head 测试 |
| 同步业务入口 | 通过 | processBlock(block, true) captor |
| warmup/max-blocks | 通过 | 参数和统计逻辑测试 |
| Checkstyle | 通过 | plugins/framework main/test |
| fat jar | 通过 | 构建、jar 内容和 help 命令 |
| diff 格式 | 通过 | `git diff --check` |

### 2.9 尚未完成的真实数据验收

本轮没有可用的真实停止 D1 和一次性 D0 副本，因此以下工作尚未执行：

- 从真实主链 D1 导出数百或数千个历史区块；
- 用 FullNode fat jar 对该文件执行完整 verify；
- 在真实 D0 副本上完成 100～1,000 块 apply preflight；
- 验证包含真实交易、合约和 maintenance block 的状态执行；
- 对比最终 head、block ID、错误数和 Prometheus 指标；
- 执行 A/B、固定 peer 同步或 ABBA；
- 测量工具文件读取与实际区块处理之外的环境开销。

因此当前可以接受的结论是：

> 固定区块文件、Toolkit 导出和 FullNode 离线 replay 的代码闭环已经实现，合成
> 数据、临时真实 RocksDB、同步入口、D0 门禁和 fat jar 命令验收通过。

当前不能接受的结论是：

> 工具已经在真实历史窗口证明某个数据库方案更快，或离线结果可以替代真实
> P2P 同步结果。

### 2.10 下一步真实验收

1. 正常停止一个覆盖目标高度的 D1；
2. 导出包含普通区块、合约密集区块和 maintenance block 的固定窗口；
3. 执行 verify-only 并保存文件 hash、范围和大小；
4. 从同一 D0 生成 A、B 一次性副本；
5. 两边先执行 100～1,000 块 `--max-blocks` preflight；
6. preflight 通过后执行固定 warmup 和 measurement 窗口；
7. 对齐 DB metrics、业务 metrics、CPU、GC 和磁盘指标；
8. 只有出现明显差异并确定 Candidate 后，再补反向轮次形成 ABBA；
9. 最后用固定单 peer 的真实同步确认端到端方向。
