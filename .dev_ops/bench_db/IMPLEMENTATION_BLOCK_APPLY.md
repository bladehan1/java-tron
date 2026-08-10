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
| 当前状态 | 首次真实 apply 暴露生命周期缺陷；代码修复完成，真实 D0 复验待执行 |

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

`context.refresh()` 不会执行正常 FullNode 的 `ApplicationImpl.startup()`，因此 replay 还需
显式启动 `ConsensusService`，并在关闭数据库前停止和等待 `RewardViCalService` 后台任务。

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

#### 首次真实 apply 的 Consensus 初始化失败

首次在真实 D0 上应用第一块时失败。代码级原因是：

1. `BlockReplay.apply()` 只执行 `context.refresh()`；
2. 随后第一块直接进入 `TronNetDelegate.processBlock()`；
3. `Consensus` 使用的 `consensusInterface` 只会在 `ConsensusService.start()` 调用
   `Consensus.start()` 后完成赋值；
4. replay 没有调用该启动链路，第一块因此在共识处理阶段失败。

原设计中“Spring context 已完成 Consensus 初始化”的表述不成立，现已修正。实现改为在
`refresh()` 后、取得区块处理 Bean 和处理第一块前显式执行
`ConsensusService.start()`。未调用完整 `Application.startup()`，避免为离线 benchmark
启动普通 API 服务；P2P 仍由 `--p2p-disable true` 强制禁用。

#### 异常退出时 RocksDB JNI SIGSEGV

主流程异常后关闭 context，又暴露了独立的关闭顺序问题：

```text
旧顺序：Manager.close() -> RocksDB close -> Spring @PreDestroy -> reward thread stop
```

`RewardViCalService` 可能仍在遍历 delegation/witness/reward RocksDB，底层数据库先关闭会与
native iterator 竞态。实际崩溃报告为：

```text
/data/blade/node_mainnet/hs_err_pid161196.log
```

修复内容：

- 为 `RewardViCalService` 增加可重复调用的 `stop()`；
- `stop()` 先设置停止标志并中断 executor，再等待线程退出；
- witness/reward iterator 改为 try-with-resources，确保 native iterator 及时关闭；
- 长 cycle 和 iterator 循环增加协作式停止检查；
- `Manager.close()` 在 `chainBaseManager.shutdown()`/RocksDB 关闭前调用该 `stop()`；
- Spring 后续再次执行 `@PreDestroy` 时保持幂等。

新的关键顺序为：

```text
Consensus stop -> reward thread stop and await -> RocksDB close -> bean destroy
```

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
- replay apply 初始化阶段显式启动 `ConsensusService`。

结果：`BlockReplayTest` 新增 Consensus 生命周期用例后共 5 个测试，全部通过。

关闭竞态另新增：

```text
org.tron.core.service.RewardViCalServiceLifecycleTest
```

测试向 reward executor 提交一个阻塞任务，再调用两次 `stop()`，验证工作线程收到 interrupt、
`stop()` 等待 executor 完全终止且重复调用安全。该用例通过。

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

生命周期缺陷修复后执行：

```bash
./gradlew -g /private/tmp/java-tron-gradle-home \
  :framework:test \
    --tests org.tron.program.BlockReplayTest \
    --tests org.tron.core.service.RewardViCalServiceLifecycleTest \
  :framework:checkstyleMain \
  :framework:checkstyleTest \
  :framework:buildFullNodeJar
```

结果：6 个聚焦测试全部通过，Checkstyle 和 FullNode fat jar 构建通过，
`BUILD SUCCESSFUL in 56s`。

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
| Consensus 启动门禁 | 通过 | replay 初始化显式调用 ConsensusService.start() |
| reward 关闭等待 | 通过 | 阻塞任务被中断，executor terminated，stop 可重复调用 |
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

首次真实 apply 证明原实现不能接受“真实 D0 回放闭环已经完成”的结论。完成生命周期修复后，
当前可以接受的结论收缩为：

> 固定区块文件、Toolkit 导出、同步入口和 D0 门禁已实现；Consensus 启动遗漏和 reward
> 后台线程关闭竞态已完成代码修复及聚焦测试，但必须再次在真实 D0 副本上 apply，才能恢复
> “真实回放闭环通过”的结论。

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
