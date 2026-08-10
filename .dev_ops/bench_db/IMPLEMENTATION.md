# DB Metrics 实现与验收记录

本文归纳 `feat/db_metric` 优化工作的后两个部分：实现过程与验收过程。问题定义、
设计和方案权衡见 [`DESIGN.md`](DESIGN.md)。

## 1. 实现过程

### 1.1 分支和提交

| 项目 | 值 |
|---|---|
| 工作目录 | `/Users/blade/java/src/awork/java-tron` |
| 分支 | `feat/db_metric` |
| 本次同步基线 | `upstream/develop` at `4a21592f95` |
| develop merge commit | `fdcf7f9673` |
| 优化 commit | `4e514b15fd61b942be863452b16f10fdc8d7ce3d` |
| commit subject | `feat(metrics): make db metrics benchmark-ready` |
| 提交规模 | 20 files, 454 insertions, 48 deletions |
| 推送状态 | 未推送；分支没有 tracking branch |

`.dev_ops/bench_db` 由 `.git/info/exclude` 排除，本文、`DESIGN.md` 和 `README.md`
都是本地操作文档，不在上述 commit 中。

### 1.2 同步最新 develop

优化前，本地 `feat/db_metric` 相对当时最新 `upstream/develop` 落后 45 个提交、
领先 7 个指标提交。先刷新 upstream，再使用仓库要求的 `--no-ff` 合并：

```bash
git fetch upstream
git merge upstream/develop --no-ff
```

合并无源码冲突，生成 `fdcf7f9673`。最终优化 commit 后，相对该
`upstream/develop` 为 `0 behind / 9 ahead`，9 个 ahead 包括原 7 个指标提交、
develop merge commit 和本次优化 commit。

### 1.3 配置实现

新增配置 Bean：

```text
MetricsConfig
└─ PrometheusConfig
   └─ DatabaseConfig
      ├─ enable=false
      └─ statIntervalSeconds=30
```

涉及文件：

- `common/.../MetricsConfig.java`：配置绑定和 5～3600 秒边界校验；
- `CommonParameter.java`：运行期配置字段；
- `framework/.../Args.java`：配置桥接，并按开关启用 RocksDB Statistics；
- `reference.conf`、`framework/config.conf`：默认配置和示例。

DB 指标最终生效条件：

```java
metricsPrometheusEnable && metricsPrometheusDatabaseEnable
```

### 1.4 热点路径实现

新增 `DbOperationMetrics`，在 LevelDB/RocksDB datasource 构造时预绑定：

- get latency / bytes；
- put latency / bytes；
- delete latency；
- batch latency / bytes。

原调用方式：

```text
每次操作 → metric key lookup → labels(engine, db, op) → child → observe
```

优化后：

```text
DB open → 一次性绑定 child
每次操作 → cached child → timer/observe
```

database metrics 关闭时所有 child 为 `null`，不计算 batch payload 总量，也不访问
Prometheus child。

### 1.5 周期状态实现

`DbStatService` 从固定 6 小时调整为配置驱动的秒级周期，只在 database metrics
开启时注册任务。

RocksDB datasource 的 `stat()` 依次采集：

1. SST level 和 size；
2. RocksDB memory；
3. compaction/flush/write pressure properties；
4. Statistics ticker delta。

LevelDB logger event counter 也受 database metrics 子开关控制，避免只开启普通
Prometheus 时继续解析事件并更新 Counter。

### 1.6 RocksDB Statistics 实现

`RocksDbSettings` 在 database metrics 开启时：

- 创建 Statistics；
- 设置 `EXCEPT_DETAILED_TIMERS`；
- 注入 Options；
- 关闭 stats dump；
- 释放设置阶段的临时 Java wrapper。

`RocksDbDataSourceImpl` 在打开 DB 后获取并缓存一个 Statistics wrapper。每次
`stat()` 读取选定 ticker 的当前累计值，与上次快照求 delta，再增加 Prometheus
Counter。

实现过程中发现 `options.statistics()` 每次调用都会创建新的 Java/native wrapper。
如果直接在 30 秒周期任务中调用而不关闭，会形成长期 native wrapper 泄漏。因此
最终实现只在 DB open 时调用一次，并在 close 时显式释放。

### 1.7 Histogram 和指标文档

DB latency buckets 从最高 10ms 扩展到 1s，新增 20ms、50ms、100ms、500ms 和
1s 等区间。

`METRICS_CHANGELOG.md` 补充：

- 独立配置示例；
- metrics-off/metrics-on 控制要求；
- RocksDB property 和 ticker 定义；
- shared cache 不应直接跨 DB 求和；
- logical payload/bytes 不能替代 OS 物理磁盘指标。

### 1.8 实现中遇到的问题

#### Worktree 与 JGit

最初为了保护其他分支工作区，在 `/private/tmp/java-tron-db-metric` worktree 中修改。
源码编译可以通过，但 `framework:generateGitProperties` 使用的 JGit 无法识别：

```text
.git/worktrees/java-tron-db-metric
```

导致 Checkstyle 前置任务失败。曾使用普通 `/private/tmp` clone 做验证；随后按用户
要求将主目录直接切换到 `feat/db_metric`，把全部修改迁回主目录继续工作。

#### 沙箱内 Gradle daemon

一次 Checkstyle 执行因沙箱禁止 Gradle daemon 绑定本地 socket 而失败。该失败
发生在 Gradle 启动阶段，不是源码或测试失败；在允许的执行环境中重跑成功。

#### Native Statistics wrapper

第一版 ticker 采集每次调用 `options.statistics()`。代码复核和 JNI bytecode 检查
确认它会返回新 wrapper，因此改为 open 时获取一次、close 时释放。随后增加真实
RocksDB ticker 导出测试，覆盖该路径。

#### LevelDB 文件换行

`LevelDbDataSourceImpl.java` 原文件使用 CRLF。修改时保留原有文件风格并确保
`git diff --check` 无新增 trailing whitespace，避免把整个文件变成无关换行 diff。

## 2. 验收过程

### 2.1 验收层级

本次只验收指标实现和最小采集链路：

```text
配置绑定
  → Java 编译
  → Checkstyle
  → 指标开关/Histogram 单测
  → LevelDB/RocksDB datasource 回归
  → 真实 RocksDB read/write/stat/ticker preflight
```

本次不包含固定 D0/D1 区块同步，因此验收结果不能解释为数据库配置性能提升。

### 2.2 编译

执行：

```bash
./gradlew -g /private/tmp/java-tron-gradle-home \
  :common:compileJava \
  :chainbase:compileJava \
  :framework:compileJava
```

结果：`BUILD SUCCESSFUL`。

覆盖配置 Bean、Prometheus 公共类、LevelDB/RocksDB datasource 和 framework Args
桥接的编译依赖。

### 2.3 配置测试

执行：

```bash
./gradlew -g /private/tmp/java-tron-gradle-home :common:test \
  --tests org.tron.core.config.args.MetricsConfigTest \
  --tests org.tron.core.config.args.ConfigParityGateTest
```

结果：`BUILD SUCCESSFUL`。

验证内容：

- database metrics 默认关闭；
- 默认周期为 30 秒；
- benchmark 可配置为 10 秒；
- 小于 5 秒的配置被拒绝；
- `reference.conf` 和配置 Bean 字段完整对齐。

### 2.4 指标和 datasource 测试

执行：

```bash
./gradlew -g /private/tmp/java-tron-gradle-home :framework:test \
  --tests org.tron.common.storage.metric.DbOperationMetricsTest \
  --tests org.tron.common.storage.rocksdb.RocksDbDataSourceImplTest \
  --tests org.tron.common.storage.leveldb.LevelDbDataSourceImplTest
```

结果：全部通过。

验证内容：

- 全局 Prometheus 开启但 database metrics 关闭时，不创建 DB timer；
- database metrics 开启时，预绑定 latency/bytes child 正常累加；
- LevelDB datasource 打开、读写、engine 检查和 watchdog 回归；
- RocksDB datasource 打开、读写、backup 和 engine 检查回归；
- 真实 RocksDB 临时库执行 put/get 后调用 `stat()`；
- Prometheus 能读到 `number_keys_written >= 1`；
- datasource 关闭后 Statistics/Options 资源释放路径无异常。

### 2.5 Checkstyle 和 diff

执行：

```bash
./gradlew -g /private/tmp/java-tron-gradle-home \
  :framework:checkstyleMain \
  :framework:checkstyleTest

git diff --check
```

结果：全部通过。

曾出现一次测试 import 顺序告警，调整 `CollectorRegistry` import 后重跑通过。

### 2.6 已通过的验收项

| 验收项 | 状态 | 证据边界 |
|---|---|---|
| 最新 develop 合并 | 通过 | 本地 `upstream/develop` at `4a21592f95` |
| DB 指标独立开关 | 通过 | 配置及关闭路径单测 |
| 周期配置 | 通过 | 默认、覆盖和非法边界测试 |
| 热点 label 预绑定 | 通过 | Histogram count 单测 |
| LevelDB 回归 | 通过 | 聚焦 datasource 测试 |
| RocksDB 回归 | 通过 | 聚焦 datasource 测试 |
| RocksDB ticker 导出 | 通过 | 真实临时库 put/get/stat preflight |
| Native wrapper 生命周期 | 通过 | 单次 wrapper 设计、关闭路径和 preflight |
| 配置 parity | 通过 | `ConfigParityGateTest` |
| Checkstyle | 通过 | main/test tasks |
| 提交规范 | 通过 | `4e514b15fd feat(metrics): make db metrics benchmark-ready` |

### 2.7 尚未完成的性能验收

以下工作明确未执行：

- metrics-off 与 metrics-on 的实际 CPU/block、latency、blocks/s 开销对照；
- 同一 D0 上固定区块离线回放 A/B；
- 固定单 peer 的真实同步 A/B；
- flush/compaction 确实发生时的 Bloom/block size 评价；
- amd64 JNI 5.15.10 与 aarch64 JNI 9.7.4 的同窗口对照；
- 出现明显差异后的 ABBA 反向复测；
- Arthas/async-profiler 差异归因。

因此当前可接受的结论仅为：

> `feat/db_metric` 已具备用于受控快速实验的配置、操作指标、RocksDB 内部指标和
> 最小采集链路，且聚焦编译、风格、配置和 datasource 回归通过。

当前不能接受的结论是：

> 某个数据库或 RocksDB 配置已经获得确定的性能提升。

### 2.8 下一阶段验收顺序

按低成本到高成本执行：

1. A、B 各回放 100～1,000 块 preflight，验证构建、D0/D1、指标和正常关闭；
2. 相同方案做 metrics-off/metrics-on，得到观测成本；
3. 离线固定区块 `A1 → B1`，筛选 DB 自身差异；
4. 固定 peer 同步 `A1 → B1`，确认端到端方向；
5. 只有出现明显差异且 Candidate 基本确定后，补 `B2 → A2` 形成 ABBA；
6. 只有差异来源不清时再采集 Arthas/async-profiler。

若 Candidate 在预热阶段失败、没有进入固定测量窗口，结果必须记录为“测量前
失败”，不能计算性能退化百分比。
