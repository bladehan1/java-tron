# Archive目录拆分待办与继续评估（2026-10-27）

本计划承接上次盘点的**全部32个目录**：10个Archive/PathState专用生产或测试目录，
以及22个宿主/文档目录。每行对应TASK-039的一个未完成子项；没有把宿主目录排除在任务之外。
原盘点采用origin/develop共同祖先`b33eed89a6a424c498d4fb1b03ca2c86eddf4840`对比工作副本，
按每个目录直接文件计数，生产和测试独立，不递归累加。原清单冻结用于防遗漏，
后续新增目录也要重新扫描并追加，不能删掉已降到10以下但尚未验收的项。

“纳入任务”包括职责评估、候选迁移和结果核对。每项均保持未完成：需要迁移的落实代码与测试；
有明确保留理由的记录职责、调用边界和替代整改，完成复核后才收口。文件数10是发现阈值，
实际目录按职责与所有权设计。禁止整体public、改变持久化格式或只建立空目录来降数字。

优先级：P0为下一批候选（含随迁测试），P1需要先解决所有权或夹具边界，P2先评估宿主全目录影响。
这是实施排序，不代表P1/P2已获准跳过。现有Gradle模块保持，产品STATE不切换。

## 全量待办

“上期”列保留上一轮扫描值；“本次”包含本计划新增文档，所以docs/archive从14变为15。

| 子项 | 目录 | 上期/最新直接文件 | 优先级 | 待办与初评 | 边界与验收 |
| --- | --- | ---: | --- | --- | --- |
| DIR-01 | `framework/src/main/java/org/tron/core/services/http` | 136 / 136 | P2 | 按账户/区块/交易/隐私等HTTP职责评估目录；先核对FullNodeHttpApiService的Archive路由装配 | URL、注入和过滤器顺序及默认关闭路径；不能只为分包迁移136个servlet |
| DIR-02 | `framework/src/test/java/org/tron/core/db` | 59 / 59 | P1 | 将历史查询准入、内存预算和maintenance夹具按功能评估归位 | 先盘点59项既有测试和同包宿主访问；迁移Archive集成测试时保留完整选例 |
| DIR-03 | `actuator/src/main/java/org/tron/core/actuator` | 42 / 42 | P2 | 评估actuator按执行域分层，先检查VMActuator的Archive接入是否可独立 | Spring发现、执行事务、energy和异常边界；VM入口所有权先核对 |
| DIR-04 | `common/src/main/java/org/tron/core/exception` | 41 / 41 | P2 | 评估异常目录按协议/执行/历史查询职责组织，追踪TronError接入 | 错误码、异常类型和RPC映射兼容；不能仅迁走一个enum宣称41文件目录完成 |
| DIR-05 | `chainbase/src/main/java/org/tron/core/store` | 39 / 39 | P1 | 按持久化Store与编码工具分层评估；优先核对StorageRowKeyCodec归属 | Account/AccountAsset/DynamicProperties仍有宿主状态与P66耦合；数据库名和键格式保持 |
| DIR-06 | `common/src/main/java/org/tron/common/utils` | 35 / 35 | P2 | 评估通用utils内文件、数据库和编码职责，先审查BlockFile | 外部调用、工具语义及路径读写；不要将无关utils一并归因于Archive |
| DIR-07 | `chainbase/src/main/java/org/tron/core/capsule` | 33 / 33 | P2 | 评估capsule职责与BlockCapsule的Archive/root字段接入 | protobuf、区块hash/序列化及共识字段保持；数据模型宿主边界需复核 |
| DIR-08 | `chainbase/src/main/java/org/tron/core/db2/stateroot` | 31 / 22 | P1 | physical记录/准入/head及native budget已迁，继续拆重建会话、snapshot/trie所有权与layer发布退休 | admitSnapshot/publishBase与Store私有成员依赖；禁止批量public；根22项逐簇处理 |
| DIR-09 | `.` | 24 / 24 | P2 | 根24文件按构建入口、贡献规范和导航评估；核对build.gradle专用逻辑能否移入Gradle脚本 | Gradle自动发现及发行入口；不是把整个项目根目录强制降到10项 |
| DIR-10 | `framework/src/test/java/org/tron/common/utils` | 23 / 23 | P2 | 评估utils测试与生产职责对应，审查DBKeyComparatorTest归属 | 包内可见性和既有Gradle选例；生产工具归属确定后再迁测试 |
| DIR-11 | `chainbase/src/main/java/org/tron/core/db2/archive/serving` | 22 / 11 | P0 | index五类和两个History来源合同已迁，继续拆服务生命周期及持久generation簇 | lease释放/计数、native snapshot与generation发布顺序；具体Store仍包内 |
| DIR-12 | `framework/src/test/java/org/tron/core/db2/stateroot` | 21 / 18 | P1 | 与stateroot生产拆分同步规划format/rebuild/snapshot/physical等测试 | 不为获得目录数提前开放故障钩子；30个总测试Java及当前根21的映射和选例均需保持 |
| DIR-13 | `chainbase/src/main/java/org/tron/core/db` | 19 / 19 | P1 | 评估db按资源计费、事务与revoking宿主接入分层 | TronDatabase/TronStoreWithRevoking的事务所有权，Bandwidth/TransactionContext生命周期与P66配对 |
| DIR-14 | `chainbase/src/main/java/org/tron/core/db2/checkpoint/common` | 19 / 9 | P0 | payload四类、format三类、materialize三类已迁，继续评估余下运行时与文件IO | 不开放构造器/恢复写权限；协调载荷版本、保留代数、durable publish与rebase顺序保持 |
| DIR-15 | `framework/src/test/java/org/tron/core/db2/archive` | 19 / 9 | P1 | 将根测试按历史执行、checkpoint/recovery、集成与key codec职责评估分组 | HistoricalTestState/HistoricalVm共享夹具私有关系、进程入口和历史VM断言保持 |
| DIR-16 | `chainbase/src/main/java/org/tron/core/db2/archive/history/store` | 18 / 17 | P1 | 按启动准入/截断恢复/正文存储评估，先定义read handles与scan anchor所有权 | BaseManifest、TruncationIntent和Store跨包非public访问；WAL/截断/重开次序必须保留 |
| DIR-17 | `plugins/src/main/java/common/org/tron/plugins` | 15 / 15 | P2 | 评估DB命令与keystore命令分组，优先审查Db/DbBlock/DbBlockExport接入 | picocli命令树、共享工具和发行打包；保持CLI路径和参数兼容 |
| DIR-18 | `chainbase/src/main/java/org/tron/core/db2/archive/history/v5` | 14 / 14 | P1 | 先建模reader generation与FD lease，再评估格式/写入/读取/后缀恢复簇 | 大部分类型包内；pool与committed view互相访问，关闭/重开不能被包迁移拆坏 |
| DIR-19 | `common/src/main/java/org/tron/core/config/args` | 14 / 13 | P1 | 评估配置目录按运行域及archive/native policy分层 | ArchiveRocksDbPolicy、StorageConfig/Storage的解析和配置优先级；existing配置/默认值不变 |
| DIR-20 | `docs/archive` | 14 / 1 | P1 | 按运行/存储/集成与迁移记录评估文档分组，并为JSON映射选择固定位置 | README入口、相对链接、历史源hash与迁移清单保持；源码目录不新建README |
| DIR-21 | `framework/src/test/java/org/tron/core/db2/archive/history/v5` | 14 / 14 | P1 | 跟随V5所有权将format/writer/reader/recovery与集成测试归组 | V5TestSupport与native夹具访问先建模，进程测试入口及故障恢复选例不丢失 |
| DIR-22 | `framework/src/test/java/org/tron/core/db2/archive/serving` | 14 / 12 | P0 | 索引generation测试已随迁，其余按生命周期/存储/性能继续评估 | ServingTestSupport同包权限；large-block专项Gradle选例随路径调整并回验 |
| DIR-23 | `actuator/src/main/java/org/tron/core/vm` | 13 / 13 | P2 | 评估VM按opcode执行、registry及运行上下文分层；先核对3个Archive接入文件 | opcode注册顺序、执行上下文与计费不可为整理目录改写 |
| DIR-24 | `common/src/main/java/org/tron/common/prometheus` | 13 / 10 | P1 | 评估prometheus按数据库/exporter和通用指标组织 | 避免重复注册及静态初始化/标签变化；先核对ChainbaseRocksDbExports、GuavaCacheExports、Metrics |
| DIR-25 | `framework/src/test/java/org/tron/core` | 13 / 13 | P2 | 评估core测试按资源/账户/钱包职责归组，先审查BandwidthProcessorTest | 既有测试宿主包访问和资源计费断言保持；不能只标记单个已改测试便关闭目录项 |
| DIR-26 | `framework/src/test/java/org/tron/core/jsonrpc` | 13 / 13 | P2 | 评估jsonrpc测试按调用、区块日志、过滤器职责分层，先审查JsonrpcServiceTest | 测试夹具、反射和服务装配依赖；结合RPC错误转换目录 |
| DIR-27 | `chainbase/src/main/java/org/tron/core/db2/common` | 12 / 12 | P1 | 评估db2/common的KV引擎适配与通用键/值合同分组 | Flusher、LevelDB/RocksDB批写/关闭、事务快照和native budget边界；避免反向依赖 |
| DIR-28 | `chainbase/src/main/java/org/tron/core/db2/core` | 12 / 9 | P1 | 优先评估P66 lane/配对物化和执行诊断从snapshot核心中分离 | SnapshotManager/Root/Impl事务与锁仍归宿主；checkpoint跨模块合同及块末原子性先建模 |
| DIR-29 | `common/src/test/java/org/tron/core/config/args` | 12 / 12 | P1 | 跟随配置职责评估StorageConfigTest与parity测试归组 | 配置兼容、parity枚举覆盖和Gradle选例保持；默认值/回退断言不能减少 |
| DIR-30 | `framework/src/test/java/org/tron/core/db2/checkpoint/common` | 12 / 9 | P0 | payload/format/materialize三项测试已随迁，继续组织runtime/recovery/kill-process | 同包注入和子进程主类引用、JUnit过滤以及恢复断言保持 |
| DIR-31 | `framework/src/test/java/org/tron/core/services/jsonrpc` | 11 / 11 | P2 | 评估RPC测试按DTO与错误映射分组，先审查HistoricalRpcExceptionTest/JsonRpcErrorResolverTest | 异常到JSON-RPC错误码和HTTP响应一致；不因迁包弱化失败关闭测试 |
| DIR-32 | `plugins/src/test/java/org/tron/plugins` | 11 / 11 | P2 | 随plugins DB/keystore候选组织测试，先审查DbBlockExportTest | CLI子命令和导出数据/异常断言保持，其他既有测试不得漏出构建 |

## 共同checkpoint进度：payload与格式身份已迁移

拆分前19个生产类，局部编译辅助扫描得到22条唯一“访问者/目标类/非public成员”记录。
已按两片迁移，生产/测试总量不减：

1. `common/payload`：CommonCheckpointCapture、CommonCheckpointPayload、CommonCheckpointPayloadCodec、
   CommonCheckpointPayloadFactory，共4类，生产根19→15，已实施。PayloadCodec使用Payload包内restore/coordination，
   Factory使用Capture包内构造器；四类同迁可以保持权限。初步类名引用图中四类不依赖其他common根类。
2. `common/format`：CommonCheckpointBaseline、CommonCheckpointBaselineFile、CommonCheckpointFormat，
   共3类，生产根15→12，已实施。Baseline仍引用根CommonCheckpointTarget，须核查外部调用和包内测试；
   当前记录没有发现它与payload功能子包之间的互相依赖。
3. `common/materialize`：CommonCheckpointMaterializer、CommonCheckpointTarget、CommonCheckpointMaterializedStore，
   3类已迁，根12→9，记录Store故障注入测试随迁。
4. 剩余9类继续评估runtime/redo/rebase与文件IO，不能仅因文件数降到阈值内就关闭DIR-14。

相应测试包含CommonCheckpointPayloadV2Test、CommonCheckpointBaselineFileTest；运行时、redo、
热恢复和kill-process选例需要对包名变更做完整发现检查，再在结构收敛后统一追加聚合测试。

备选：若外部调用或测试暴露未识别的同包依赖，先只实施payload四类，将实际阻碍记录到DIR-14。
不通过扩大构造器/恢复方法可见性绕过阻碍。

## Serving进度：索引合同与内存实现已迁移

已迁入`serving/index`的5类：ServingKeyIndex、PinnedServingKeyIndex、ServingKeyIndexSource、
ServingKeyIndexGeneration、ServingKeyIndexCatalog；22→17。六个迁移类（含测试）类体保持，
20个既有调用方仅改import。实现/lease所有权未改变，实际回归结果由本片记录提供。

后续拆分围绕服务生命周期与持久generation所有权。ServingLease同时调用service/store与
PersistentServingKeyIndexGeneration，ServingView有包内构造且引用Store；
不能把所有public类型按model/api名称移动。lease snapshot先释放再减计数的顺序应保留。
ServingIndexIncrementalPlan、ServingIndexTiming、ServingState/Failure的归属在这条边界稳定后继续评估。

## 其余三个专用生产目录的评估

| 目录 | 局部非public访问记录 | 可读性收益与主要阻碍 | 下一步 |
| --- | ---: | --- | --- |
| stateroot | 342，部分扫描 | 根31仍同时承载trie/snapshot、physical、layer、重建；队列admitSnapshot/publishBase和Store内操作耦合 | 建模重建会话与delta只读合同；再对physical journal与head分组，不再机械搬公开类 |
| history/store | 79 | 启动、WAL准入、截断恢复与正文Store混合；BaseManifest/ScanAnchor/TruncationIntent直接关联内部Store | 先画打开/截断/恢复/关闭责任链，再挑不导出可变Store的边界 |
| history/v5 | 179 | 14类大多包内；reader generation、FD pool、committed view互相协作 | 先建reader generation/FD lease合同，再评估format/write/read/recovery；不能整体public |

扫描限制：扫描范围为各目录生产Java，使用当前已有本地构建classpath与`-proc:none`，
只统计已解析到的类间非public访问；不是完整调用图，也不覆盖测试/反射和全部常量依赖。
serving/common/store/v5分析诊断0错误；stateroot的扫描classpath缺少Lombok及其生成logger，出现12条编译诊断，342项仅是部分线索，
不把扫描进程退出0当编译成功。实施时必须重新编译并核对外部调用/测试。

## 每个目录的完成记录

逐项保留迁移前后数量、职责归属、路径映射、未迁类/保留理由与实际验证结果；
生产与测试/共享夹具一起核查；新版原生组合的已有Snappy失败也要与纯目录回归分开记录。
目录结构收敛后追加完整聚合测试及固定release合并演练，当前局部104项等旧结果不替代后续验收。
原评估切片没有搬迁源码。后续已迁共同checkpoint的7个生产类/2个测试类，
并将绑定计算提取成单方法合同以解除包循环；其余目录仍按本计划推进。

[当前目录盘点](directory-inventory-20261027.md) · [PathStateRoot导航](../architecture/stateroot-layout.md) ·
[路径映射](source-path-moves.json)


共同checkpoint后续实施：payload四类和format三类保持原有包内权限；factory改依赖
`StateArchiveCheckpointBindingPlanner`单方法合同，旧planner继承它并保留prepare/发布职责。
跨三棵功能包图排除根聚合包后无循环，不能据此声称全图无环。最终验证与身份由TASK-039本片结果记录，
原32项全部保留，DIR-14/30仍待后续runtime等责任归位，不能因根测试降到10便自动删除该项。


Serving index后续实施：5个生产类和1个测试类归位，根22→17/测试14→13；没有新增生产类或扩大可见性。
路径映射累计192条，含二次迁移路径，旧目标已接到最终当前位置；DIR-11/22仍保留为未完成项。
下一片继续评估服务/lease与持久generation的所有权，不能按public/model表面属性机械迁移。


后续History来源/物化切片：两个只读来源合同进入serving/source，serving根17→15；
共同checkpoint物化协议、目标和记录Store三类进入materialize，根12→9、测试10→9。
增量计划曾尝试迁入index，但其计时调用与父包形成循环，已保留原位，未更改计时指标。
当前历史路径映射198条；完整32目录待办保持，下降到阈值内的共同checkpoint项也不删除。


## 2026-10-27 本轮提交/部署总进度

全部32个目录已纳入并完成初评，累计6个原目录已有代码/测试迁移；其余26项尚未实施。
共同checkpoint生产与测试两个原目录已降到10以内，但所有32项仍保留最终归位/聚合验收待办。
这不是32项全部完成或release可合入的结论。

相对上次已部署9aa993d00d，本地本轮累计迁移38个生产类、12个测试类，新增3个生产边界/构建入口和1个测试helper。
历史路径记录从148增至198条，重复迁移目标已串接，不把记录数当不同类数。

| 原目录 | 上次部署时 | 当前 | 状态 |
| --- | ---: | ---: | --- |
| stateroot生产根 | 52 | 31 | commitment/runtime/storage/format/trie已分层，私有核心继续拆分 |
| stateroot测试根 | 29 | 21 | 配套8类归位，新增helper保留生产访问权限 |
| common checkpoint生产根 | 19 | 9 | payload/format/materialize已分层，runtime/redo/IO待复核 |
| common checkpoint测试根 | 12 | 9 | 3类随迁，故障钩子仍包内 |
| serving生产根 | 22 | 15 | index/source已分层，服务/lease/generation待继续拆分 |
| serving测试根 | 14 | 13 | generation测试随迁，计划因计时耦合保留 |

本轮交付在聚合测试后提交并推送origin/feature/state_archive，再以准确commit在ARM-001原生构建和验证。
ARM-001原9aa实例已由30GiB容量保护正常停止；独立副本保留原数据/配置/格式，容量不足时停止验证。
具体签名、测试数、JAR和运行窗口以本轮发布结果回执为准。


## 3ff发布后的physical本地首片

用户继续要求拆分stateroot：5生产类迁入physical，根31→26，新增一个只读记录合同，
physical下6文件、stateroot总55。GlobalIntent、ReverseJournal、IngestCheckpoint和OracleWindow实现仍包内，
向调用方提供只读视图；CURRENT常量归格式声明，StoreSet/两个head仍留根包。
69项定向回归通过；迁移前后16组/112份编码逐字节相同，损坏拒绝/复制与包内实现检查通过。
本片为本地候选，3ff仍是上次已发布/ARM验证代码；上面的提交总进度表绑定3ff阶段。
下一片继续处理physical执行与发布所有权，DIR-08/12及全32目录任务保持开放。


physical后续本地片：SnapshotHead归physical，NativeBudget归storage，根26→24，physical8/storage4文件。
head使用受限存储handle，prepared token绑定创建handle，私有适配器拒绝跨owner/伪造token；
StoreSet内部方法与故障钩子不扩权。预算策略共享原表/纯函数，原native参数不变。
87项定向回归通过（含新增token隔离及预算/启动集成）；本片仍本地候选，205条历史路径记录。
PhysicalStoreSet/OverlayHead及其他32目录待办继续开放；上面的3ff发布回执不充当本片ARM验证。

## 本轮剩余有价值拆分收敛与32目录复核

以下是当前本地工作副本的直接文件数。目录阈值用于发现问题，不要求通用宿主或同一私有生命周期簇一律降到10以下。
本片迁移14生产类、15测试类，新增1个观测入口及1个测试访问桥接，另迁14份文档/记录；源码映射累计234条。
STATE保持TASK-045/ITER524，未执行本片提交、推送或节点部署。当前已复核32/32；“保留”是本轮目录整理决策，不代表产品功能验收完成。

| 编号 | 上期扫描 | 当前直接文件 | 实施/保留理由 |
| --- | ---: | ---: | --- |
| DIR-01 | 136 | 136 | 保留HTTP宿主；Archive路由装配已独立，136个既有servlet按公开URL注册，本轮没有独立新增的Archive servlet簇。 |
| DIR-02 | 59 | 59 | 保留数据库集成测试宿主；跟随Revoking/Manager的包内装配与恢复入口，不能独立迁测试并扩大生产权限。 |
| DIR-03 | 42 | 42 | 保留通用actuator注册与协议执行目录；Archive执行已归core/archive/execution，迁42个既有执行器无本轮专属职责收益。 |
| DIR-04 | 41 | 41 | 保留通用异常API；Archive失败分类已在专属包，剩余异常由协议及公共调用方共享。 |
| DIR-05 | 39 | 39 | 保留公共Store注册与Capsule适配；StorageRowKeyCodec仍是VM与Store共用的稳定physical-key入口，不为一个类再建通用codec目录。 |
| DIR-06 | 35 | 35 | 保留通用工具；Archive专属IO已有history/io，BlockFile等还供非Archive路径使用。 |
| DIR-07 | 33 | 33 | 保留Capsule与protobuf领域类型；Archive/root接入字段属于块身份，拆目录不解除宿主依赖。 |
| DIR-08 | 31 | 22 | 已迁physical与metrics；余22类共享root/prepared/delta、原生写集合、CURRENT/layer恢复及重建checkpoint的包内访问，保留所有者簇。 |
| DIR-09 | 24 | 24 | 保留项目根构建、发行、贡献规范与入口；不移动自动发现文件或用目录嵌套隐藏构建复杂度。 |
| DIR-10 | 23 | 23 | 保留utils测试与通用生产工具对应；没有已迁生产簇可供测试随迁。 |
| DIR-11 | 22 | 11 | 已迁index/source/status/telemetry；根11类保留service/lease/native Store/catalog/generation发布所有权，不再开放包内构造与故障钩子。 |
| DIR-12 | 21 | 18 | 已迁观测/format/独立缓存实验；根18类依赖根实现及私有原生/故障夹具，随保留的生产所有权驻留。 |
| DIR-13 | 19 | 19 | 保留TronDatabase/Revoking等公共宿主；Archive具体编排已有专属包，事务及锁继续归宿主。 |
| DIR-14 | 19 | 9 | payload/format/materialize已分组，根9类共同协调发布、redo/rebase及原子IO，保留协调所有者。 |
| DIR-15 | 19 | 9 | 9个历史执行/夹具类归execution，StorageRowKeyCodecTest归Store测试；根9项保留checkpoint、descriptor及Manager集成入口。 |
| DIR-16 | 18 | 17 | Async实现已独立，根17类共享scan anchor、body/index/commit句柄、bootstrap及截断恢复；进一步迁移需新的读写租约合同。 |
| DIR-17 | 15 | 15 | 保留CLI命令树；Archive导出继续使用既有DB命令与参数，不重组无关keystore/DB命令及picocli注册。 |
| DIR-18 | 14 | 14 | 保留V5的14类FD/generation/committed-view/writer/suffix-recovery簇；非public交叉访问覆盖租约释放和恢复写入，拆包需另建生命周期协议。 |
| DIR-19 | 14 | 13 | ArchiveRocksDbPolicy归config/archive；其余13个args文件按通用配置域命名，保留Storage解析/优先级及默认值。 |
| DIR-20 | 14 | 1 | 14个指南/迁移记录按architecture/integration/maintenance分组，根只留README导航；历史来源/hash映射串接到最终目标。 |
| DIR-21 | 14 | 14 | 跟随保留的V5生产所有者；测试共享V5TestSupport、FD及kill-process夹具，不拆断私有恢复断言。 |
| DIR-22 | 14 | 12 | 索引generation与增量plan测试已归index，根12项保留service/lease/store/lifecycle集成；计时计数用测试专用桥接，不开放生产方法。 |
| DIR-23 | 13 | 13 | 保留VM调度宿主；历史执行/Repository已抽出，opcode注册、执行上下文和计费边界仍归VM。 |
| DIR-24 | 13 | 10 | 3个Collector实现归exporters，根13→10；注册时机、指标名/标签和静态epoch不变。 |
| DIR-25 | 13 | 13 | 保留core账户/资源集成测试；对应生产仍在通用宿主，不能只搬Archive触及的一项测试作为目录收口。 |
| DIR-26 | 13 | 13 | 保留jsonrpc契约与装配测试；只在生产形成新领域包时随迁，维持反射/配置/错误转换断言。 |
| DIR-27 | 12 | 12 | 保留12个KV通用合同及引擎适配；Key/Value/DB/Flusher/快照共享同一事务抽象，分出引擎包需额外合同而无本轮独立编排收益。 |
| DIR-28 | 12 | 9 | P66启动/配对物化归p66，执行观测归metrics，根12→9；Snapshot实际读写与checkpoint适配仍归宿主。 |
| DIR-29 | 12 | 12 | 保留StorageConfig/parity与通用args生产配置对应；Archive策略回归在setting/ArchiveAdmissionTest中验证。 |
| DIR-30 | 12 | 9 | 3项payload/format/materialize测试已随生产迁移，根9项保留协调恢复/进程故障场景和包内注入。 |
| DIR-31 | 11 | 11 | 保留RPC DTO/异常映射11项小型契约测试；与既有服务类型对应，未形成独立Archive生命周期簇。 |
| DIR-32 | 11 | 11 | 随保留的CLI生产命令树驻留，维持导出子命令、异常及数据断言。 |

剩余7个专用生产/测试目录仍超过10文件，见[实时盘点](directory-inventory-20261027.md)。
没有新增超过阈值且未纳入的目录。完整产品、长期ARM、release合并维护Gate继续独立记录；本轮按源码目录收益收敛。
