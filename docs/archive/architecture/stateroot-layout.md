# PathStateRoot源码导航

实际源码为 `chainbase/src/main/java/org/tron/core/db2/stateroot`，与 `db2/archive` 同级。
保留原Gradle模块和宿主生命周期。首批迁移17个生产类、6个测试类，后续再迁4个生产类、2个测试类；
根生产目录52→35→31→26→24→22，总生产Java52→58，新增文件为原生存储协议、流式构建入口、physical只读记录合同、head存储合同和观测入口。

| 目录 | 文件数 | 职责 |
| --- | ---: | --- |
| `commitment` | 8 | participant、规范化、commitment编码、block transition与collector |
| `runtime` | 4 | head合同、运行准入、attachment与async prepare入口 |
| `storage` | 4 | 原生引擎协议、包内driver、perf采样与缓存/预算策略 |
| `format` | 3 | manifest、根元数据、engine和持久化目录名声明 |
| `trie` | 4 | 节点合同、流式构建入口、包内StackTrie与磁盘排序构建器 |
| `rebuild` | 1 | 原生快照来源与分页 |
| `physical` | 9 | 包内记录codec、只读合同、准入、snapshot head及受限存储合同 |
| `metrics` | 3 | 包内采样/指标实现及观测入口，不持有Store或发布权限 |
| 根包 | 22 | trie、当前/物理head、layer发布退休、重建编排等待继续拆分 |

`PathStateNativeAccess`承接原有19项实例操作和5个open工厂。具体driver与perf实现保持包内，
原生句柄仍由实现私有持有，Store的原生协议字段也保持私有；没有新增包装实例或批次转换。
批次mutation构造保持复制/校验行为，扫描结果保留防御复制getter。

两个manifest迁移保持原格式与目录协议。`base.next`和`base.previous`常量移入manifest，
原compaction常量保留别名；没有改动打开/关闭顺序、同步语义、恢复流程或默认开关。
Java全限定类名随目录更新，仓内调用及测试已同步；这不承诺仓外Java调用兼容。

根包仍有包内协作。下一批先梳理trie/node与snapshot ownership、physical head/store、
layer publication/retirement三簇，按所有权拆分，避免整体开放可变Store内部权限。
根聚合包仍与子包互相引用，当前只核查功能子包之间无循环，不能声称全图无环。

超过10个直接文件的完整清单见[目录盘点](../maintenance/directory-inventory-20261027.md)，
生产/测试路径变化见[迁移清单](../maintenance/source-path-moves.json)。


流式构建入口`PathStateTrieBuild`直接创建原来的两个包内实现：`ordered`接收严格升序键，
`spooled`在磁盘排序后构建。两个返回合同只暴露消费/完成操作；调用方仍持有node/leaf writer，
没有导出具体Store、原生句柄或内部计数器。构造、close时机、排序和失败清理保持原序。
`PathNodeStore`归入trie，`PathStateRootMetadata`归入format，两者原有公开方法不扩权。
`PathStateCatchUpQueue`因与coordinator的包内admit/publish操作耦合暂留根包，下一批继续梳理。


## Physical记录与准入首片

`physical/PathStatePhysicalRecords`提供4类只读记录视图、校验工厂及3个嵌套不可变值类型；
4个具体编解码实现仍为包内类，构造和解码仍在该包。字段不向调用方公开数组，getter继续防御复制，
participant/journal列表保持原来的不可变性及复制方式。记录对象直接实现视图，没有新增wrapper或逐条转换。
`OracleWindow`仅验证调用方提供的离线一致窗口，不提供历史root查找或数据库写接口。

`PathStatePhysicalRuntimeAdmission`归入physical。`CURRENT`文件名声明归已有format manifest，
StoreSet保留包内别名，字面值、禁用/非创建式检查和打开次序不变。

在记录拆分片结束时，StoreSet、SnapshotHead和OverlayHead仍访问root的执行、节点叠层和delta内部操作，暂留根包。
下一片先明确这些所有权/发布回调，再迁移，具体Store、原生句柄和故障钩子不扩大可见性。
根包的native/准入集成测试继续保留原包，以维持其私有故障访问；测试仅适配新的只读视图及工厂。


## Physical snapshot head与原生预算后续片

`PathStatePhysicalSnapshotHead`归入physical，通过`PathStatePhysicalHeadStorage`使用恢复、校验、
prepare/publish、rewind和close能力。StoreSet中的私有适配器转发原有方法，内部发布方法和故障钩子保持原权限。
head的锁、CURRENT二次核对、异常失败关闭及close清理次序保持。

准备结果是绑定创建handle的opaque token：其他handle及外部实现不能交给该handle发布。
适配器为每次prepare创建一个常量大小的私有owned token，复用原plan和delta，不逐条复制mutation或导出Store。
原PreparedPhysicalTransition仍包内，原matches采用transition对象身份的语义保留。

`PathStateNativeBudget`归入storage，接收原有giant/large映射、cache shard和benchmark选择纯函数。
预算计算与Store打开共享这份策略；StoreSet原包内方法保留转发别名。profile/预算/分片数/开关保持原值。
剩余PhysicalStoreSet与OverlayHead继续在root内，下一片仍需梳理root执行和delta所有权。


## 本轮剩余候选收敛

OperationTimer和AttributionMetrics归`metrics`，具体实现保持包内；`PathStateObservations`只公开观测能力。
Timer由原对象直接实现，不增加每次采样的包装、批次转换或指标注册实例；采样步长、开关、标签及单位保持。
观测测试归metrics，持久格式测试归format，独立SST缓存分片实验归storage；根测试21→18。

根22个类继续保留同包协作：trie/root/prepared/delta维护实际节点及叶子写集合；
base/current/layer负责CURRENT、恢复和退休；rebuild checkpoint与NodeStoreSet共享恢复进度；
physical StoreSet、OverlayHead和checkpoint materializer共享原生数据与块内发布边界。
这些协作存在包内写入、构造及故障访问，进一步拆包需要改变所有权协议，单纯目录搬迁收益已不足以抵偿该改动。
这里保留明确职责与调用导航，不统一公开内部写入或故障钩子。


## P1 journal 管理

`PathStatePhysicalJournals`归physical（现9文件），StoreSet仍私有持有并在原同步入口调用；
索引、祖先链和保留策略由该对象管理，MetadataFile耐久读/删仍经原所有者适配。
根22文件保持，StoreSet从2851降到2672行；无持久格式、写入顺序或故障钩子权限变更。
见[P1记录](../maintenance/p1-layout-20261027.md)。
