# PathStateRoot源码导航

实际源码为 `chainbase/src/main/java/org/tron/core/db2/stateroot`，与 `db2/archive` 同级。
保留原Gradle模块和宿主生命周期。首批迁移17个生产类、6个测试类，后续再迁4个生产类、2个测试类；
根生产目录52→35→31，总生产Java52→54，新增文件为原生存储协议和流式构建入口。

| 目录 | 文件数 | 职责 |
| --- | ---: | --- |
| `commitment` | 8 | participant、规范化、commitment编码、block transition与collector |
| `runtime` | 4 | head合同、运行准入、attachment与async prepare入口 |
| `storage` | 3 | 原生引擎协议、包内driver和perf采样 |
| `format` | 3 | manifest、根元数据、engine和持久化目录名声明 |
| `trie` | 4 | 节点合同、流式构建入口、包内StackTrie与磁盘排序构建器 |
| `rebuild` | 1 | 原生快照来源与分页 |
| 根包 | 31 | trie、当前/物理head、layer发布退休、重建编排等待继续拆分 |

`PathStateNativeAccess`承接原有19项实例操作和5个open工厂。具体driver与perf实现保持包内，
原生句柄仍由实现私有持有，Store的原生协议字段也保持私有；没有新增包装实例或批次转换。
批次mutation构造保持复制/校验行为，扫描结果保留防御复制getter。

两个manifest迁移保持原格式与目录协议。`base.next`和`base.previous`常量移入manifest，
原compaction常量保留别名；没有改动打开/关闭顺序、同步语义、恢复流程或默认开关。
Java全限定类名随目录更新，仓内调用及测试已同步；这不承诺仓外Java调用兼容。

根包仍有包内协作。下一批先梳理trie/node与snapshot ownership、physical head/store、
layer publication/retirement三簇，按所有权拆分，避免整体开放可变Store内部权限。
根聚合包仍与子包互相引用，当前只核查功能子包之间无循环，不能声称全图无环。

超过10个直接文件的完整清单见[目录盘点](directory-inventory-20261027.md)，
生产/测试路径变化见[迁移清单](source-path-moves.json)。


流式构建入口`PathStateTrieBuild`直接创建原来的两个包内实现：`ordered`接收严格升序键，
`spooled`在磁盘排序后构建。两个返回合同只暴露消费/完成操作；调用方仍持有node/leaf writer，
没有导出具体Store、原生句柄或内部计数器。构造、close时机、排序和失败清理保持原序。
`PathNodeStore`归入trie，`PathStateRootMetadata`归入format，两者原有公开方法不扩权。
`PathStateCatchUpQueue`因与coordinator的包内admit/publish操作耦合暂留根包，下一批继续梳理。
