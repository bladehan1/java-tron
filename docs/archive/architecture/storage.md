# Archive storage and Snapshot integration

Archive code stays inside the existing `chainbase` module. The directories below contain the
functional groups migrated so far; Java packages follow the directories.

| Directory | Responsibility | Entry point |
|---|---|---|
| [history/](../../../chainbase/src/main/java/org/tron/core/db2/archive/history) | Validate block identity continuity across state Snapshot layers | `BlockSnapshotContinuity` |
| [history/write/](../../../chainbase/src/main/java/org/tron/core/db2/archive/history/write) | Bounded writer queues, durable-epoch waiting, backpressure and queued-fork removal | `OldValueCollector`, `BlockReverseDiffSink`, `DurableBlockReverseDiffSink`, `BoundedBlockReverseDiffQueue` |
| [history/model/](../../../chainbase/src/main/java/org/tron/core/db2/archive/history/model) | Immutable body/index locations, committed markers, coverage and the read-only committed authority contract | `HistoryLocation`, `HistoryIndexLocation`, `HistoryIndexRecord`, `HistoryCommitMarker`, `HistoryCoverage`, `CommittedHistoryAuthority` |
| [history/codec/](../../../chainbase/src/main/java/org/tron/core/db2/archive/history/codec) | Deterministic historical body and authoritative index encoding, compression, checksum and key-order validation | `BlockHistoryCodec`, `HistoryIndexCodec`, `HistoryCommitMarkerCodec`, `P66AccountAssetCodec` |
| [history/store/](../../../chainbase/src/main/java/org/tron/core/db2/archive/history/store) | Physical Store owners, durable writer, bootstrap and recovery | `ArchiveHistoryWriter`, `ArchiveHistoryAdmission`, `ArchiveHistoryBootstrap` |
| [history/async/](../../../chainbase/src/main/java/org/tron/core/db2/archive/history/async) | Async single-writer ownership, bounded queue and durable waiting | `AsyncArchiveHistorySink` |
| [history/read/](../../../chainbase/src/main/java/org/tron/core/db2/archive/history/read) | Read old values from an immutable committed prefix, checking marker/index/body identity | `CommittedHistoryReader`, `PersistentCommittedHistoryReader` |
| [contract/](../../../chainbase/src/main/java/org/tron/core/db2/archive/contract) | Shared pinned-history and query-limit contracts | `PinnedHistory`, `ArchiveQueryLimitExceededException` |
| [serving/index/](../../../chainbase/src/main/java/org/tron/core/db2/archive/serving/index) | Backend-neutral key-index model, generation publication and read-only acquisition contracts | `ServingKeyIndexGeneration`, `ServingKeyIndexCatalog`, `ServingKeyIndex`, `PinnedServingKeyIndex`, `ServingKeyIndexSource` |
| [recovery/](../../../chainbase/src/main/java/org/tron/core/db2/archive/recovery) | Validate the intent-proven commit marker and durably shrink its commit log before Store recovery | `ArchiveCommitLogRecovery` |
| [checkpoint/](../../../chainbase/src/main/java/org/tron/core/db2/archive/checkpoint) | Submit the non-revertible history range and obtain durable marker evidence before checkpoint publication | `ArchiveHistoryFlush`, `StateArchiveCheckpointPlanner`, `ArchiveCommittedPrefixPublisher` |
| [query/](../../../chainbase/src/main/java/org/tron/core/db2/archive/query) | Runtime quiescence/drain, admission, cancellation/deadline control and failure classification | `ArchiveRuntimeQueryGate`, `HistoricalQueryAdmission`, `HistoricalQueryControl`, `HistoricalFailureCategory`, `HistoricalQueryException`, `HistoricalQueryBudgetException`, `HistoricalQueryCapabilityException` |
| [query/snapshot/](../../../chainbase/src/main/java/org/tron/core/db2/archive/query/snapshot) | Request-owned physical snapshots and bounded one-Store historical range materialization | `ArchivePointSnapshot`, `ArchiveReadSnapshot`, `HistoricalRangeOverlay`, `CheckpointLatestStateFactory` |
| [query/latest/](../../../chainbase/src/main/java/org/tron/core/db2/archive/query/latest) | Stable engine snapshots, barrier-bound generation acquisition and reference-counted publication | `LatestStateGenerationAdapter`, `LatestStateGenerationCoordinator`, `LatestStateGenerationRegistry` |
| [query/account/](../../../chainbase/src/main/java/org/tron/core/db2/archive/query/account) | Decode historical TRX balances from one request-owned account snapshot | `HistoricalAccountBalanceReader` |

`SnapshotManager` retains the canonical monitor, transaction boundaries and publication order.
Its block-final path calls the history validator before attaching artifacts. Its legacy flush path
calls the history durability gate before creating the checkpoint, publishing the committed prefix,
refreshing Chainbase, publishing readable state and releasing the durable sink's bookkeeping.
The pending submission resets after that release succeeds. It also resets on runtime detach or
shutdown; a pending submission prevents `fastPop`.

`ArchiveHistoryFlush` is owned by one `SnapshotManager`. Every call to it runs under that manager's
monitor, including retry, pending-state inspection and reset. It introduces no additional lock.
Common checkpoint continues through its existing separate branch in `SnapshotManager.flush`.

Retained root-package contracts and implementations are mapped in
[ROOT-BOUNDARIES.md](../integration/root-boundaries.md). In particular, package-private types and members stay private to their
current package; moving a file does not justify making them public.

Shared Store identities belong to `ArchiveStoreScope`; the account reader retains its public
constant as an alias. Storage writers depend on that root contract rather than the query package.

`ArchiveHistoryReadHandles` is the Store-package bridge to package-private body placement and scan
anchors. It opens body, index and commit handles and closes them in index/body/commit order.
The persistent reader validates serving/history identity before exposing the handles and retains
its synchronized read/close boundary and close-once flag. Open and validation failures attach each
cleanup IOException directly to the original failure.

`OldValueCollector`, `BlockReverseDiffSink` and `DurableBlockReverseDiffSink` live with the queue
implementations in `history/write/`. `history/store/ArchiveHistoryWriter` is the durable owner; its async implementation lives beside it.
The async writer integration test follows the Store package so fault-injection hooks remain internal. Checkpoint planning and committed-prefix publication
contracts live in `checkpoint/`; their method signatures and the host call order are preserved.

`serving/index/` contains the backend-neutral model and its read contracts. The persistent
index, service, worker and build coordinator remain in the parent `serving/` package as a cohesive
implementation group with package-private dependencies. `ArchiveStoreScope.scopeIdentity` delegates participant
identity to the existing root descriptor without exposing that descriptor. The in-memory model
uses Guava unsigned lexicographic ordering for variable-length keys, preserving range semantics.

Historical query control and failure types live in `query/`. Admission owns capacity and stop
cancellation; request controls enforce the original owner thread, deadline and sticky terminal
failure. `query/session/HistoricalExecutionScope` remains one thread-local guard shared by historical VM frames
and latest-state traps. `query/session` sessions keep owning their pinned views and resource release.
The moved query admission/control tests follow the production package; no visibility is widened.

`history/codec/` owns the existing compressed body-record and authoritative index encodings.
Their format constants and validation rules remain the same. Both codecs use Guava unsigned
lexicographic key ordering, with byte-for-byte comparison against the pre-migration codecs.
`HistoryCommitMarkerCodec` lives in `history/codec`; the commit store reads its prefix width
through the codec's read-only `headerLength()` method; body placement, scan anchors, truncation and durable publication keep their owners.

`ArchiveTruncationRecovery` retains intent loading, package-private Store construction and all
stage notifications. Its commit-log file validation/truncate/force sequence lives in
`recovery/ArchiveCommitLogRecovery`. The helper accepts only the already-validated immutable
intent's offsets and encoded marker; it does not parse or publish recovery authorities. The owner
still shrinks the commit log before publishing the scan anchor, truncating index/body and clearing
the intent. Missing/short/mismatched logs fail before any truncation.

`query/snapshot/ArchivePointSnapshot` is the shared request-owned access contract. Runtime query gates
also live in `query/`, preserving synchronized pin/quiesce/drain and close-once lease accounting.
The pin-source constructor and interface remain package-private; storage-owner tests share a
test-only adapter in the query test package. `contract/ArchiveQueryLimitExceededException` retains its existing type hierarchy and messages.
Serving and snapshot readers share it without importing request admission or control.

`query/latest/` groups the native snapshot adapter and latest-state generation coordinator.
Its registry assembly validates the frozen Store set and snapshot capability before constructing
the coordinator. The root `LatestStateGenerationCoordinatorFactory` retains the private progress
file read and its existing entry points, delegating only the registry assembly. Adapter/coordinator
package-private methods stay in the same functional package with their tests. Participant scope
identity uses the existing `ArchiveStoreScope.scopeIdentity` delegate; acquisition/publication,
CAS, retirement, reference counting and resource cleanup retain their original order.

`history/model/` contains immutable committed-history identities and location records shared by
writers, readers, checkpoint publication and recovery. Their constructors, validation, defensive
copies and record contents remain unchanged. Codecs and physical Store owners retain their
existing formats, private file handles and publication order; only model imports change.

`query/snapshot/` owns request snapshot pinning, identity checks and resource release, together
with the bounded range overlay. Keeping these classes together preserves the package-private
candidate-budget accessor. Snapshot pin/close order and synchronized methods are unchanged.
The overlay uses the existing Guava unsigned lexicographic comparator instead of reaching into
the root package's private byte comparator; prefix bounds and failure budgets stay unchanged.

## Root-package boundary review

The root retains shared contracts and cohesive groups whose package-private members
are kept together for this directory reorganization. The following are retained integration entry points:

| Group | Root entry points and reason |
|---|---|
| Shared block/value contracts | `BlockChangeView`, `BlockReverseDiff`, `OldValue`, `ArchiveStoreScope`: shared by Snapshot, Archive and PathState |
| Host lifecycle | `StateArchiveRuntimeOwner`: owns quiescence and release; `runtime/ArchiveRuntimeAttachment` carries borrowed collaborators |
| Canonical acquisition | `ArchiveStateBarrier`, `StateArchiveCanonicalBlockMetaSource`: bind Store acquisition to canonical state |
| Private progress-file bridge | `LatestStateGenerationCoordinatorFactory`: delegates registry assembly while retaining private progress-file access |
| Shared failure | `ArchivePersistenceException`: used across storage and recovery boundaries |

Physical history, authority/recovery, checkpoint and serving implementations remain cohesive
root-package owners; the [complete retained-boundary map](../integration/root-boundaries.md) lists the remaining 69 root files and the migrated functional groups.
Further splitting of these groups remains part of the directory task; each move must prove its access boundary. No package-private
Store, anchor, recovery stage or native handle is exposed merely to create another directory.

## Read contracts and dependency direction

`contract/PinnedHistory` is shared by committed-history readers and request snapshots. Readers
implement this contract without depending on `query/snapshot`. `ArchivePointSnapshot` lives beside
its snapshot implementation so request controls can depend on snapshots in one direction.

`serving/index/PinnedServingKeyIndex` exposes read metadata and one owned generation lease;
`serving/index/ServingKeyIndexSource` acquires that lease. Snapshot and history readers, and latest-state
coordination, depend on these interfaces rather than persistent generation/catalog classes.
Closing a snapshot releases its history, latest-state and serving leases in that order. The shared
catalog owner retains its lifecycle. `CheckpointLatestStateFactory` shares latest-state pinning and
identity-checked point reads with the checkpoint integration without depending on its root owner.

These boundaries remove the functional subpackage cycles identified in the read path. The root
package still assembles persistent implementations and shared data contracts; the whole Archive
dependency graph is not claimed to be acyclic. Java consumers must update the moved public type
imports and the extracted nested interface names; this is a source package refactor, not a binary
compatibility guarantee or a persistent-format migration.

## Request, capture and WAL groups

`query/session/` owns `HistoricalQuerySession`, `ArchiveReadContext`, `HistoricalForkView` and
`HistoricalExecutionScope`. Keeping the scope beside its typed session avoids a query/session
package cycle.
Their owner-thread, budget, terminal-failure and pin/close behavior stays together. The generic
point-snapshot opener remains package-private, with a test-only adapter for external fixtures.
`query/account/` groups AccountAsset point/prefix resolution with the existing account reader.

`history/write/` owns both physical Snapshot collectors. Shared database names and the P66
physical property key live in `contract/AccountAssetStorage`, so capture does not depend on
query implementations. `history/codec/P66AccountAssetCodec` retains its binary encoding and
validations; unsigned key-order checks use the existing Guava comparator.

`recovery/wal/` owns `ArchiveWalBinding` and its checksummed codec. Their internal constructor
and reserved database constant remain package-private. The startup validator stays with the
root owner until its private manifest/Store dependencies can be separated.
`runtime/ArchiveRuntimeAttachment` carries borrowed collaborators; its installation and release
remain owned by SnapshotManager. `serving/ServingState` groups lifecycle states with serving contracts.

## Physical history, bootstrap and admission

`history/store/` keeps the legacy body, index, commit, scan anchor and truncation owners together
with `ArchiveHistoryWriter`. Their constructors, fault hooks, durable stages and close order stay
internal. `ArchiveHistoryBootstrap` contains the existing complete staging/anchor/reopen/atomic
publication operation; `StateArchiveRuntimeOwner.bootstrapAndRecover` supplies recovery callbacks
and retains concrete runtime ownership. Store bootstrap does not import the runtime owner.
`ArchiveHistoryAdmission` exposes only detached base identity and the existing startup fixed-point
validation. MANIFEST, bootstrap anchor and WAL validators remain package-private.

`history/write/DurableHistoryMarkerRangeEvidence` now accepts a `CommittedHistoryEvidenceSource`
instead of importing a concrete writer. The real writer provides callbacks to its original
synchronized marker-copy and body-read operations. Async acquisition delegates to that same factory.
This separates write contracts from storage implementations without weakening marker validation.
The writer implements that read-only contract through a marker method delegating to its original
synchronized copy operation. Existing `new DurableHistoryMarkerRangeEvidence(writer, limit)` source
usage remains valid after import updates, and the existing writer factory remains available.
Public type imports change as recorded in the path manifest.

`history/io/ArchiveFileDurability` shares directory creation and directory force across history,
checkpoint and serving. Its method bodies are the former helper bodies; Store-local directory
sync delegates to the same leaf operation. No file names, format bytes, force order or defaults
change as part of this package migration.


## Serving index package follow-up

Five public contracts and in-memory index types now live in `serving/index/`:
`ServingKeyIndex`, `PinnedServingKeyIndex`, `ServingKeyIndexSource`,
`ServingKeyIndexGeneration` and `ServingKeyIndexCatalog`.
Their class bodies, generation checks, unsigned ordering and CAS publication semantics are unchanged.
The index generation test follows that package; callers only update imports.

The parent production directory has 17 files, down from 22, and its test directory has 13,
down from 14. Persistent generations, native writers, leases and lifecycle accounting retain
parent-package ownership. The [32-directory backlog](../maintenance/directory-split-plan-20261027.md) still
tracks further serving decomposition. Historical path mappings compose repeated moves so
older source paths resolve to the final current path.


## Serving History source contracts

`serving/source/` owns the two existing read-only History source interfaces:
`ServingHistorySource` and `StateArchiveServingSource`. Their immutable boundaries, byte budgets,
exceptions and method bodies are unchanged. History publishers implement these contracts without
exposing their mutable Store handles. The serving parent directory now has 15 production files.

`ServingIndexIncrementalPlan` remains in the parent package: its DIGEST timing calls depend on
`ServingIndexTiming`, which also observes service state and coordinator progress. Moving only the
plan into `index/` would introduce a package cycle. The timing behavior is retained for a later
cohesive review. The [directory backlog](../maintenance/directory-split-plan-20261027.md) keeps this work open.


## 本轮serving与异步写入归位

serving状态及不可变失败记录归`serving/status`；计时/指标归`serving/telemetry`，增量计划归`serving/index`。
原ServingView与BuildProgress直接实现计时器内的只读观测合同，指标不再引用服务/协调器类型；
不添加观测包装对象，原字段、标签、ready判定、线程局部作用域和计时位置保持。
serving生产根15→11，增量计划测试随迁；计时集成测试仍留根以使用包内generation夹具，
测试专用访问桥接保留计数方法包内权限。

异步写入归`history/async`而非write合同包，避免write→Store→write包循环；Store根18→17。
bootstrap、截断与reader句柄共享包内scan anchor及具体Store，继续保留在Store所有者包。
V5 generation、pool、committed view、writer及suffix recovery共享FD租约与恢复协议，暂不拆成互相依赖的子包。


## P1 持久格式职责

`serving/format`集中key、page、coverage和descriptor纯编解码，generation保留原值类型、Store和发布所有权。
解码通过调用方工厂创建原对象；格式版本及校验保持，17组迁移前黄金字节作为固定测试语料。
见[P1记录](../maintenance/p1-layout-20261027.md)。
