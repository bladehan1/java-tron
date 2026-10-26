# Package ownership and shared root contracts

The directory split leaves six shared root contracts, down from the original 99 root files.
Java packages follow directories and the existing Gradle modules are retained.
The [path manifest](source-path-moves.json) records all production and test relocations.

Serving and runtime assembly are now in their owning packages. Aggregate fixture adaptation and
new runtime publication/recovery tests accompany the split; validation identities and scope are
recorded in TASK-039. The six foundational types below remain together because further subdivision
would add package boundaries without clarifying ownership.

## Functional packages now in place

| Responsibility | Package under `org.tron.core.db2.archive` |
| --- | --- |
| Common authority snapshots and mutation metadata | `checkpoint.authority` |
| Durable progress protocol and read-only loader | `checkpoint.progress` |
| Shared checkpoint redo database and forced target-file protocol | `checkpoint.storage` |
| Checkpoint materializers, exact index and request-owned read adapter | `checkpoint` |
| Native index engine, reader/writer/cursor protocol and engine identity | `storage.index` |
| Legacy history Store, recovery and bootstrap | `history.store` |
| Independent Hot History database and immutable binding | `history.hot` |
| V5 file descriptors and immutable tail protocol | `history.format` |
| V5 physical frames, lane indexes, reader lifetime, tail recovery and participant | `history.v5` |
| Physical file durability | `history.io` |
| Shared point-read and canonical metadata contracts | `contract` |
| Historical session lifetime and latest-state acquisition | `query.session`, `query.latest` |
| Generation capsules and checkpoint read snapshots | `query.snapshot` |
| Serving indexes, plans, leases and lifecycle | `serving` |
| Normal-write attachment, recovery host and publication assembly | `runtime` |

Mutable native backends, Store constructors and fault hooks retain package-local ownership.
Cross-package APIs provide immutable metadata, read contracts, explicit lifecycle entry points,
or the generic native index protocol. They do not expose private business Store implementations.

## Shared scope and immutable block/value contracts (6)

- [ArchiveParticipantDescriptor.java](../../chainbase/src/main/java/org/tron/core/db2/archive/ArchiveParticipantDescriptor.java)
- [ArchivePersistenceException.java](../../chainbase/src/main/java/org/tron/core/db2/archive/ArchivePersistenceException.java)
- [ArchiveStoreScope.java](../../chainbase/src/main/java/org/tron/core/db2/archive/ArchiveStoreScope.java)
- [BlockChangeView.java](../../chainbase/src/main/java/org/tron/core/db2/archive/BlockChangeView.java)
- [BlockReverseDiff.java](../../chainbase/src/main/java/org/tron/core/db2/archive/BlockReverseDiff.java)
- [OldValue.java](../../chainbase/src/main/java/org/tron/core/db2/archive/OldValue.java)

## Runtime assembly boundary

[ServingRuntimeAccess](../../chainbase/src/main/java/org/tron/core/db2/archive/serving/ServingRuntimeAccess.java)
connects the runtime owner to catalog lifecycle operations and an owned logical-plan builder.
The native writer, mutable index implementation, concrete runtime builder and fault hooks remain
package-local. The real builder implements the narrow lifecycle interface directly, without a
per-operation adapter or batch conversion.

`storage.index` owns engine selection, so serving does not depend on checkpoint implementation.
The shared `HistoricalQueryException` lives in `contract`, so serving does not depend on query
sessions or snapshots. Test fixtures use adapters under test sources to reach private implementations.

The path manifest records relocations; new helper contracts are directly discoverable in their
owning packages. Existing Gradle modules and on-disk formats are retained.
