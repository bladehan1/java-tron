# Serving Index batched build (compatible, no format change)

Status: **V1 of the Geth-aligned build pipeline** (as of 2026-11-24). This change only
affects how the exact key index is *built*. It does not change any persistent encoding, so an
index written by PR #55 opens and keeps appending from its durable progress `I`.

## What stays byte-identical

| Record | Key prefix | Encoding |
|---|---|---|
| KeyMeta | `0x03` | INLINE (≤ 4 epochs) or PAGED |
| KeyEpochPage | `0x04` | fixed 512 epochs per page, 8-byte big-endian each |
| StoreCoverage | `0x05` | one row per participating store |
| Progress descriptor / state record | `7f 'S' 'I' 01` / `7f 'S' 'S' 01` | unchanged |

`ServingIndexRecordCodec`, `ServingIndexKeyCodec` and the append path
(`prepareExactChanges` / `PreparedExactChanges.appendTo`) are untouched. R-IDEM for legacy
solo writes keeps its existing rule: with durable progress `I`, a KeyMeta whose last epoch is
`I + 1` is residue of an interrupted solo batch and is dropped from the replay.

The only new key is the transient **flush intent** `7f 'S' 'F' 01`. It exists only while a
multi-write flush is in progress and is deleted by the write that publishes progress.

## Build pipeline

One build round reads bounded History windows (existing `maxBlocks` / `maxSourceBytes`
budgets) into one in-memory change table, `ServingChangeAccumulator`, which merges
`(store, rawKey) → ordered native epochs` across windows. A round flushes when the real
change count reaches `flushChanges` (default 8 × 1024 × 1024, Geth's `historyIndexBatch`) or
when the round target `W` is consumed; a window boundary alone no longer publishes progress.
`seal()` turns the table into a `GroupedAppend` and the single commit path takes over:

```
accumulate windows ──► seal() ──► GroupedAppend ──► commit(append, clearRebuild, dataBatchBytes)
                                                             │
                                   workers: contiguous key chunks, read + encode only
                                                             │
                                   build thread: FlushFlow batches mutations by bytes
```

Workers are opt-in through `-Dtron.archive.serving.buildThreads=N` (default 1 = the serial
path, capped at available processors). Each worker claims the next chunk of contiguous input
keys (`max(1, min(keys / (N × 8), 1024))`), reads KeyMeta and the tail page, and returns the
key's complete mutation group; a key's epochs are always appended by one worker in order.
Finished chunks reach the build thread in completion order, so a slow chunk never delays
already-finished chunks and no re-sequencing buffer exists. Only the build thread writes.
The first failure stops dispatching, in-flight workers finish their current key, and the
failure propagates after they exit.

## Flush protocol

- **Flush fits `dataBatchBytes`** (default 32 MiB of encoded mutations): index data, the 27
  coverage rows and the descriptor stay in one `sync` batch, exactly like a plan-sized commit.
- **Larger flush**: write the flush intent (`base I`, `hash(I)`, target, `hash(target)`, rolled
  source digest, change count), then key-atomic data batches, then one final write that
  publishes coverage and progress and deletes the intent. A key's mutations are never split
  across batches. Queries keep using the old descriptor until the final write, so data above
  `I` is invisible while batches are already durable.

## Recovery

`ServingIndexStore.open` validates a leftover intent against the descriptor and keeps it as
`pendingFlushIntent()`. The service then replays the same History range into a fresh
accumulator, checks the replay against the intent (heights, hashes, digest, change count) and
calls `recoverFlush`. Per key:

- durable last epoch `≤ I`, or exactly `I + 1` (this flush's single-epoch group / legacy solo
  residue): append through the normal R-IDEM path;
- last epoch in `(I, target]`: the last `expected.length` durable epochs must equal the replay
  and the entry before them must not exceed `I`; the key is then skipped;
- anything ahead of `target`, or any differing suffix: `INDEX_CORRUPT` / residue error.

For paged keys the suffix is read from the fixed-size pages it touches; when it starts at a
page boundary the preceding page is read too so the "nothing else above `I`" check has an
entry to compare. Recovery is idempotent and may itself be interrupted.

**Downgrade note:** an intent is invisible to the PR #55 build. If an older build is started
while an intent exists, keys ahead of `I` fail the old R-IDEM rule with `INDEX_CORRUPT`
(fail-closed, no silent corruption). Finish the recovery with this build before downgrading.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `maxBlocks` / `maxSourceBytes` | 1000 / 32 MiB | History read window (unchanged) |
| `flushChanges` | 8 × 1024 × 1024 | real changes accumulated before one flush |
| `dataBatchBytes` | 32 MiB | encoded mutation bytes above which a flush streams |
| `tron.archive.serving.buildThreads` | 1 | parallel preparation workers |

`flushChanges` and `dataBatchBytes` are `ServingIndexService.Config` setters; the removed
`maxWriteMutations` / `maxWriteBytes` soft triggers and the plan's byte/mutation estimates no
longer exist.

## Tests

`ServingIndexAccumulationTest` covers accumulation, the flush protocol, recovery at every
injection point, old-`I` query bounds, legacy solo residue and a PR #55-shaped index that
reopens untouched and continues through a streamed parallel flush.
`ServingIndexParallelBuildTest` covers serial/parallel row equality, GET-count equality,
completion-order delivery and worker failure; `ServingIndexWindowCommitTest` replaces the
former soft-batch tests.
