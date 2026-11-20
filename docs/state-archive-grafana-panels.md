# State Archive Grafana Panels

Operational guide for the **State Archive** row and the **Archive DB Size** panel in the
`java tron server with state_archive` dashboard, plus the metric prerequisites that are not obvious from the
queries.

- Dashboard: `java tron server with state_archive` (uid `NFF4S5d18L`) for the State Archive row
  and Archive DB Size panel; the base `java tron server` (uid `WAY66gQ07X`) no longer carries
  state-archive panels since the 2026-11-10 split
- Datasource: Prometheus variable `${datasource}`; panels filter by `{group=$group, instance=$instance}`
- Metrics are emitted by the fork's `feature/state_archive` line; stock java-tron does not have them

## Panel map

| Panel | Query (core metric) | Shows | Prerequisite |
|---|---|---|---|
| Archive Serving State | `tron_archive_serving_state{state=...}` | Lifecycle state over time (1 = active): `LIVE`, `BULK_CATCH_UP`, `REBUILDING`, `DEGRADED`, `CLOSED` | state archive enabled |
| Last Prepared Height | `tron_pathstate_last_prepared_height` | Highest height prepared by the path-state engine; may rewind (not canonical) | JVM flag, see below |
| Checkpoint Store Rate | `rate(tron_chainbase_checkpoint_store_total{kind=...})` | Checkpoint flush activity: `entries/s`, `calls/s` | JVM flag, see below |
| PathState Resident Cache | `tron_pathstate_resident_cache_*` | Resident node cache footprint | path-state enabled |
| Serving Index Progress | `tron_archive_serving_progress{kind=...}` | `indexed_from` / `indexed_through` / `committed_through` (left axis, heights) and `backlog_blocks` (right axis) | state archive enabled |
| Archive DB Size (DB row) | `tron:db_size_bytes{db=~"path-state-root\|serving-index"}` | On-disk size of the two archive databases | build with `ArchiveDbSizeMetrics`, see below |

> Note: this panel was temporarily lost during the manual dashboard restructure of
> 2026-11-10 (v57/v58) and restored in v60 with id 147. If it disappears again after
> future UI edits, re-create it from the query above.

## Archive DB Size: what the `db` label means

The state archive adds two databases on disk, and the panel distinguishes them by name:

| `db` label | Kind | Disk location | Opened by |
|---|---|---|---|
| `path-state-root` | pathTreeNode archive store | `<output>/path-state-root` (`storage.pathStateRoot.directory`) | `PathStateNodeStoreSet` (native RocksDB) |
| `serving-index` | serving index | `<archive>/serving-index` (contains `generations/`) | `PersistentServingKeyIndexCatalog` |

Chainbase stores also emit `tron:db_size_bytes`, but they are intentionally excluded from this
panel (they dominate the scale and are observable elsewhere).

### How the size is reported

Unlike chainbase stores — whose sizes come from RocksDB `levelstats` per SST level via
`DbStatService` — the archive databases open their storage directly and bypass that machinery.
`ArchiveDbSizeMetrics` (`chainbase/src/main/java/org/tron/common/storage/metric/`) closes the gap:

- a daemon thread walks the database directory and sums file sizes;
- it reports hourly as `tron:db_size_bytes{db=<name>, level="all", type=<engine>}`;
- because it is a raw file-tree walk, the value includes files RocksDB has not yet garbage
  collected. Treat it as on-disk footprint, not live logical size.

Registration happens at the three startup paths that can open these stores:
`Manager.initStateArchive` (serving-index), `Manager.initCommonCheckpoint` and the standalone
path-state startup (path-state-root). **Panels stay empty until a build containing this class
is deployed**; the first sample lands ~1 minute after startup.

## Gated metrics: two panels need JVM flags

`Last Prepared Height` and `Checkpoint Store Rate` read metrics that are only emitted when
attribution is enabled at the JVM level:

```bash
java -Dtron.pathstate.attribution=true \
     -Dtron.chainbase.executionAttribution=true \
     ... FullNode.jar
```

| Flag | Unlocks | Guard in code |
|---|---|---|
| `-Dtron.pathstate.attribution=true` | `tron_pathstate_last_prepared_height`, all `tron_pathstate_prepared_*` | `PathStateOperationTimer.enabled()` |
| `-Dtron.chainbase.executionAttribution=true` | `tron_chainbase_checkpoint_store_total` | `ExecutionAttribution` |

Both flags are sampling/attribution instrumentation: they add per-operation accounting overhead
on the prepare and checkpoint paths. Enable them on validation nodes; think twice before turning
them on in latency-critical production.

## Reading the PathState Resident Cache panel

The panel mixes two quantities on purpose, and they can look deceptively similar:

- **cache bytes** (left axis) is capped by `storage.pathStateRoot.nodeCacheBytes`
  (default 1 GiB). A flat line at ~1024 MiB means the cache is full — it is a size bound, not a
  leak or a plateau in usage.
- **entries (count)** and **evictions (cum)** live on the right axis. When the cache is full,
  entry count stabilizes too; seeing `1024` entries next to `1024 MiB` is a coincidence of
  numbers, not a unit bug. The tooltip prints bytes with binary units (`MiB`/`GiB`) and counts
  as plain numbers.

A rising eviction counter with flat bytes means the cache is churning — hot working set exceeds
`nodeCacheBytes`.

## Troubleshooting: a panel is empty

1. Check the metric exists at all: `tron_archive_serving_state` should always be present when
   the archive runs. If every State Archive panel is empty, the node is likely stock java-tron.
2. Flag-gated panels (`Last Prepared Height`, `Checkpoint Store Rate`): verify both `-D` flags
   are on the running process (`jps -v` or `ps -ef | grep FullNode`), then remember the metrics
   only appear after the first relevant event post-restart.
3. `Archive DB Size`: requires the `ArchiveDbSizeMetrics` build. Verify with
   `curl -s localhost:9527/metrics | grep 'db_size_bytes{db="path-state-root"'` on the node
   (adjust the metrics port).
4. `Serving Index Progress` reports `-1` for unknown values (e.g. `backlog_bytes` when the
   source commits are not yet sized); that is "no data", not zero.
