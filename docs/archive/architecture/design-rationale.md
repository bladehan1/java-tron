# Design rationale: why capture-based V5

This document distills the selection rationale for the State Archive design so
future maintainers can audit why this path was chosen. It summarizes the
prior-art survey and the long-term questions; the authoritative design
contracts remain the core-design documents and the decision register.

## Problem

A java-tron node keeps only "latest state + a short revoking window". Old values
are lost once a block is applied, there is no common durable boundary across the
state databases, and no historical point query or historical call is available.

## Prior art (Ethereum state-history problem domain)

| Source | Approach | Relation to this design |
| --- | --- | --- |
| Geth (PBSS archive) | History kept inside the state trie itself; freezer for immutable segments | No independent old-value log; trie-based |
| Besu (Bonsai + trie log) | Per-block old-value journal (trie log), rollback/roll-forward | Closest precedent for our `BlockReverseDiff` capture |
| Erigon | Flat state + append-only history files with index | Precedent for independent history store + side index |
| Reth | Static files: append-only ~2 GiB segments with per-segment index | 2 GiB segment + side index matches V5 layout |
| Nethermind | Archive = full trie retention | No independent diff storage |
| TRON (develop) | SnapshotManager revoking layer only; no cross-DB checkpoint | This branch is the first TRON implementation |

Convergent direction across ETH clients: history lives outside the online state,
beside an index, in append-only segments.

## Positioning of this design (adapt vs originate)

- **Adapted**: block-final old-value capture (≈ Besu trie log); five-lane V5
  append-only segmented storage (≈ Erigon history files + Reth static files);
  derived serving key index (≈ Erigon/Reth side index); restricted historical
  `eth_call` follows Ethereum read-only call semantics. Lane split
  (lane 0/4/5/13/22 for the five TRON state DBs) is a TRON-specific adaptation.
- **Original (3-C)**: the common-checkpoint protocol (multi-participant durable
  boundary, retained redo generations, durableBase) has no direct ETH equivalent
  (their clients use single embedded DBs); it borrows from classic WAL/redo
  theory. The "catch up in background → freeze exact common boundary → build the
  new history synchronously with callers waiting" handoff orchestration is also
  original.

## Alternatives considered and rejected

- **Replay recompute** (re-execute blocks to answer history): simplest, zero
  storage growth, but seconds-to-minutes per query — cannot serve the online API
  requirement. Adopted only as the offline `BlockReplay` / archive-vs-trace
  validation tool.
- **Versioned state DB / trie overlay** (Geth archive / Besu Bonsai style): keeps
  full history inside the online state storage — largest long-term storage and
  compaction cost, deepest coupling with RocksDB tuning; conflicts with the TRON
  multi-DB structure and performance baseline. Rejected.

## Long-term questions (answered at selection time)

- **Maintenance cost**: V5 carries version/descriptor/digest mount points for
  evolution; lane additions require the LANE_IDS whitelist. Format stability is
  governed by [format-stability](format-stability.md).
- **Evolvability**: laneIndex / segment / tail are separate layers; a V3-compat
  coexistence path was kept for derived-index migration.
- **Ecosystem independence**: the format is TRON-owned (store-mapping domain,
  five-lane mapping digests); no ETH tooling dependency, and no external
  validator either — compatibility risk is owned here.
- **Reversibility**: every subsystem defaults to off; disabling returns to
  develop behavior. After enablement, rollback = discard the archive directory
  (no chain-data impact, non-consensus).

## Key invariants the design relies on

- Capture completeness: every state mutation must flow through `BlockChangeView`
  so the reverse diff covers all changes. Registering new stores is a discipline
  requirement (`ArchiveStoreScope.validate(dbs)` checks the registered set).
- Read consistency: historical point queries use the lock-free `T + B(key)`
  model (request fixes target height T; each key resolves at access time);
  fail-closed on coverage/canonical/corruption/capability checks. This is an
  eventually-consistent prototype contract, not a strong snapshot guarantee.
