# State Archive format stability statement

Status: **experimental / prototype** (as of 2026-11-19). This document is the
weak-stability commitment for the State Archive persistent formats; it is updated
only by explicit human decision.

## What "experimental" means

- The V5 append-file history format self-identifies as a prototype
  (`FORMAT_ID = "archive-state/geth-lane-index/v5"`). It carries version,
  descriptor, and digest mount points so a future revision *can* be introduced
  without ambiguity — but no stability window is promised yet.
- Enabling `stateArchive.appendFile` writes history data in this format. If the
  format is later revised (bug fix or evolution), existing on-disk history may
  become unreadable by newer code and would need either a migration tool or a
  full re-scan from genesis (re-capture requires replaying the chain).
- There is a single implementation and no external validator; compatibility risk
  is owned entirely by this repo.

## Production enablement precondition

Format GA is a **precondition Gate** for any production enablement: production
use of state archive requires (1) this document flipped to GA with an explicit
freeze window, (2) a written v6+ migration or re-scan plan, and (3) the
production-operations gates in the decision register (O-05/O-06). Internal
validation nodes may run the experimental format after accepting the re-scan
risk above.

## Version mechanics today

- V3-era directories are retained only for derived-index compatibility and
  internal names; fresh data is written as V5.
- The serving index and hot store are independent auxiliary databases; changing
  their engine or layout requires rebuilding/resyncing that DB, not the history.
- `pathStateRoot.formatVersion` is a reserved latch validated to `1`; it exists
  only as a future format gate and does not affect runtime behavior.

## Revision history

- 2026-11-19: statement created (weak form) as review finding
  `state_archive`/MUST; format GA recorded as a production-enablement
  precondition.
