# FullNode Archive integration

This directory groups Archive integration by function inside the existing `framework` module.
Java packages follow the directory tree.

| Directory | Responsibility | Entry point |
|---|---|---|
| [startup/](startup/) | Base-format admission, runtime recovery, writer attachment and failure cleanup | `StateArchiveStartup` |
| [startup/diagnostic/](startup/diagnostic/) | Optional startup checks against current Store samples and P66 balances | `ArchiveStateDiagnostic` |
| [checkpoint/](checkpoint/) | Archive collector and append-file materializer setup for Common checkpoint | `StateArchiveCheckpointStartup` |
| [query/http/](query/http/) | Experimental Archive balance HTTP request/response adapter | `GetAccountBalanceFromArchiveServlet` |
| [query/](query/) | Canonical query-session identity, admission ownership and failure cleanup | `StateArchiveQuerySessions` |

`Manager` owns FullNode startup order, runtime fields and shutdown. Its public query methods remain
the service entry points. `org.tron.program.ArchiveStateDiagnostic` preserves the program entry point
and system property while delegating to the diagnostic implementation here.

Storage engines, persistent formats and shared checkpoint coordination belong to `chainbase`.
Implementations move here only when they orchestrate the FullNode's Archive integration.

The Archive balance servlet keeps its simple class name, Spring component name and rate-limiter
key. `FullNodeHttpApiService` keeps the same bean field and `/wallet/getaccountbalancefromarchive`
registration; only the servlet import changes. HTTP parsing, Wallet delegation and error encoding
remain in the moved adapter with unchanged method bodies.
