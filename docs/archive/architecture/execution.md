# Archive historical execution in actuator

Archive execution stays in the existing `actuator` Gradle module. These packages group historical
execution logic by responsibility; the node and VM retain their original lifecycle entry points.

| Directory | Responsibility | Entry points |
|---|---|---|
| [execution/](../../../actuator/src/main/java/org/tron/core/archive/execution) | Run a historical constant call, enforce precompile capabilities and propagate request-terminal failures | `HistoricalConstantExecution`, `HistoricalExecutionGuard`, `HistoricalFailures`, `HistoricalCapabilityException` |
| [repository/](../../../actuator/src/main/java/org/tron/core/archive/repository) | Select current/historical roots and adapt request-owned state reads | `HistoricalRepositoryProvider`, `RepositoryStateSources` |

`RuntimeImpl` invokes the historical execution coordinator for its existing execution mode. The
coordinator binds the session once, enters `HistoricalExecutionScope`, validates and executes,
checks the terminal failure and clears request VM configuration/operation tables in the original
finally block. VM dispatch and child-frame code call the same capability and failure helpers.
Latest execution continues through its existing path.

`RepositoryImpl.createHistoricalRoot` remains the public construction boundary. The package-private
state-source contract, current/historical adapters and property logging now live in
[repository/](repository.md). `RepositoryStateSources` provides the host read facade while
preserving request-local caches, child-frame sharing and pinned-session ownership.

Historical VM integration tests and both package-private fixtures now live together in
`framework/src/test/java/org/tron/core/db2/archive/execution`; fixture visibility stays unchanged. Java package names of the migrated public helpers change;
repository callers have been updated together.
