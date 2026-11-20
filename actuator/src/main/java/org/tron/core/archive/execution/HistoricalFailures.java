package org.tron.core.archive.execution;

import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.query.HistoricalQueryCapabilityException;
import org.tron.core.vm.VM;
import org.tron.core.vm.repository.Repository;

/**
 * The single mark-then-throw helper for historical-class failures raised inside the VM (chapter 05
 * section 3.7 P1/P3). Marking goes Repository -> RepositoryStateSource -> session, so the failure
 * becomes the request's sticky TERMINAL cause before any frame can swallow it.
 */
public final class HistoricalFailures {

  private HistoricalFailures() {
  }

  /** Creates and marks a capability failure; callers write {@code throw capability(..)}. */
  public static RuntimeException capability(Repository repository, String message) {
    return repository.markHistoricalFailure(new HistoricalCapabilityException(message));
  }

  public static RuntimeException capability(Repository repository, String detail,
      String message) {
    return repository.markHistoricalFailure(new HistoricalCapabilityException(detail, message));
  }

  /**
   * Historical-class failures: capability, budget, corrupt, data-access, unavailable, deadline and
   * cancellation. Everything an archive read can throw is an {@link ArchivePersistenceException}.
   */
  public static boolean isHistoricalClass(Throwable failure) {
    return failure instanceof ArchivePersistenceException
        || failure instanceof HistoricalQueryCapabilityException;
  }

  /**
   * VM outer-catch decision for historical execution (P3). Returns the failure that must propagate
   * out of this frame, already marked TERMINAL, or {@code null} when {@code failure} is an
   * ordinary TVM result that keeps upstream child-frame semantics.
   */
  public static RuntimeException escalate(Repository repository, RuntimeException failure) {
    if (isHistoricalClass(failure)) {
      return repository.markHistoricalFailure(failure);
    }
    if (failure instanceof NullPointerException) {
      return repository.markHistoricalFailure(new HistoricalCapabilityException(
          HistoricalQueryCapabilityException.UNEXPECTED_STATE,
          "Unexpected state in historical execution", failure));
    }
    // A failure recorded earlier but swallowed by some frame wins over this TVM result.
    try {
      repository.checkHistoricalQueryActive();
    } catch (RuntimeException terminal) {
      return terminal;
    }
    return null;
  }
}
