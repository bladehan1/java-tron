package org.tron.core.db;

import lombok.Data;
import org.tron.common.runtime.ProgramResult;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.db2.archive.HistoricalQuerySession;
import org.tron.core.store.StoreFactory;

@Data
public class TransactionContext {

  public enum ExecutionMode {
    CURRENT_CONSENSUS,
    CURRENT_CONSTANT,
    HISTORICAL_CONSTANT
  }

  private BlockCapsule blockCap;
  private TransactionCapsule trxCap;
  private StoreFactory storeFactory;
  private ProgramResult programResult = new ProgramResult();
  private boolean isStatic;
  private boolean eventPluginLoaded;
  private final ExecutionMode executionMode;
  private final HistoricalQuerySession historicalQuerySession;

  public TransactionContext(BlockCapsule blockCap, TransactionCapsule trxCap,
      StoreFactory storeFactory,
      boolean isStatic,
      boolean eventPluginLoaded) {
    this(blockCap, trxCap, storeFactory, isStatic, eventPluginLoaded,
        isStatic ? ExecutionMode.CURRENT_CONSTANT : ExecutionMode.CURRENT_CONSENSUS, null);
  }

  public TransactionContext(BlockCapsule blockCap, TransactionCapsule trxCap,
      StoreFactory storeFactory, boolean isStatic, boolean eventPluginLoaded,
      ExecutionMode executionMode, HistoricalQuerySession historicalQuerySession) {
    this.blockCap = blockCap;
    this.trxCap = trxCap;
    this.storeFactory = storeFactory;
    this.isStatic = isStatic;
    this.eventPluginLoaded = eventPluginLoaded;
    this.executionMode = java.util.Objects.requireNonNull(executionMode, "executionMode");
    this.historicalQuerySession = historicalQuerySession;
    if (executionMode == ExecutionMode.HISTORICAL_CONSTANT) {
      if (!isStatic || historicalQuerySession == null) {
        throw new IllegalArgumentException(
            "HISTORICAL_CONSTANT requires static execution and a historical query session");
      }
    } else if (historicalQuerySession != null) {
      throw new IllegalArgumentException(
          "A historical query session is only valid for HISTORICAL_CONSTANT execution");
    }
  }
}
