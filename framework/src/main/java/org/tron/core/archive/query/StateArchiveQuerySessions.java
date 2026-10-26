package org.tron.core.archive.query;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;
import org.tron.common.parameter.CommonParameter;
import org.tron.core.ChainBaseManager;
import org.tron.core.db2.archive.contract.HistoricalQueryException;
import org.tron.core.db2.archive.contract.HistoricalQueryException.Reason;
import org.tron.core.db2.archive.query.HistoricalQueryAdmission;
import org.tron.core.db2.archive.query.session.HistoricalQuerySession;
import org.tron.core.db2.archive.runtime.StateArchiveRuntimeOwner;
import org.tron.core.db2.checkpoint.common.CommonCheckpointRuntimeAttachment;
import org.tron.core.exception.BadItemException;
import org.tron.core.exception.ItemNotFoundException;

/** Canonical identity checks and request ownership for FullNode historical query sessions. */
public final class StateArchiveQuerySessions {

  private StateArchiveQuerySessions() {
  }

  public static HistoricalQuerySession open(long blockNumber, byte[] expectedBlockHash,
      HistoricalQueryAdmission historicalQueryAdmission, ChainBaseManager chainBaseManager,
      Supplier<StateArchiveRuntimeOwner> legacyRuntime,
      Supplier<CommonCheckpointRuntimeAttachment> commonRuntime)
      throws ItemNotFoundException, BadItemException {
    Objects.requireNonNull(expectedBlockHash, "expectedBlockHash");
    if (expectedBlockHash.length != 32) {
      throw new IllegalArgumentException("expectedBlockHash must be exactly 32 bytes");
    }
    HistoricalQuerySession.Limits limits = HistoricalQuerySession.Limits.defaults();
    HistoricalQueryAdmission.Request request = historicalQueryAdmission.acquire(
        CommonParameter.getInstance().getMaxHttpConnectNumber(), limits.getTimeoutMillis());
    HistoricalQuerySession session = null;
    boolean transferred = false;
    Throwable openFailure = null;
    try {
      StateArchiveRuntimeOwner runtime = legacyRuntime.get();
      CommonCheckpointRuntimeAttachment common = commonRuntime.get();
      if (runtime == null && common == null) {
        throw new HistoricalQueryException(Reason.UNAVAILABLE,
            "Experimental state archive is disabled");
      }
      request.getControl().checkActive();
      byte[] canonicalHash = chainBaseManager.getBlockByNum(blockNumber).getBlockId().getBytes();
      if (!Arrays.equals(expectedBlockHash, canonicalHash)) {
        throw new HistoricalQueryException(Reason.NON_CANONICAL,
            "Historical block number and hash do not match");
      }
      session = common == null
          ? HistoricalQuerySession.open(runtime.pinHistoricalState(blockNumber), canonicalHash,
              limits, request.getControl(), request::close)
          : HistoricalQuerySession.open(common.pinPoint(blockNumber, request.getControl()),
              canonicalHash, limits, request.getControl(), request::close);
      byte[] afterHash = chainBaseManager.getBlockByNum(blockNumber).getBlockId().getBytes();
      if (!Arrays.equals(canonicalHash, afterHash)) {
        throw new HistoricalQueryException(Reason.NON_CANONICAL,
            "Canonical historical block changed while opening query session");
      }
      session.requirePinnedIdentity();
      session.checkActive();
      transferred = true;
      return session;
    } catch (IOException failure) {
      openFailure = failure;
      throw new HistoricalQueryException(Reason.UNAVAILABLE,
          "Failed to open request-owned historical query session", failure);
    } catch (ItemNotFoundException | BadItemException | RuntimeException failure) {
      openFailure = failure;
      if (failure instanceof RuntimeException && !(failure instanceof HistoricalQueryException)) {
        throw new HistoricalQueryException(Reason.UNAVAILABLE,
            "Historical query view is not available", failure);
      }
      throw failure;
    } finally {
      if (!transferred) {
        try {
          if (session != null) {
            try {
              session.close();
            } catch (RuntimeException closeFailure) {
              if (openFailure == null) {
                throw closeFailure;
              }
              openFailure.addSuppressed(closeFailure);
            }
          }
        } finally {
          request.close();
        }
      }
    }
  }
}
