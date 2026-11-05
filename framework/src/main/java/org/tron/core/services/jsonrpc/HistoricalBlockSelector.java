package org.tron.core.services.jsonrpc;

import java.util.Map;
import org.tron.core.exception.jsonrpc.JsonRpcInvalidParamsException;

/** Shared selector syntax; every historical hash is subject to canonical admission. */
final class HistoricalBlockSelector {

  private final String numberOrTag;
  private final byte[] hash;

  private HistoricalBlockSelector(String numberOrTag, byte[] hash) {
    this.numberOrTag = numberOrTag;
    this.hash = hash;
  }

  static HistoricalBlockSelector parse(Object value) throws JsonRpcInvalidParamsException {
    if (value instanceof String) {
      return new HistoricalBlockSelector((String) value, null);
    }
    if (!(value instanceof Map)) {
      throw invalid();
    }
    Map<?, ?> fields = (Map<?, ?>) value;
    boolean hasNumber = fields.containsKey("blockNumber");
    boolean hasHash = fields.containsKey("blockHash");
    if (hasNumber == hasHash) {
      throw invalid();
    }
    for (Object key : fields.keySet()) {
      if (!"blockNumber".equals(key) && !"blockHash".equals(key)
          && !(hasHash && "requireCanonical".equals(key))) {
        throw invalid();
      }
    }
    if (fields.containsKey("requireCanonical")
        && !(fields.get("requireCanonical") instanceof Boolean)) {
      throw invalid();
    }
    Object selected = fields.get(hasHash ? "blockHash" : "blockNumber");
    if (!(selected instanceof String)) {
      throw invalid();
    }
    return hasHash
        ? new HistoricalBlockSelector(null, JsonRpcApiUtil.hashToByteArray((String) selected))
        : new HistoricalBlockSelector((String) selected, null);
  }

  boolean isLatest() {
    return hash == null && JsonRpcApiUtil.LATEST_STR.equalsIgnoreCase(numberOrTag);
  }

  String getNumberOrTag() {
    return numberOrTag;
  }

  byte[] getHash() {
    return hash == null ? null : java.util.Arrays.copyOf(hash, hash.length);
  }

  private static JsonRpcInvalidParamsException invalid() {
    return new JsonRpcInvalidParamsException("invalid historical block selector");
  }
}
