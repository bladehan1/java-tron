package org.tron.core.db2.stateroot;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Immutable identity and byte-format version for one path-state participant. */
public final class PathStateParticipant implements Comparable<PathStateParticipant> {

  private final int storeId;
  private final String dbName;
  private final int storeFormatVersion;

  public PathStateParticipant(int storeId, String dbName, int storeFormatVersion) {
    if (storeId <= 0) {
      throw new IllegalArgumentException("storeId must be positive");
    }
    this.dbName = Objects.requireNonNull(dbName, "dbName");
    int encodedNameLength = dbName.getBytes(StandardCharsets.UTF_8).length;
    if (encodedNameLength == 0 || encodedNameLength > 128) {
      throw new IllegalArgumentException("dbName must encode to 1..128 bytes");
    }
    if (storeFormatVersion <= 0) {
      throw new IllegalArgumentException("storeFormatVersion must be positive");
    }
    this.storeId = storeId;
    this.storeFormatVersion = storeFormatVersion;
  }

  public int getStoreId() {
    return storeId;
  }

  public String getDbName() {
    return dbName;
  }

  public int getStoreFormatVersion() {
    return storeFormatVersion;
  }

  @Override
  public int compareTo(PathStateParticipant other) {
    return Integer.compare(storeId, other.storeId);
  }

  @Override
  public String toString() {
    return storeId + ":" + dbName + ":v" + storeFormatVersion;
  }
}
