package org.tron.core.db2.stateroot;

import java.util.Arrays;
import java.util.Objects;

/** Immutable current-state mutation consumed by {@link PathStateRoot}. */
public final class PathStateMutation {

  private final String dbName;
  private final byte[] physicalKey;
  private final byte[] physicalValue;

  private PathStateMutation(String dbName, byte[] physicalKey, byte[] physicalValue) {
    this.dbName = Objects.requireNonNull(dbName, "dbName");
    this.physicalKey = copy(physicalKey, "physicalKey");
    this.physicalValue = physicalValue == null ? null
        : Arrays.copyOf(physicalValue, physicalValue.length);
  }

  public static PathStateMutation put(String dbName, byte[] physicalKey, byte[] physicalValue) {
    return new PathStateMutation(dbName, physicalKey,
        Objects.requireNonNull(physicalValue, "physicalValue"));
  }

  public static PathStateMutation delete(String dbName, byte[] physicalKey) {
    return new PathStateMutation(dbName, physicalKey, null);
  }

  public String getDbName() {
    return dbName;
  }

  /** Exact physical key bytes supplied by the Chainbase mutation/source boundary. */
  public byte[] getPhysicalKey() {
    return Arrays.copyOf(physicalKey, physicalKey.length);
  }

  /** Exact physical value bytes, or {@code null} for an absent/delete mutation. */
  public byte[] getPhysicalValue() {
    return physicalValue == null ? null : Arrays.copyOf(physicalValue, physicalValue.length);
  }

  /** @deprecated Use {@link #getPhysicalKey()}; this alias is retained for old-format callers. */
  @Deprecated
  public byte[] getCanonicalKey() {
    return getPhysicalKey();
  }

  public boolean isDelete() {
    return physicalValue == null;
  }

  /** @deprecated Use {@link #getPhysicalValue()}; this alias is retained for old-format callers. */
  @Deprecated
  public byte[] getCanonicalValue() {
    return getPhysicalValue();
  }

  private static byte[] copy(byte[] value, String name) {
    byte[] source = Objects.requireNonNull(value, name);
    return Arrays.copyOf(source, source.length);
  }
}
