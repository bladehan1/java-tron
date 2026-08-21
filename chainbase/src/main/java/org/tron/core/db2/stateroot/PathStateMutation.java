package org.tron.core.db2.stateroot;

import java.util.Arrays;
import java.util.Objects;

/** Immutable current-state mutation consumed by {@link PathStateRoot}. */
public final class PathStateMutation {

  private final String dbName;
  private final byte[] canonicalKey;
  private final byte[] canonicalValue;

  private PathStateMutation(String dbName, byte[] canonicalKey, byte[] canonicalValue) {
    this.dbName = Objects.requireNonNull(dbName, "dbName");
    this.canonicalKey = copy(canonicalKey, "canonicalKey");
    this.canonicalValue = canonicalValue == null ? null
        : Arrays.copyOf(canonicalValue, canonicalValue.length);
  }

  public static PathStateMutation put(String dbName, byte[] canonicalKey, byte[] canonicalValue) {
    return new PathStateMutation(dbName, canonicalKey,
        Objects.requireNonNull(canonicalValue, "canonicalValue"));
  }

  public static PathStateMutation delete(String dbName, byte[] canonicalKey) {
    return new PathStateMutation(dbName, canonicalKey, null);
  }

  public String getDbName() {
    return dbName;
  }

  public byte[] getCanonicalKey() {
    return Arrays.copyOf(canonicalKey, canonicalKey.length);
  }

  public boolean isDelete() {
    return canonicalValue == null;
  }

  public byte[] getCanonicalValue() {
    return canonicalValue == null ? null : Arrays.copyOf(canonicalValue, canonicalValue.length);
  }

  private static byte[] copy(byte[] value, String name) {
    byte[] source = Objects.requireNonNull(value, name);
    return Arrays.copyOf(source, source.length);
  }
}
