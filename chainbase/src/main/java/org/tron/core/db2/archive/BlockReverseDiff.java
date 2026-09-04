package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.tron.core.db2.archive.BlockSnapshotMeta;

/** Immutable reverse diff for one canonical block. */
public final class BlockReverseDiff {

  private final BlockSnapshotMeta meta;
  private final List<DbGroup> groups;
  private final byte[] mutationViewDigest;

  public BlockReverseDiff(BlockSnapshotMeta meta, List<DbGroup> groups) {
    this(meta, groups, null);
  }

  public BlockReverseDiff(BlockSnapshotMeta meta, List<DbGroup> groups,
      byte[] mutationViewDigest) {
    this.meta = Objects.requireNonNull(meta, "meta");
    List<DbGroup> sorted = new ArrayList<>(groups);
    sorted.sort(Comparator.comparing(DbGroup::getDbName));
    this.groups = Collections.unmodifiableList(sorted);
    this.mutationViewDigest = optionalDigest(mutationViewDigest);
  }

  public BlockSnapshotMeta getMeta() {
    return meta;
  }

  public List<DbGroup> getGroups() {
    return groups;
  }

  /** Returns the block-final mutation-view identity, or null for decoded legacy payloads. */
  public byte[] getMutationViewDigest() {
    return mutationViewDigest == null ? null
        : Arrays.copyOf(mutationViewDigest, mutationViewDigest.length);
  }

  private static byte[] optionalDigest(byte[] supplied) {
    if (supplied == null) {
      return null;
    }
    byte[] copy = Arrays.copyOf(supplied, supplied.length);
    if (copy.length != 32) {
      throw new IllegalArgumentException("mutationViewDigest must contain exactly 32 bytes");
    }
    return copy;
  }

  public static final class DbGroup {
    private final String dbName;
    private final List<Entry> entries;

    public DbGroup(String dbName, List<Entry> entries) {
      this.dbName = Objects.requireNonNull(dbName, "dbName");
      List<Entry> sorted = new ArrayList<>(entries);
      sorted.sort((left, right) -> compareUnsigned(left.key, right.key));
      this.entries = Collections.unmodifiableList(sorted);
    }

    public String getDbName() {
      return dbName;
    }

    public List<Entry> getEntries() {
      return entries;
    }
  }

  public static final class Entry {
    private final byte[] key;
    private final OldValue oldValue;

    public Entry(byte[] key, OldValue oldValue) {
      Objects.requireNonNull(key, "key");
      this.key = Arrays.copyOf(key, key.length);
      this.oldValue = Objects.requireNonNull(oldValue, "oldValue");
    }

    public byte[] getKey() {
      return Arrays.copyOf(key, key.length);
    }

    public OldValue getOldValue() {
      return oldValue;
    }
  }

  static int compareUnsigned(byte[] left, byte[] right) {
    int length = Math.min(left.length, right.length);
    for (int i = 0; i < length; i++) {
      int comparison = Integer.compare(left[i] & 0xff, right[i] & 0xff);
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(left.length, right.length);
  }
}
