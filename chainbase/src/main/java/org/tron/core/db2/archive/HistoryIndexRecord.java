package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Authoritative block-to-body location and per-key changed delta. */
public final class HistoryIndexRecord {

  private final BlockSnapshotMeta meta;
  private final HistoryLocation historyLocation;
  private final List<KeyGroup> groups;

  public HistoryIndexRecord(BlockSnapshotMeta meta, HistoryLocation historyLocation,
      List<KeyGroup> groups) {
    this.meta = Objects.requireNonNull(meta, "meta");
    this.historyLocation = Objects.requireNonNull(historyLocation, "historyLocation");
    this.groups = Collections.unmodifiableList(new ArrayList<>(groups));
  }

  public static HistoryIndexRecord from(BlockReverseDiff diff, HistoryLocation location) {
    List<KeyGroup> groups = new ArrayList<>();
    diff.getGroups().forEach(group -> {
      List<byte[]> keys = new ArrayList<>();
      group.getEntries().forEach(entry -> keys.add(entry.getKey()));
      groups.add(new KeyGroup(group.getDbName(), keys));
    });
    return new HistoryIndexRecord(diff.getMeta(), location, groups);
  }

  public BlockSnapshotMeta getMeta() {
    return meta;
  }

  public HistoryLocation getHistoryLocation() {
    return historyLocation;
  }

  public List<KeyGroup> getGroups() {
    return groups;
  }

  public static final class KeyGroup {
    private final String dbName;
    private final List<byte[]> keys;

    public KeyGroup(String dbName, List<byte[]> keys) {
      this.dbName = Objects.requireNonNull(dbName, "dbName");
      List<byte[]> copied = new ArrayList<>();
      keys.forEach(key -> copied.add(Arrays.copyOf(key, key.length)));
      this.keys = Collections.unmodifiableList(copied);
    }

    public String getDbName() {
      return dbName;
    }

    public List<byte[]> getKeys() {
      List<byte[]> copied = new ArrayList<>();
      keys.forEach(key -> copied.add(Arrays.copyOf(key, key.length)));
      return copied;
    }
  }
}
