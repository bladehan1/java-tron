package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import org.tron.core.db2.archive.AccountAssetForwardProjector.AssetMutation;
import org.tron.core.db2.archive.AccountAssetForwardProjector.Projection;
import org.tron.core.db2.archive.BlockChangeView.PostValue;
import org.tron.core.db2.archive.P66AccountAssetCodec.Phase;

/** Immutable one-shot account projection input bound to one exact committed target. */
public final class AccountAssetForwardMutationManifest implements AccountAssetForwardProjector {

  private final byte[] encodedTarget;
  private final String formatId;
  private final Phase targetPhase;
  private final TreeMap<Key, Entry> entries = new TreeMap<>();
  private final TreeSet<Key> consumed = new TreeSet<>();
  private boolean begun;
  private boolean completed;

  public AccountAssetForwardMutationManifest(HistoryCommitMarker target, Phase targetPhase,
      List<Entry> entries) {
    HistoryCommitMarker expectedTarget = Objects.requireNonNull(target, "target");
    if (!expectedTarget.getDatabases().equals(sortedParticipants())) {
      throw new IllegalArgumentException("Manifest target must cover exact VERSIONED_STATE set");
    }
    encodedTarget = new HistoryCommitMarkerCodec().encode(expectedTarget);
    formatId = P66AccountAssetCodec.FORMAT_ID;
    this.targetPhase = Objects.requireNonNull(targetPhase, "targetPhase");
    for (Entry entry : Objects.requireNonNull(entries, "entries")) {
      if (entry == null) {
        throw new IllegalArgumentException("Manifest contains null entry");
      }
      Key key = new Key(entry.accountPhysicalKey);
      if (this.entries.put(key, entry) != null) {
        throw new IllegalArgumentException("Duplicate manifest account physical key");
      }
    }
  }

  String getFormatId() {
    return formatId;
  }

  Phase getTargetPhase() {
    return targetPhase;
  }

  @Override
  public synchronized void begin(HistoryCommitMarker target,
      List<byte[]> changedAccountPhysicalKeys) {
    if (begun || completed) {
      throw new ArchivePersistenceException("AccountAsset manifest is one-shot");
    }
    if (!Arrays.equals(encodedTarget,
        new HistoryCommitMarkerCodec().encode(Objects.requireNonNull(target, "target")))) {
      throw new ArchivePersistenceException("AccountAsset manifest target identity mismatch");
    }
    TreeMap<Key, Boolean> changed = new TreeMap<>();
    for (byte[] key : Objects.requireNonNull(changedAccountPhysicalKeys,
        "changedAccountPhysicalKeys")) {
      if (changed.put(new Key(key), Boolean.TRUE) != null) {
        throw new ArchivePersistenceException("Duplicate changed account physical key");
      }
    }
    if (!changed.keySet().equals(entries.keySet())) {
      throw new ArchivePersistenceException(
          "AccountAsset manifest does not exactly cover changed account keys");
    }
    begun = true;
  }

  @Override
  public synchronized Projection project(byte[] accountPhysicalKey,
      PostValue rawAccountPostValue) {
    if (!begun || completed) {
      throw new ArchivePersistenceException("AccountAsset manifest is not active");
    }
    Entry entry = entries.get(new Key(accountPhysicalKey));
    if (entry == null) {
      throw new ArchivePersistenceException("AccountAsset manifest entry is missing");
    }
    Key key = new Key(accountPhysicalKey);
    if (consumed.contains(key)) {
      throw new ArchivePersistenceException("AccountAsset manifest entry was already consumed");
    }
    if (!samePostValue(entry.rawAccountPostValue,
        Objects.requireNonNull(rawAccountPostValue, "rawAccountPostValue"))) {
      throw new ArchivePersistenceException("AccountAsset manifest raw account value mismatch");
    }
    consumed.add(key);
    return entry.projection;
  }

  @Override
  public synchronized void complete() {
    if (!begun || completed) {
      throw new ArchivePersistenceException("AccountAsset manifest is not active");
    }
    if (consumed.size() != entries.size()) {
      throw new ArchivePersistenceException("AccountAsset manifest contains unused entry");
    }
    completed = true;
  }

  private static boolean samePostValue(PostValue left, PostValue right) {
    return left.isPresent() == right.isPresent()
        && (!left.isPresent() || Arrays.equals(left.getValue(), right.getValue()));
  }

  private static List<String> sortedParticipants() {
    return ArchiveParticipantDescriptor.current().getParticipants();
  }

  /** One changed account's exact raw input and canonical physical outputs. */
  public static final class Entry {
    private final byte[] accountPhysicalKey;
    private final PostValue rawAccountPostValue;
    private final Projection projection;

    public Entry(byte[] accountPhysicalKey, PostValue rawAccountPostValue,
        PostValue canonicalAccountPostValue, List<AssetMutation> assetMutations) {
      this.accountPhysicalKey = Arrays.copyOf(
          Objects.requireNonNull(accountPhysicalKey, "accountPhysicalKey"),
          accountPhysicalKey.length);
      this.rawAccountPostValue = Objects.requireNonNull(rawAccountPostValue,
          "rawAccountPostValue");
      PostValue canonical = Objects.requireNonNull(canonicalAccountPostValue,
          "canonicalAccountPostValue");
      if (rawAccountPostValue.isPresent() != canonical.isPresent()) {
        throw new IllegalArgumentException(
            "Raw and canonical account presence must match");
      }
      List<AssetMutation> mutations = new ArrayList<>(Objects.requireNonNull(assetMutations,
          "assetMutations"));
      TreeMap<Key, Boolean> assetKeys = new TreeMap<>();
      for (AssetMutation mutation : mutations) {
        if (mutation == null) {
          throw new IllegalArgumentException("Manifest entry contains null asset mutation");
        }
        byte[] assetKey = mutation.getPhysicalRawKey();
        if (!strictlyExtends(this.accountPhysicalKey, assetKey)) {
          throw new IllegalArgumentException(
              "AccountAsset physical key does not belong to account");
        }
        if (assetKeys.put(new Key(assetKey), Boolean.TRUE) != null) {
          throw new IllegalArgumentException("Duplicate account-asset physical key");
        }
      }
      projection = new Projection(canonical, mutations);
    }

    private static boolean strictlyExtends(byte[] prefix, byte[] value) {
      if (value.length <= prefix.length) {
        return false;
      }
      for (int i = 0; i < prefix.length; i++) {
        if (prefix[i] != value[i]) {
          return false;
        }
      }
      return true;
    }
  }

  private static final class Key implements Comparable<Key> {
    private final byte[] value;

    private Key(byte[] value) {
      this.value = Arrays.copyOf(Objects.requireNonNull(value, "key"), value.length);
    }

    @Override
    public int compareTo(Key other) {
      return BlockReverseDiff.compareUnsigned(value, other.value);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Key && Arrays.equals(value, ((Key) object).value);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(value);
    }
  }
}
