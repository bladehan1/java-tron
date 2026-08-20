package org.tron.core.db2.archive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import org.tron.core.db2.archive.AccountAssetForwardMutationManifest.Entry;
import org.tron.core.db2.archive.AccountAssetForwardProjector.AssetMutation;
import org.tron.core.db2.archive.BlockChangeView.PostValue;

/** Collects explicit execution-time AccountAsset events for one target without Store reads. */
public final class AccountAssetForwardMutationRecorder {

  private final BlockSnapshotMeta targetMeta;
  private final ArchiveBlockForwardMutationLimits limits;
  private final TreeMap<Key, AccountEvents> accounts = new TreeMap<>();
  private int accountCount;
  private int assetMutationCount;
  private long totalPayloadBytes;
  private boolean sealed;

  public AccountAssetForwardMutationRecorder(BlockSnapshotMeta targetMeta,
      ArchiveBlockForwardMutationLimits limits) {
    this.targetMeta = Objects.requireNonNull(targetMeta, "targetMeta");
    this.limits = Objects.requireNonNull(limits, "limits");
  }

  public synchronized void recordAccount(BlockSnapshotMeta eventMeta,
      byte[] accountPhysicalKey, PostValue rawAccountPostValue,
      PostValue canonicalAccountPostValue) {
    requireOpenMeta(eventMeta);
    PostValue raw = Objects.requireNonNull(rawAccountPostValue, "rawAccountPostValue");
    PostValue canonical = Objects.requireNonNull(canonicalAccountPostValue,
        "canonicalAccountPostValue");
    if (raw.isPresent() != canonical.isPresent()) {
      throw new ArchivePersistenceException(
          "Raw and canonical account presence must match");
    }
    Key key = new Key(accountPhysicalKey);
    requireKeyLength(key.value.length);
    AccountEvents current = accounts.get(key);
    if (current != null && current.rawAccountPostValue != null) {
      throw new ArchivePersistenceException("Duplicate AccountAsset account event");
    }
    long rawBytes = valueLength(raw);
    long canonicalBytes = valueLength(canonical);
    AccountEvents account = current == null ? new AccountEvents(key.value) : current;
    reserve(current == null, false,
        (current == null ? key.value.length : 0L) + rawBytes + canonicalBytes);
    if (current == null) {
      accounts.put(key, account);
    }
    account.rawAccountPostValue = raw;
    account.canonicalAccountPostValue = canonical;
  }

  public synchronized void recordAssetPut(BlockSnapshotMeta eventMeta,
      byte[] accountPhysicalKey, byte[] assetPhysicalKey, byte[] value) {
    recordAsset(eventMeta, accountPhysicalKey, assetPhysicalKey,
        PostValue.present(Objects.requireNonNull(value, "value")));
  }

  public synchronized void recordAssetDelete(BlockSnapshotMeta eventMeta,
      byte[] accountPhysicalKey, byte[] assetPhysicalKey) {
    recordAsset(eventMeta, accountPhysicalKey, assetPhysicalKey, PostValue.absent());
  }

  public synchronized AccountAssetForwardMutationManifest seal(HistoryCommitMarker sealTarget) {
    requireOpen();
    HistoryCommitMarker committedTarget = Objects.requireNonNull(sealTarget, "sealTarget");
    if (!targetMeta.equals(committedTarget.getMeta())) {
      throw new ArchivePersistenceException(
          "AccountAsset recorder seal target meta mismatch");
    }
    List<Entry> entries = new ArrayList<>();
    for (AccountEvents account : accounts.values()) {
      if (account.rawAccountPostValue == null) {
        throw new ArchivePersistenceException(
            "AccountAsset recorder contains incomplete account");
      }
      entries.add(new Entry(account.accountPhysicalKey, account.rawAccountPostValue,
          account.canonicalAccountPostValue, new ArrayList<>(account.assets.values())));
    }
    AccountAssetForwardMutationManifest manifest =
        new AccountAssetForwardMutationManifest(committedTarget, entries);
    sealed = true;
    clearPayload();
    return manifest;
  }

  synchronized void discard() {
    requireOpen();
    sealed = true;
    clearPayload();
  }

  synchronized boolean isPayloadReleased() {
    return accounts.isEmpty()
        && accountCount == 0
        && assetMutationCount == 0
        && totalPayloadBytes == 0;
  }

  synchronized void reserveView(BlockChangeView view) {
    requireOpen();
    long additionalBytes = 0;
    long remaining = limits.getMaxTotalPayloadBytes() - totalPayloadBytes;
    for (BlockChangeView.DatabaseChanges database : view.getDatabases()) {
      for (BlockChangeView.Change change : database.getChanges()) {
        byte[] key = change.getKey();
        requireKeyLength(key.length);
        long entryBytes = key.length + valueLength(change.getPostValue());
        if (entryBytes > remaining - additionalBytes) {
          throw new ArchivePersistenceException(
              "Block forward mutation payload exceeds total limit");
        }
        additionalBytes += entryBytes;
      }
    }
    reserve(false, false, additionalBytes);
  }

  private void recordAsset(BlockSnapshotMeta eventMeta, byte[] accountPhysicalKey,
      byte[] assetPhysicalKey, PostValue postValue) {
    requireOpenMeta(eventMeta);
    Key accountKey = new Key(accountPhysicalKey);
    byte[] assetKey = Arrays.copyOf(Objects.requireNonNull(assetPhysicalKey,
        "assetPhysicalKey"), assetPhysicalKey.length);
    requireKeyLength(accountKey.value.length);
    requireKeyLength(assetKey.length);
    if (!strictlyExtends(accountKey.value, assetKey)) {
      throw new ArchivePersistenceException(
          "AccountAsset physical key does not belong to account");
    }
    AccountEvents current = accounts.get(accountKey);
    Key mutationKey = new Key(assetKey);
    if (current != null && current.assets.containsKey(mutationKey)) {
      throw new ArchivePersistenceException("Duplicate AccountAsset asset event");
    }
    long valueBytes = valueLength(postValue);
    AccountEvents account = current == null ? new AccountEvents(accountKey.value) : current;
    AssetMutation mutation = new AssetMutation(assetKey, postValue);
    reserve(current == null, true,
        (current == null ? accountKey.value.length : 0L) + assetKey.length + valueBytes);
    if (current == null) {
      accounts.put(accountKey, account);
    }
    account.assets.put(mutationKey, mutation);
  }

  private long valueLength(PostValue value) {
    long length = value.isPresent() ? value.getValue().length : 0L;
    if (length > limits.getMaxValueBytes()) {
      throw new ArchivePersistenceException("Block forward mutation value exceeds limit");
    }
    return length;
  }

  private void requireKeyLength(int length) {
    if (length > limits.getMaxKeyBytes()) {
      throw new ArchivePersistenceException("Block forward mutation key exceeds limit");
    }
  }

  private void reserve(boolean newAccount, boolean newAsset, long additionalBytes) {
    if (newAccount && accountCount >= limits.getMaxAccounts()) {
      throw new ArchivePersistenceException("Block forward mutation account count exceeds limit");
    }
    if (newAsset && assetMutationCount >= limits.getMaxAssetMutations()) {
      throw new ArchivePersistenceException("Block forward mutation asset count exceeds limit");
    }
    long remaining = limits.getMaxTotalPayloadBytes() - totalPayloadBytes;
    if (additionalBytes < 0 || additionalBytes > remaining) {
      throw new ArchivePersistenceException("Block forward mutation payload exceeds total limit");
    }
    if (newAccount) {
      accountCount++;
    }
    if (newAsset) {
      assetMutationCount++;
    }
    totalPayloadBytes += additionalBytes;
  }

  private void requireOpenMeta(BlockSnapshotMeta eventMeta) {
    requireOpen();
    if (!targetMeta.equals(Objects.requireNonNull(eventMeta, "eventMeta"))) {
      throw new ArchivePersistenceException("AccountAsset recorder event meta mismatch");
    }
  }

  private void requireOpen() {
    if (sealed) {
      throw new ArchivePersistenceException("AccountAsset recorder is already sealed");
    }
  }

  private void clearPayload() {
    accounts.clear();
    accountCount = 0;
    assetMutationCount = 0;
    totalPayloadBytes = 0;
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

  private static final class AccountEvents {
    private final byte[] accountPhysicalKey;
    private final TreeMap<Key, AssetMutation> assets = new TreeMap<>();
    private PostValue rawAccountPostValue;
    private PostValue canonicalAccountPostValue;

    private AccountEvents(byte[] accountPhysicalKey) {
      this.accountPhysicalKey = Arrays.copyOf(
          Objects.requireNonNull(accountPhysicalKey, "accountPhysicalKey"),
          accountPhysicalKey.length);
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
  }
}
