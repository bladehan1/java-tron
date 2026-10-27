package org.tron.core.db2.core.p66;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.tron.core.db2.common.Key;
import org.tron.core.db2.common.Value;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.Snapshot;
import org.tron.core.db2.core.SnapshotImpl;
import org.tron.core.db2.core.SnapshotRoot;
import org.tron.protos.Protocol.Account;

/**
 * Materializes coupled Account/AccountAsset physical writes once, inside the active revocable
 * block layer (design 07 §3.9.4, 07-D).
 *
 * <p>Every row of the 07 §3.9.4 table mirrors upstream {@code SnapshotRoot.processAccount}
 * (GreatVoyage-v4.8.2.3 {@code SnapshotRoot.java:124-148}) applied to this block layer alone:
 * <ol>
 *   <li>P66 = 0: no operation.</li>
 *   <li>DELETE: the Account stays deleted; every visible asset row of the address is deleted.</li>
 *   <li>PUT of empty bytes: the Account stays an empty PUT; every visible row is deleted.</li>
 *   <li>PUT, {@code asset_optimized = false}: every visible row is deleted first (upstream
 *       {@code getDeletedAssets}), then the map rows are written (non-zero PUT, zero DELETE),
 *       and the Account is rewritten optimized with both maps cleared.  This also covers an
 *       Account deleted and re-created inside one block layer, whose DELETE was overwritten by
 *       the final un-optimized PUT (review R4-07).</li>
 *   <li>PUT, {@code asset_optimized = true}: map rows (non-zero PUT, zero DELETE); maps cleared.</li>
 *   <li>Account key not 21 bytes, or key differs from {@code Account.address}: local corruption,
 *       {@link IllegalStateException} (classified F by the block finalization).</li>
 *   <li>Flag missing, not 8 bytes, not 0/1, or moving 1 to 0: local corruption, same as 6.</li>
 * </ol>
 * The complete plan is built in memory before either Snapshot layer is modified.
 */
public final class P66CoupledMutationMaterializer {
  private static final byte[] FLAG =
      "ALLOW_ASSET_OPTIMIZATION".getBytes(StandardCharsets.US_ASCII);
  private static final int ADDRESS_LENGTH = 21;
  private final Chainbase accounts;
  private final Chainbase assets;
  private final Chainbase properties;

  public P66CoupledMutationMaterializer(Chainbase accounts, Chainbase assets,
      Chainbase properties) {
    this.accounts = accounts;
    this.assets = assets;
    this.properties = properties;
  }

  public Statistics materialize() {
    if (!Snapshot.isImpl(accounts.getHead()) || !Snapshot.isImpl(assets.getHead())
        || !Snapshot.isImpl(properties.getHead())) {
      throw new IllegalStateException("P66 materialization requires active Snapshot layers");
    }
    Statistics stats = new Statistics();
    byte[] flag = properties.getUnchecked(FLAG);
    if (flag == null || flag.length != Long.BYTES
        || (Longs.fromByteArray(flag) != 0 && Longs.fromByteArray(flag) != 1)) {
      throw new IllegalStateException("P66 post-state property must be 0 or 1");
    }
    byte[] previous = properties.getHead().getPrevious().get(FLAG);
    if (previous != null && previous.length == Long.BYTES
        && Longs.fromByteArray(previous) == 1 && Longs.fromByteArray(flag) == 0) {
      throw new IllegalStateException("P66 phase cannot move backwards");
    }
    if (Longs.fromByteArray(flag) == 0) {
      return stats;
    }
    // Build the complete deterministic plan before modifying either Snapshot map.
    Map<byte[], byte[]> accountPlan = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
    Map<byte[], byte[]> assetPlan = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
    List<Map.Entry<Key, Value>> changed = new ArrayList<>();
    ((SnapshotImpl) accounts.getHead()).getDb().forEach(changed::add);
    for (Map.Entry<Key, Value> entry : changed) {
      byte[] address = entry.getKey().getBytes();
      if (address.length != ADDRESS_LENGTH) {
        throw new IllegalStateException("P66 Account key must be exactly 21 bytes");
      }
      stats.changedAccounts++;
      byte[] bytes = entry.getValue().getBytes();
      if (entry.getValue().getOperator() == Value.Operator.DELETE
          || bytes == null || bytes.length == 0) {
        // Rows 2 and 3: the Account op itself is kept as-is (DELETE or empty PUT).
        planPrefixDelete(address, assetPlan, stats);
        continue;
      }
      Account account;
      try {
        account = Account.parseFrom(bytes);
      } catch (InvalidProtocolBufferException invalid) {
        throw new IllegalStateException("Invalid P66 Account bytes", invalid);
      }
      if (!Arrays.equals(address, account.getAddress().toByteArray())) {
        throw new IllegalStateException("P66 Account key/address mismatch");
      }
      if (!account.getAssetOptimized()) {
        // Row 4: upstream getDeletedAssets before setAssetOptimized(true).
        planPrefixDelete(address, assetPlan, stats);
        stats.migratedAccounts++;
      }
      if (account.getAssetV2Map().isEmpty()) {
        stats.emptyAssetSkips++;
      }
      // Upstream AccountAssetStore.getAssets: non-zero PUT, zero DELETE; it overrides the
      // prefix deletes above for the same key.
      for (Map.Entry<String, Long> asset : account.getAssetV2Map().entrySet()) {
        byte[] key = Bytes.concat(address, asset.getKey().getBytes(StandardCharsets.UTF_8));
        long value = asset.getValue();
        assetPlan.put(key, value == 0 ? null : Longs.toByteArray(value));
      }
      accountPlan.put(address, account.toBuilder().clearAsset().clearAssetV2()
          .setAssetOptimized(true).build().toByteArray());
    }
    assetPlan.forEach((key, value) -> {
      if (value == null) {
        assets.delete(key);
        stats.assetDeletes++;
      } else {
        assets.put(key, value);
        stats.assetPuts++;
      }
    });
    accountPlan.forEach(accounts::put);
    return stats;
  }

  private void planPrefixDelete(byte[] address, Map<byte[], byte[]> assetPlan,
      Statistics stats) {
    stats.prefixQueries++;
    Map<WrappedByteArray, byte[]> rows = assets.prefixQuery(address);
    stats.prefixRows += rows.size();
    rows.forEach((key, value) -> assetPlan.put(key.getBytes(), null));
  }

  public static final class Statistics {
    public long changedAccounts;
    public long migratedAccounts;
    public long emptyAssetSkips;
    public long assetPuts;
    public long assetDeletes;
    public long prefixQueries;
    public long prefixRows;
  }
}
