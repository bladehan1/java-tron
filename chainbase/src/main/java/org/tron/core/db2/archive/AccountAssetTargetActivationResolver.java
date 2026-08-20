package org.tron.core.db2.archive;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.archive.AccountAssetBlockProjectionBridge.TargetAssetOptimization;
import org.tron.core.db2.archive.BlockChangeView.Change;
import org.tron.core.db2.archive.BlockChangeView.DatabaseChanges;
import org.tron.core.db2.archive.BlockChangeView.PostValue;

/** Resolves proposal-66 activation from the target block's exact properties post view. */
public final class AccountAssetTargetActivationResolver {

  static final String PROPERTIES_DB = "properties";
  static final String PROPOSAL_66_KEY = "ALLOW_ASSET_OPTIMIZATION";
  static final String PROPOSAL_53_KEY = "ALLOW_ACCOUNT_ASSET_OPTIMIZATION";

  private static final byte[] PROPOSAL_66_PHYSICAL_KEY =
      PROPOSAL_66_KEY.getBytes(StandardCharsets.UTF_8);

  public TargetAssetOptimization resolve(BlockSnapshotMeta target, BlockChangeView view) {
    BlockSnapshotMeta expectedTarget = Objects.requireNonNull(target, "target");
    BlockChangeView input = Objects.requireNonNull(view, "view");
    if (!expectedTarget.equals(input.getMeta())) {
      throw new ArchivePersistenceException("Asset optimization target identity mismatch");
    }

    DatabaseChanges properties = null;
    for (DatabaseChanges database : input.getDatabases()) {
      if (PROPERTIES_DB.equals(database.getDbName())) {
        if (properties != null) {
          throw new ArchivePersistenceException("Duplicate properties block view");
        }
        properties = database;
      }
    }
    if (properties == null) {
      throw new ArchivePersistenceException("Missing properties block view");
    }

    byte[] value = null;
    boolean changed = false;
    for (Change change : properties.getChanges()) {
      if (Arrays.equals(PROPOSAL_66_PHYSICAL_KEY, change.getKey())) {
        if (changed) {
          throw new ArchivePersistenceException("Duplicate proposal-66 property mutation");
        }
        changed = true;
        PostValue postValue = change.getPostValue();
        if (!postValue.isPresent()) {
          throw new ArchivePersistenceException("Proposal-66 property must not be deleted");
        }
        value = postValue.getValue();
      }
    }
    if (!changed) {
      value = properties.getPrevious(PROPOSAL_66_PHYSICAL_KEY);
    }
    return TargetAssetOptimization.forTarget(expectedTarget, decode(value));
  }

  static byte[] proposal66PhysicalKey() {
    return Arrays.copyOf(PROPOSAL_66_PHYSICAL_KEY, PROPOSAL_66_PHYSICAL_KEY.length);
  }

  private static boolean decode(byte[] value) {
    if (value == null) {
      throw new ArchivePersistenceException("Missing proposal-66 property value");
    }
    if (value.length != Long.BYTES) {
      throw new ArchivePersistenceException("Proposal-66 property value must be exactly 8 bytes");
    }
    long decoded = ByteArray.toLong(value);
    if (decoded != 0L && decoded != 1L) {
      throw new ArchivePersistenceException("Proposal-66 property value must be 0 or 1");
    }
    return decoded == 1L;
  }
}
