package org.tron.core.db2.archive;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.SnapshotCapableStore;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.core.Chainbase;
import org.tron.core.db2.core.Snapshot;
import org.tron.core.db2.core.SnapshotManager;
import org.tron.core.db2.core.SnapshotRoot;

/** Builds a latest-state coordinator from one frozen SnapshotManager Store registry. */
public final class LatestStateGenerationCoordinatorFactory {

  private LatestStateGenerationCoordinatorFactory() {
  }

  public static LatestStateGenerationCoordinator create(SnapshotManager manager,
      Path readerVisiblePath) throws ArchivePersistenceException {
    Objects.requireNonNull(readerVisiblePath, "readerVisiblePath");
    ArchiveProgressFile readerVisible = new ArchiveProgressFile(readerVisiblePath,
        new ArchiveProgressEnvelopeCodec());
    return create(manager, readerVisible::load);
  }

  public static LatestStateGenerationCoordinator create(SnapshotManager manager,
      LatestStateGenerationCoordinator.AuthorityReader authorityReader)
      throws ArchivePersistenceException {
    Objects.requireNonNull(authorityReader, "authorityReader");
    return create(manager, java.util.Collections.emptyMap(), authorityReader);
  }

  public static LatestStateGenerationCoordinator create(SnapshotManager manager,
      Map<String, SnapshotCapableStore> supplementalStores,
      LatestStateGenerationCoordinator.AuthorityReader authorityReader)
      throws ArchivePersistenceException {
    LatestStateGenerationAdapter adapter = createAdapter(manager, supplementalStores);
    return new LatestStateGenerationCoordinator(adapter.participantsForCoordinator(),
        adapter.storesForCoordinator(), manager::withArchiveStateBarrier, authorityReader);
  }

  /** Builds a direct request-pinning adapter for the common-checkpoint read gate. */
  public static LatestStateGenerationAdapter createAdapter(SnapshotManager manager,
      Map<String, SnapshotCapableStore> supplementalStores)
      throws ArchivePersistenceException {
    Objects.requireNonNull(manager, "manager");
    Objects.requireNonNull(supplementalStores, "supplementalStores");
    List<Chainbase> registered = new ArrayList<>(manager.getDbs());
    try {
      ArchiveStoreScope.validate(registered);
    } catch (IllegalStateException invalid) {
      throw new ArchivePersistenceException("Invalid SnapshotManager archive Store registry",
          invalid);
    }

    TreeMap<String, Chainbase> stateDatabases = new TreeMap<>();
    for (Chainbase database : registered) {
      if (ArchiveStoreScope.isStateDatabase(database.getDbName())) {
        stateDatabases.put(database.getDbName(), database);
      }
    }
    TreeSet<String> expected = new TreeSet<>(ArchiveStoreScope.getStateDatabases());
    if (!expected.containsAll(stateDatabases.keySet())) {
      throw new ArchivePersistenceException(
          "SnapshotManager archive state Store set is unexpected");
    }

    TreeMap<String, SnapshotCapableStore> stores = new TreeMap<>();
    for (Map.Entry<String, Chainbase> entry : stateDatabases.entrySet()) {
      Snapshot root = entry.getValue().getHead().getRoot();
      if (!Snapshot.isRoot(root)) {
        throw new ArchivePersistenceException(
            "Archive state Store does not resolve to SnapshotRoot: " + entry.getKey());
      }
      DB<byte[], byte[]> engine = ((SnapshotRoot) root).getDb();
      if (!(engine instanceof SnapshotCapableStore)
          || !entry.getKey().equals(engine.getDbName())) {
        throw new ArchivePersistenceException(
            "Archive state Store root lacks a supported snapshot engine: " + entry.getKey());
      }
      stores.put(entry.getKey(), (SnapshotCapableStore) engine);
    }
    for (Map.Entry<String, SnapshotCapableStore> entry : supplementalStores.entrySet()) {
      SnapshotCapableStore store = Objects.requireNonNull(entry.getValue(),
          "supplemental Store");
      if (!entry.getKey().equals(store.getDbName())
          || stores.putIfAbsent(entry.getKey(), store) != null) {
        throw new ArchivePersistenceException(
            "Duplicate or mismatched supplemental latest Store: " + entry.getKey());
      }
    }
    if (!stores.keySet().equals(expected)) {
      throw new ArchivePersistenceException(
          "SnapshotManager plus supplemental archive Store set is incomplete or unexpected");
    }

    List<String> participants = new ArrayList<>(stores.keySet());
    return new LatestStateGenerationAdapter(participants, stores);
  }
}
