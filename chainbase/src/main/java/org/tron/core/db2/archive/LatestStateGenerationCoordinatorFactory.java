package org.tron.core.db2.archive;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
    LatestStateGenerationAdapter adapter = createAdapter(manager);
    return new LatestStateGenerationCoordinator(adapter.participantsForCoordinator(),
        adapter.storesForCoordinator(), manager::withArchiveStateBarrier, authorityReader);
  }

  /** Builds a direct request-pinning adapter for the common-checkpoint read gate. */
  public static LatestStateGenerationAdapter createAdapter(SnapshotManager manager)
      throws ArchivePersistenceException {
    Objects.requireNonNull(manager, "manager");
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
    for (java.util.Map.Entry<String, Chainbase> entry : stateDatabases.entrySet()) {
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
    if (!stores.keySet().equals(expected)) {
      throw new ArchivePersistenceException(
          "SnapshotManager archive Store set is incomplete or unexpected");
    }

    List<String> participants = new ArrayList<>(stores.keySet());
    return new LatestStateGenerationAdapter(participants, stores);
  }
}
