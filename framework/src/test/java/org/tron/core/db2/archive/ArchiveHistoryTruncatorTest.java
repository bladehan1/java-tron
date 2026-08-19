package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveHistoryTruncator.Stage;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;

public class ArchiveHistoryTruncatorTest {

  private static final List<String> PARTICIPANTS = Collections.singletonList("account");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void everyStageCrashKeepsCommitAuthoritativeAndSecondRecoveryConverges()
      throws Exception {
    for (Stage failedStage : Stage.values()) {
      Path archive = temporaryFolder.newFolder("truncate-" + failedStage).toPath();
      initialize(archive);

      try (Stores stores = new Stores(archive)) {
        ArchiveHistoryTruncator truncator = new ArchiveHistoryTruncator(stores.commits,
            stores.index, stores.bodies, stage -> {
          if (stage == failedStage) {
            throw new IOException("injected after " + stage);
          }
        });
        assertThrows(IOException.class, () -> truncator.truncateAfter(10));
      }

      try (Stores afterCrash = new Stores(archive)) {
        assertEquals(10, commitHead(afterCrash));
        assertNull(afterCrash.commits.get(11));
        assertEquals(failedStage == Stage.COMMIT_AUTHORITY ? 12 : 10,
            indexHead(afterCrash));
        assertEquals(failedStage == Stage.HISTORY_BODY ? 10 : 12,
            bodyHead(afterCrash));
        new ArchiveHistoryTruncator(afterCrash.commits, afterCrash.index,
            afterCrash.bodies).truncateAfter(10);
      }

      try (Stores recovered = new Stores(archive)) {
        assertEquals(10, commitHead(recovered));
        assertEquals(10, indexHead(recovered));
        assertEquals(10, bodyHead(recovered));
        new ArchiveHistoryTruncator(recovered.commits, recovered.index,
            recovered.bodies).truncateAfter(10);
        assertEquals(3, recovered.commits.size());
      }
    }
  }

  @Test
  public void rejectsUnknownTargetBeforeShrinkingAnyStore() throws Exception {
    Path archive = temporaryFolder.newFolder("unknown-target").toPath();
    initialize(archive);
    try (Stores stores = new Stores(archive)) {
      assertThrows(ArchivePersistenceException.class,
          () -> new ArchiveHistoryTruncator(stores.commits, stores.index,
              stores.bodies).truncateAfter(7));
      assertEquals(12, commitHead(stores));
      assertEquals(12, indexHead(stores));
      assertEquals(12, bodyHead(stores));
    }
  }

  private static void initialize(Path archive) throws Exception {
    try (HistorySegmentStore bodies = new HistorySegmentStore(
        archive, new BlockHistoryCodec(), 4096);
        HistoryIndexStore index = new HistoryIndexStore(archive, new HistoryIndexCodec());
        HistoryCommitStore commits = new HistoryCommitStore(
            archive, new HistoryCommitMarkerCodec())) {
      List<HistoryCommitMarker> markers = new ArrayList<>();
      for (long epoch = 8; epoch <= 12; epoch++) {
        BlockReverseDiff diff = diff(epoch);
        HistoryLocation body = bodies.append(diff);
        HistoryIndexLocation indexLocation = index.append(HistoryIndexRecord.from(diff, body));
        markers.add(new HistoryCommitMarker(diff.getMeta(), epoch - 1, body, indexLocation,
            bytes(16, (int) epoch + 40), PARTICIPANTS));
      }
      bodies.sync();
      index.sync();
      commits.commitAll(markers);
    }
  }

  private static long commitHead(Stores stores) {
    return stores.commits.head().getMeta().getEpoch();
  }

  private static long indexHead(Stores stores) {
    return stores.index.getScanResult().getHead().getRecord().getMeta().getEpoch();
  }

  private static long bodyHead(Stores stores) {
    return stores.bodies.getScanResult().getHead().getDiff().getMeta().getEpoch();
  }

  private static BlockReverseDiff diff(long epoch) {
    return new BlockReverseDiff(new BlockSnapshotMeta(epoch, epoch,
        bytes(32, (int) epoch), bytes(32, (int) epoch - 1), epoch * 1_000),
        Collections.singletonList(new DbGroup("account", Collections.singletonList(
            new Entry(bytes(8, (int) epoch), OldValue.present(bytes(12, (int) epoch)))))));
  }

  private static byte[] bytes(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }

  private static final class Stores implements AutoCloseable {
    private final HistorySegmentStore bodies;
    private final HistoryIndexStore index;
    private final HistoryCommitStore commits;

    private Stores(Path archive) throws IOException {
      bodies = new HistorySegmentStore(archive, new BlockHistoryCodec(), 4096);
      index = new HistoryIndexStore(archive, new HistoryIndexCodec());
      commits = new HistoryCommitStore(archive, new HistoryCommitMarkerCodec());
    }

    @Override
    public void close() throws IOException {
      commits.close();
      index.close();
      bodies.close();
    }
  }
}
