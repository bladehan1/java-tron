package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CommittedHistoryAuthorityTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void writerAndStoreExposeTheSameReadOnlyCommittedAuthority() throws Exception {
    Path archive = temporaryFolder.newFolder("committed-authority").toPath();
    Path reader = archive.resolve("progress/reader.progress");
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 1_000L);

    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(
        archive, 4096, ArchiveStoreScope.getStateDatabases())) {
      writer.accept(new BlockReverseDiff(meta, Collections.emptyList()));
      assertAuthority(writer, meta);

      new ArchiveReaderHeadPublisher(writer, reader, participants()).publish(1);
      ArchiveProgressEnvelope published = new ArchiveProgressFile(reader,
          new ArchiveProgressEnvelopeCodec()).load();
      assertEquals(1L, published.getEpoch());
      assertArrayEquals(meta.getBlockHash(), published.getBlockHash());
    }

    try (HistoryCommitStore store = new HistoryCommitStore(
        archive, new HistoryCommitMarkerCodec())) {
      assertAuthority(store, meta);
    }
  }

  private static void assertAuthority(CommittedHistoryAuthority authority,
      BlockSnapshotMeta expected) {
    assertEquals(1L, authority.firstEpoch());
    assertNotNull(authority.head());
    assertEquals(expected, authority.head().getMeta());
    assertEquals(expected, authority.get(1).getMeta());
    HistoryCoverage coverage = authority.coverage();
    assertNotNull(coverage);
    assertEquals(1L, coverage.getFirstEpoch());
    assertEquals(1L, coverage.getRecordCount());
    assertEquals(1L, coverage.getHeadEpoch());
    assertArrayEquals(expected.getBlockHash(), coverage.getHeadHash());
  }

  private static List<String> participants() {
    List<String> participants = new ArrayList<>(ArchiveStoreScope.getStateDatabases());
    Collections.sort(participants);
    return participants;
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }
}
