package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.StateArchiveFiveLaneBlockCodecV3.EncodedBundle;

public class StateArchiveHistoryCatalogV3Test {

  private static final int EXPECTED_RETAINED_GENERATIONS = 3;
  // Frozen before terminal-aware encoding; do not regenerate this fixture with the new encoder.
  private static final String LEGACY_GENERATION_BASE64 =
      "U0NHMwADAAAAAAEAAAAAAAAAAAAAAANgAAAAAAAAAAAAAAAAAAAF3AAAAAUAAAAAAAAAcAAAAMAA"
          + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMVGTTwcOy7i1WItq9gxadshQKwF1/6ZWGa9"
          + "KUUkuPakmp5Gxbf+qIomK8fk5SWCsMDUK9bl5FQAHB1XG3cHbEpv2B8Z9Lk6nogBrFI5W7lAoaV6"
          + "PtKLkQPnv7BntMw0GgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
          + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
          + "AAAAAAAAAAAAAgEAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAdRIr9ko"
          + "BlRYz2cLYPWllNc1rwFyyNZ/IqgWgBMmgcoABAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
          + "AAAAAAAAAAIBAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABG7pzYr7swzg"
          + "DvTO7JU2M2y96Hz/6rOaKDvDKhnU7Cr6AAUAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
          + "AAAAAAACAQAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAVJv671RpfnzIDq"
          + "poKzYqvpVdl/wi2x7x/7litFrZogLQANAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
          + "AAAAAgEAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAANkxZv9a1fmvR77tBS"
          + "TXYoSYlnzP9A7EE+Tiaf0LbzSqEAFgACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
          + "AAIBAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFvS9gRWK0A14bJyh+pzM"
          + "m/sFXN7EKrQKGLGu3Q0ftE631OO7dmzvELp8sGjxeCepVobRsSgZ5/MB/w6+/h5oFpIAAAAAAAAD"
          + "YHt98QUzR0NT";
  private static final String LEGACY_CURRENT_BASE64 =
      "U0NVMwADAAAAAAAAAAAAANTju3Zs7xC6fLBo8XgnqVaG0bEoGefzAf8Ovv4eaBaSAAAAAAAAAAAA"
          + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEsBUnjNVQ1MAAAAA";

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void boundsRetainedGenerationFiles() throws Exception {
    Path root = temporaryFolder.newFolder("catalog-retention").toPath();
    publishEmptyGenerations(root, 5);

    assertEquals(EXPECTED_RETAINED_GENERATIONS, generationFileCount(root));
  }

  @Test
  public void retainsSelectedAndImmediatelyPreviousGenerations() throws Exception {
    Path root = temporaryFolder.newFolder("catalog-current-and-previous").toPath();
    StateArchiveHistoryCatalogV3 catalog = publishEmptyGenerations(root, 5);

    assertEquals(4, catalog.selected().getGeneration());
    assertTrue(Files.exists(generationPath(root, 4)));
    assertTrue(Files.exists(generationPath(root, 3)));
    assertFalse(Files.exists(generationPath(root, 1)));
  }

  @Test
  public void reopensWithRetainedCatalogAndValidatesSegmentSelection() throws Exception {
    Path root = temporaryFolder.newFolder("catalog-reopen").toPath();
    byte[] baseline = hash(90);
    StateArchiveFiveLaneBlockCodecV3 codec = new StateArchiveFiveLaneBlockCodecV3();
    byte[] previous = baseline;
    try (StateArchiveFiveLaneSegmentWriterV3 writer =
        new StateArchiveFiveLaneSegmentWriterV3(root, baseline,
            StateArchiveFileFormatV3.COMPRESSION_NONE, 1_500)) {
      for (int block = 1; block <= 5; block++) {
        EncodedBundle bundle = codec.encode(diff(block, block == 1 ? 0 : block - 1, 1_400),
            previous, StateArchiveFileFormatV3.COMPRESSION_NONE);
        writer.append(bundle);
        BlockSnapshotMeta meta = bundle.getDiff().getMeta();
        StateArchiveFiveLaneSegmentWriterV3.ArchiveDurabilityProof proof = writer.sync(block,
            new StateArchiveFiveLaneRecoveryIntentV3.RecoveryPoint(meta.getEpoch(),
                meta.getBlockNumber(), meta.getTimestamp(), meta.getBlockHash(),
                meta.getParentHash(), bundle.getResultHistoryDigest()), hash(100 + block));
        StateArchiveFiveLaneDurabilityProofV3.publish(root, proof);
        previous = bundle.getResultHistoryDigest();
      }
    }
    assertEquals(EXPECTED_RETAINED_GENERATIONS, generationFileCount(root));

    try (StateArchiveFiveLaneSegmentWriterV3 reopened =
        new StateArchiveFiveLaneSegmentWriterV3(root, baseline,
            StateArchiveFileFormatV3.COMPRESSION_NONE, 1_500)) {
      assertEquals(5, reopened.getAppendHead().getBlockNumber());
      assertEquals(5, reopened.readCommittedDiffs(0, 5).size());
    }
  }

  @Test
  public void legacyGenerationDecodesAsFiveCurrentTerminals() throws Exception {
    Path root = temporaryFolder.newFolder("catalog-legacy-terminals").toPath();
    StateArchiveHistoryCatalogV3 catalog = StateArchiveHistoryCatalogV3.openOrEmpty(root);
    List<StateArchiveSegmentFormatV3.CurrentSegment> current = currentSegments();
    catalog.publish(1_500, current, Collections.emptyList());
    byte[] encoded = Files.readAllBytes(generationPath(root, 0));
    assertEquals(0, ByteBuffer.wrap(encoded).getInt(16));

    StateArchiveHistoryCatalogV3 reopened = StateArchiveHistoryCatalogV3.openOrEmpty(root);
    assertEquals(5, reopened.selected().getTerminals().size());
    assertTrue(reopened.selected().getTerminals().stream()
        .allMatch(terminal -> terminal.getKind()
            == StateArchiveHistoryCatalogV3.TerminalKind.CURRENT));
    assertTrue(reopened.selected().sameSnapshot(catalog.selected()));
  }

  @Test
  public void opensFrozenPreChangeLegacyCatalogBytes() throws Exception {
    Path root = temporaryFolder.newFolder("catalog-frozen-legacy").toPath();
    Path catalogRoot = root.resolve(StateArchiveHistoryCatalogV3.DIRECTORY);
    Files.createDirectories(generationsPath(root));
    Files.write(generationPath(root, 0), java.util.Base64.getDecoder().decode(
        LEGACY_GENERATION_BASE64));
    Files.write(catalogRoot.resolve(StateArchiveHistoryCatalogV3.CURRENT),
        java.util.Base64.getDecoder().decode(LEGACY_CURRENT_BASE64));

    StateArchiveHistoryCatalogV3 catalog = StateArchiveHistoryCatalogV3.openOrEmpty(root);
    assertEquals(0, catalog.selected().getGeneration());
    assertEquals(5, catalog.selected().getTerminals().size());
    assertTrue(catalog.selected().getTerminals().stream()
        .allMatch(terminal -> terminal.getKind()
            == StateArchiveHistoryCatalogV3.TerminalKind.CURRENT));
  }

  @Test
  public void terminalAwareCatalogRoundTripsMixedCurrentAndSealedTerminals() throws Exception {
    Path root = temporaryFolder.newFolder("catalog-mixed-terminals").toPath();
    List<StateArchiveSegmentFormatV3.CurrentSegment> current = currentSegments();
    StateArchiveSegmentFormatV3.SealedSegment sealed = sealedSegment(0, 0);
    current.remove(0);
    List<StateArchiveHistoryCatalogV3.Terminal> terminals = new ArrayList<>();
    terminals.add(StateArchiveHistoryCatalogV3.Terminal.sealed(sealed));
    for (StateArchiveSegmentFormatV3.CurrentSegment segment : current) {
      terminals.add(StateArchiveHistoryCatalogV3.Terminal.current(segment));
    }
    StateArchiveHistoryCatalogV3 catalog = StateArchiveHistoryCatalogV3.openOrEmpty(root);
    catalog.publish(1_500, current, Collections.singletonList(sealed), terminals);

    StateArchiveHistoryCatalogV3 reopened = StateArchiveHistoryCatalogV3.openOrEmpty(root);
    assertEquals(StateArchiveHistoryCatalogV3.TerminalKind.SEALED,
        reopened.selected().terminalForLane(0).getKind());
    assertEquals(StateArchiveHistoryCatalogV3.TerminalKind.CURRENT,
        reopened.selected().terminalForLane(22).getKind());
    assertEquals(4, reopened.selected().getCurrent().size());
    assertEquals(5, reopened.selected().getTerminals().size());
    assertTrue(reopened.selected().sameSnapshot(catalog.selected()));
  }

  @Test
  public void rejectsUnknownFlagsAndNonFinalSealedTerminal() throws Exception {
    Path root = temporaryFolder.newFolder("catalog-terminal-validation").toPath();
    StateArchiveHistoryCatalogV3 catalog = StateArchiveHistoryCatalogV3.openOrEmpty(root);
    List<StateArchiveSegmentFormatV3.CurrentSegment> current = currentSegments();
    List<StateArchiveSegmentFormatV3.SealedSegment> sealed = Arrays.asList(
        sealedSegment(0, 0), sealedSegment(0, 1));
    current.remove(0);
    List<StateArchiveHistoryCatalogV3.Terminal> terminals = new ArrayList<>();
    terminals.add(StateArchiveHistoryCatalogV3.Terminal.sealed(sealed.get(0)));
    for (StateArchiveSegmentFormatV3.CurrentSegment segment : current) {
      terminals.add(StateArchiveHistoryCatalogV3.Terminal.current(segment));
    }
    assertThrows(IllegalArgumentException.class,
        () -> catalog.publish(1_500, current, sealed, terminals));

    catalog.publish(1_500, currentSegments(), Collections.emptyList());
    Path generation = generationPath(root, 0);
    byte[] encoded = Files.readAllBytes(generation);
    ByteBuffer.wrap(encoded).putInt(16, 1 << 8);
    Files.write(generation, encoded);
    assertThrows(IOException.class, () -> StateArchiveHistoryCatalogV3.openOrEmpty(root));
  }

  @Test
  public void preparedPublicationCasReusesExactOrphanAndRejectsDifferentOrphan()
      throws Exception {
    Path root = temporaryFolder.newFolder("catalog-prepared-publication").toPath();
    StateArchiveHistoryCatalogV3 catalog = StateArchiveHistoryCatalogV3.openOrEmpty(root);
    StateArchiveHistoryCatalogV3.PreparedGeneration first = catalog.prepare(1_500,
        Collections.emptyList(), Collections.emptyList());
    Files.createDirectories(generationsPath(root));
    Files.write(generationPath(root, 0), first.getEncoded());
    catalog.publishPrepared(null, first);
    assertEquals(0, catalog.selected().getGeneration());

    StateArchiveHistoryCatalogV3.PreparedGeneration expected = catalog.prepare(1_500,
        Collections.emptyList(), Collections.emptyList());
    StateArchiveHistoryCatalogV3.PreparedGeneration different = catalog.prepare(1_500,
        currentSegments(), Collections.emptyList());
    Files.write(generationPath(root, 1), different.getEncoded());
    assertThrows(IOException.class, () -> catalog.publishPrepared(catalog.selected(), expected));
    Files.delete(generationPath(root, 1));

    StateArchiveHistoryCatalogV3.PreparedGeneration stale = catalog.prepare(1_500,
        Collections.emptyList(), Collections.emptyList());
    StateArchiveHistoryCatalogV3.PreparedGeneration next = catalog.prepare(1_500,
        currentSegments(), Collections.emptyList());
    catalog.publishPrepared(catalog.selected(), next);
    assertThrows(IllegalArgumentException.class,
        () -> catalog.publishPrepared(stale.getSource(), stale));
  }

  @Test
  public void exactOrphanSyncsGenerationsBeforeCurrentCommit() throws Exception {
    Path root = temporaryFolder.newFolder("catalog-orphan-sync-order").toPath();
    Path catalogRoot = root.resolve(StateArchiveHistoryCatalogV3.DIRECTORY);
    Path current = catalogRoot.resolve(StateArchiveHistoryCatalogV3.CURRENT);
    List<Path> syncOrder = new ArrayList<>();
    StateArchiveHistoryCatalogV3.DirectorySync sync = directory -> {
      syncOrder.add(directory);
      if (directory.equals(generationsPath(root)) && syncOrder.size() == 1) {
        assertFalse(Files.exists(current));
      }
      if (directory.equals(catalogRoot)) {
        assertTrue(Files.exists(current));
      }
    };
    StateArchiveHistoryCatalogV3 catalog = StateArchiveHistoryCatalogV3.openOrEmpty(root, sync);
    StateArchiveHistoryCatalogV3.PreparedGeneration prepared = catalog.prepare(1_500,
        Collections.emptyList(), Collections.emptyList());
    Files.createDirectories(generationsPath(root));
    Files.write(generationPath(root, 0), prepared.getEncoded());

    catalog.publishPrepared(null, prepared);

    assertEquals(3, syncOrder.size());
    assertEquals(generationsPath(root), syncOrder.get(0));
    assertEquals(catalogRoot, syncOrder.get(1));
  }

  @Test
  public void retentionBoundaryKeepsExactlyThreeAfterGenerationThree() throws Exception {
    Path root = temporaryFolder.newFolder("catalog-retention-boundary").toPath();
    StateArchiveHistoryCatalogV3 catalog = StateArchiveHistoryCatalogV3.openOrEmpty(root);
    for (int index = 0; index < 4; index++) {
      StateArchiveHistoryCatalogV3.PreparedGeneration prepared = catalog.prepare(1_500,
          Collections.emptyList(), Collections.emptyList());
      catalog.publishPrepared(catalog.isPublished() ? catalog.selected() : null, prepared);
    }
    assertEquals(3, generationFileCount(root));
    assertFalse(Files.exists(generationPath(root, 0)));
    assertTrue(Files.exists(generationPath(root, 1)));
    assertTrue(Files.exists(generationPath(root, 2)));
    assertTrue(Files.exists(generationPath(root, 3)));
  }

  @Test
  public void rejectsUnequalTerminalHeadsAndNonBijectiveCurrentProjection() throws Exception {
    Path root = temporaryFolder.newFolder("catalog-terminal-invariants").toPath();
    StateArchiveHistoryCatalogV3 catalog = StateArchiveHistoryCatalogV3.openOrEmpty(root);
    List<StateArchiveSegmentFormatV3.CurrentSegment> unequal = currentSegments();
    unequal.set(0, new StateArchiveSegmentFormatV3.CurrentSegment(0, 0, 0, 1, 900, 2,
        hash(0)));
    List<StateArchiveHistoryCatalogV3.Terminal> terminals = new ArrayList<>();
    for (StateArchiveSegmentFormatV3.CurrentSegment segment : unequal) {
      terminals.add(StateArchiveHistoryCatalogV3.Terminal.current(segment));
    }
    final List<StateArchiveHistoryCatalogV3.Terminal> unequalTerminals = terminals;
    assertThrows(IllegalArgumentException.class,
        () -> catalog.publish(1_500, unequal, Collections.emptyList(), unequalTerminals));

    List<StateArchiveSegmentFormatV3.CurrentSegment> current = currentSegments();
    terminals = new ArrayList<>();
    for (StateArchiveSegmentFormatV3.CurrentSegment segment : current) {
      if (segment.getLaneId() != 22) {
        terminals.add(StateArchiveHistoryCatalogV3.Terminal.current(segment));
      }
    }
    final List<StateArchiveHistoryCatalogV3.Terminal> missingTerminals = terminals;
    assertThrows(IllegalArgumentException.class,
        () -> catalog.publish(1_500, current, Collections.emptyList(), missingTerminals));
  }

  private static StateArchiveHistoryCatalogV3 publishEmptyGenerations(Path root, int count)
      throws Exception {
    StateArchiveHistoryCatalogV3 catalog = StateArchiveHistoryCatalogV3.openOrEmpty(root);
    for (int generation = 0; generation < count; generation++) {
      catalog.publish(1_500, Collections.emptyList(), Collections.emptyList());
    }
    return catalog;
  }

  private static long generationFileCount(Path root) throws Exception {
    try (Stream<Path> files = Files.list(generationsPath(root))) {
      return files.filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().startsWith("catalog-"))
          .filter(path -> path.getFileName().toString().endsWith(".bin"))
          .count();
    }
  }

  private static Path generationPath(Path root, long generation) {
    return generationsPath(root).resolve(String.format("catalog-%020d.bin", generation));
  }

  private static Path generationsPath(Path root) {
    return root.resolve(StateArchiveHistoryCatalogV3.DIRECTORY).resolve("generations");
  }

  private static BlockReverseDiff diff(int blockNumber, int parent, int valueLength) {
    byte[] value = new byte[valueLength];
    java.util.Arrays.fill(value, (byte) blockNumber);
    return new BlockReverseDiff(new BlockSnapshotMeta(blockNumber, blockNumber,
        hash(blockNumber), hash(parent), blockNumber * 3_000L),
        Collections.singletonList(new DbGroup(StateArchiveFileFormatV3.dbName(1),
            Collections.singletonList(new Entry(new byte[]{1}, OldValue.present(value))))));
  }

  private static List<StateArchiveSegmentFormatV3.CurrentSegment> currentSegments() {
    List<StateArchiveSegmentFormatV3.CurrentSegment> result = new ArrayList<>();
    for (int laneId : StateArchiveFileFormatV3.fiveLaneIds()) {
      result.add(new StateArchiveSegmentFormatV3.CurrentSegment(laneId, 0, 0, 0,
          StateArchiveFileFormatV3.PART_HEADER_LENGTH + 1, 1, hash(laneId)));
    }
    return result;
  }

  private static StateArchiveSegmentFormatV3.SealedSegment sealedSegment(int laneId,
      long sequence) {
    return new StateArchiveSegmentFormatV3.SealedSegment(laneId, sequence, sequence, sequence, 1,
        StateArchiveFileFormatV3.PART_HEADER_LENGTH
            + StateArchiveFileFormatV3.SEAL_HEADER_LENGTH
            + StateArchiveFileFormatV3.FRAME_TRAILER_LENGTH,
        StateArchiveFileFormatV3.BLOCK_INDEX_HEADER_LENGTH, hash(laneId + 30),
        hash(laneId + 40), hash(laneId + 50), hash(laneId + 60));
  }

  private static byte[] hash(int suffix) {
    byte[] result = new byte[32];
    result[31] = (byte) suffix;
    return result;
  }
}
