package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.StateArchiveFiveLaneBlockCodecV3.EncodedBundle;

public class StateArchiveHistoryCatalogV3Test {

  private static final int EXPECTED_RETAINED_GENERATIONS = 3;

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

  private static byte[] hash(int suffix) {
    byte[] result = new byte[32];
    result[31] = (byte) suffix;
    return result;
  }
}
