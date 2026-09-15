package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.StateArchiveFiveLaneBlockCodecV3.EncodedBundle;
import org.tron.core.db2.archive.StateArchiveFiveLaneRecoveryIntentV3.RecoveryPoint;
import org.tron.core.db2.archive.StateArchiveFiveLaneSegmentWriterV3.ArchiveDurabilityProof;
import org.tron.core.db2.archive.StateArchiveFiveLaneSegmentWriterV3.FileTailProof;
import org.tron.core.db2.archive.StateArchiveSegmentFormatV3.CurrentSegment;
import org.tron.core.db2.archive.StateArchiveSegmentFormatV3.SealedSegment;

/**
 * The proof-boundary fast reopen must rebuild the exact identity the retained history scan
 * rebuilds, must fail closed when the persisted boundary is missing or drifting, and must not
 * read sealed segment data beyond each segment's terminal seal frame.
 */
public class StateArchiveFastReopenV3Test {

  private static final long ROTATION = 6_000;
  private static final int BLOCKS = 39;
  private static final int CHECKPOINT_EVERY = 6;
  private static final int LAST_CHECKPOINT = 36;
  private static final byte[] BASELINE = hash(90);
  private static final long SEAL_FRAME_LENGTH = StateArchiveFileFormatV3.SEAL_HEADER_LENGTH
      + StateArchiveFileFormatV3.FRAME_TRAILER_LENGTH;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void fastReopenRebuildsTheScanPathIdentityFieldByField() throws Exception {
    Path root = temporaryFolder.newFolder("fast-reopen-equivalence").toPath();
    Path stash = temporaryFolder.newFolder("fast-reopen-equivalence-stash").toPath();
    Map<Integer, RecoveryPoint> points = buildLibrary(root, stash, BLOCKS);
    Identity fast = identity(root, points.get(BLOCKS), false);
    assertEquals(fast.sealedRecords.size() * SEAL_FRAME_LENGTH, fast.sealedDataReadBytes);
    Identity scanned = identity(root, points.get(BLOCKS), true);
    assertSameIdentity(scanned, fast);
  }

  @Test
  public void sealAfterFastReopenRecomputesScanIdenticalStatistics() throws Exception {
    Path root = temporaryFolder.newFolder("fast-reopen-seal").toPath();
    Path stash = temporaryFolder.newFolder("fast-reopen-seal-stash").toPath();
    Map<Integer, RecoveryPoint> points = buildLibrary(root, stash, BLOCKS);
    StateArchiveFiveLaneBlockCodecV3 codec = new StateArchiveFiveLaneBlockCodecV3();
    int sealedBefore;
    try (StateArchiveFiveLaneSegmentWriterV3 writer =
        new StateArchiveFiveLaneSegmentWriterV3(root, BASELINE,
            StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION)) {
      sealedBefore = writer.getSealedSegments().size();
      byte[] previous = writer.getResultHistoryDigest();
      for (int block = BLOCKS + 1; block <= BLOCKS + 12; block++) {
        // Oversized values rotate lane 0 quickly, sealing fast-reopened segments.
        EncodedBundle bundle = codec.encode(diff(block, 1_500), previous,
            StateArchiveFileFormatV3.COMPRESSION_NONE);
        long checkpoint = block <= BLOCKS + 3 ? BLOCKS + 3
            : block <= BLOCKS + 9 ? BLOCKS + 9 : BLOCKS + 12;
        writer.appendForCheckpoint(bundle, checkpoint, hash(50 + (int) checkpoint));
        previous = bundle.getResultHistoryDigest();
        points.put(block, point(bundle));
        if (block == checkpoint) {
          StateArchiveFiveLaneDurabilityProofV3.publish(root,
              writer.sync(checkpoint, point(bundle), hash(50 + (int) checkpoint)));
        }
      }
      assertNotNull(writer.getLastDurabilityProof());
    }
    Identity fast = identity(root, points.get(BLOCKS + 12), false);
    Identity scanned = identity(root, points.get(BLOCKS + 12), true);
    assertSameIdentity(scanned, fast);
    org.junit.Assert.assertTrue("the appended range must seal fast-reopened segments",
        fast.sealedRecords.size() > sealedBefore);
  }

  @Test
  public void secondFastReopenIsZeroAction() throws Exception {
    Path root = temporaryFolder.newFolder("fast-reopen-zero-action").toPath();
    Path stash = temporaryFolder.newFolder("fast-reopen-zero-action-stash").toPath();
    buildLibrary(root, stash, BLOCKS);
    Map<String, Long> before = fileSizes(root);
    Identity fast = identity(root, null, false);
    assertEquals(BLOCKS, fast.head.getBlockNumber());
    assertEquals(before, fileSizes(root));
  }

  @Test
  public void missingProofFailsClosed() throws Exception {
    Path root = temporaryFolder.newFolder("fast-reopen-missing-proof").toPath();
    Path stash = temporaryFolder.newFolder("fast-reopen-missing-proof-stash").toPath();
    buildLibrary(root, stash, BLOCKS);
    Files.delete(root.resolve(StateArchiveFiveLaneDurabilityProofV3.FILE_NAME));
    assertThrows(IOException.class, () -> new StateArchiveFiveLaneSegmentWriterV3(root,
        BASELINE, StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION));
  }

  @Test
  public void staleProofFailsClosed() throws Exception {
    Path root = temporaryFolder.newFolder("fast-reopen-stale-proof").toPath();
    Path stash = temporaryFolder.newFolder("fast-reopen-stale-proof-stash").toPath();
    buildLibrary(root, stash, BLOCKS);
    // A superseded proof points at older segments than the Catalog current boundary.
    Files.write(root.resolve(StateArchiveFiveLaneDurabilityProofV3.FILE_NAME),
        Files.readAllBytes(stash.resolve("proof-" + LAST_CHECKPOINT / 2)));
    assertThrows(IOException.class, () -> new StateArchiveFiveLaneSegmentWriterV3(root,
        BASELINE, StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION));
  }

  @Test
  public void sealedSegmentDriftFailsClosed() throws Exception {
    Path truncated = temporaryFolder.newFolder("fast-reopen-seal-truncated").toPath();
    buildLibrary(truncated, temporaryFolder.newFolder("stash-a").toPath(), BLOCKS);
    Path sealed = sealedDataFile(truncated);
    try (FileChannel channel = FileChannel.open(sealed, StandardOpenOption.WRITE)) {
      channel.truncate(channel.size() - 8);
      channel.force(false);
    }
    assertThrows(IOException.class, () -> new StateArchiveFiveLaneSegmentWriterV3(truncated,
        BASELINE, StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION));

    Path extended = temporaryFolder.newFolder("fast-reopen-seal-extended").toPath();
    buildLibrary(extended, temporaryFolder.newFolder("stash-b").toPath(), BLOCKS);
    try (FileChannel channel = FileChannel.open(sealedDataFile(extended),
        StandardOpenOption.WRITE)) {
      channel.position(channel.size());
      channel.write(ByteBuffer.allocate(16));
      channel.force(false);
    }
    assertThrows(IOException.class, () -> new StateArchiveFiveLaneSegmentWriterV3(extended,
        BASELINE, StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION));

    Path corrupted = temporaryFolder.newFolder("fast-reopen-seal-corrupt").toPath();
    buildLibrary(corrupted, temporaryFolder.newFolder("stash-c").toPath(), BLOCKS);
    Path sealedData = sealedDataFile(corrupted);
    try (FileChannel channel = FileChannel.open(sealedData, StandardOpenOption.READ,
        StandardOpenOption.WRITE)) {
      ByteBuffer one = ByteBuffer.allocate(1);
      channel.position(channel.size() - SEAL_FRAME_LENGTH + 40);
      channel.read(one);
      one.flip();
      one.put(0, (byte) (one.get(0) ^ 1));
      channel.position(channel.size() - SEAL_FRAME_LENGTH + 40);
      channel.write(one);
      channel.force(false);
    }
    assertThrows(IOException.class, () -> new StateArchiveFiveLaneSegmentWriterV3(corrupted,
        BASELINE, StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION));
  }

  @Test
  public void markerDigestDriftFailsClosed() throws Exception {
    Path root = temporaryFolder.newFolder("fast-reopen-marker-drift").toPath();
    Path stash = temporaryFolder.newFolder("fast-reopen-marker-drift-stash").toPath();
    buildLibrary(root, stash, BLOCKS);
    ArchiveDurabilityProof proof = StateArchiveFiveLaneDurabilityProofV3.decode(
        Files.readAllBytes(root.resolve(StateArchiveFiveLaneDurabilityProofV3.FILE_NAME)));
    FileTailProof tail = proof.getFileTails().get(0);
    Path data = segmentData(root, tail.getLaneId(), tail.getSegmentSeq());
    try (FileChannel channel = FileChannel.open(data, StandardOpenOption.READ,
        StandardOpenOption.WRITE)) {
      ByteBuffer one = ByteBuffer.allocate(1);
      channel.position(tail.getMarkerOffset() + 20);
      channel.read(one);
      one.flip();
      one.put(0, (byte) (one.get(0) ^ 1));
      channel.position(tail.getMarkerOffset() + 20);
      channel.write(one);
      channel.force(false);
    }
    assertThrows(IOException.class, () -> new StateArchiveFiveLaneSegmentWriterV3(root,
        BASELINE, StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION));
  }

  @Test
  public void tornUncheckpointedTailFailsClosedThenRecoveryRepairs() throws Exception {
    Path root = temporaryFolder.newFolder("fast-reopen-torn-tail").toPath();
    Path stash = temporaryFolder.newFolder("fast-reopen-torn-tail-stash").toPath();
    Map<Integer, RecoveryPoint> points = buildLibrary(root, stash, BLOCKS);
    Path lane0Current;
    try (StateArchiveFiveLaneSegmentWriterV3 writer =
        new StateArchiveFiveLaneSegmentWriterV3(root, BASELINE,
            StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION)) {
      long sequence = writer.getCurrentSegments().stream()
          .filter(segment -> segment.getLaneId() == 0).findFirst().get().getSegmentSeq();
      lane0Current = segmentData(root, 0, sequence);
    }
    try (FileChannel channel = FileChannel.open(lane0Current, StandardOpenOption.WRITE)) {
      channel.truncate(channel.size() - 10);
      channel.force(false);
    }
    assertThrows(IOException.class, () -> new StateArchiveFiveLaneSegmentWriterV3(root,
        BASELINE, StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION));
    // Repair to the last fully intact block (one past the checkpoint), then the fast reopen
    // resumes from the persisted proof boundary and scans only the retained tail.
    RecoveryPoint authorized = points.get(BLOCKS - 1);
    RecoveryPoint common = points.get(LAST_CHECKPOINT);
    try (StateArchiveFiveLaneSegmentWriterV3 recovered =
        StateArchiveFiveLaneSegmentWriterV3.recover(root, BASELINE,
            StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION, authorized, common,
            StateArchiveFiveLaneSegmentWriterV3.RecoveryFaultHook.NONE)) {
      assertEquals(BLOCKS - 1, recovered.getAppendHead().getBlockNumber());
    }
    Identity fast = identity(root, authorized, false);
    assertEquals(BLOCKS - 1, fast.head.getBlockNumber());
    assertEquals(LAST_CHECKPOINT, fast.proof.getCheckpointSequence());
    assertEquals(fast.sealedRecords.size() * SEAL_FRAME_LENGTH, fast.sealedDataReadBytes);
  }

  private interface WriterFactory {
    StateArchiveFiveLaneSegmentWriterV3 open() throws Exception;
  }

  private static final class Identity {
    private final BlockSnapshotMeta head;
    private final byte[] resultHistoryDigest;
    private final List<CurrentSegment> current;
    private final List<String> sealedRecords;
    private final ArchiveDurabilityProof proof;
    private final long sealedDataReadBytes;

    private Identity(BlockSnapshotMeta head, byte[] resultHistoryDigest,
        List<CurrentSegment> current, List<String> sealedRecords, ArchiveDurabilityProof proof,
        long sealedDataReadBytes) {
      this.head = head;
      this.resultHistoryDigest = resultHistoryDigest;
      this.current = current;
      this.sealedRecords = sealedRecords;
      this.proof = proof;
      this.sealedDataReadBytes = sealedDataReadBytes;
    }
  }

  private static Identity identity(Path root, RecoveryPoint scanBoundary, boolean scan)
      throws Exception {
    WriterFactory factory = scan
        ? () -> StateArchiveFiveLaneSegmentWriterV3.recover(root, BASELINE,
            StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION, scanBoundary, scanBoundary,
            StateArchiveFiveLaneSegmentWriterV3.RecoveryFaultHook.NONE)
        : () -> new StateArchiveFiveLaneSegmentWriterV3(root, BASELINE,
            StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION);
    try (StateArchiveFiveLaneSegmentWriterV3 writer = factory.open()) {
      BlockSnapshotMeta head = writer.getAppendHead();
      assertNotNull(head);
      List<String> sealed = new ArrayList<>();
      for (SealedSegment segment : writer.getSealedSegments()) {
        sealed.add(java.util.Base64.getEncoder().encodeToString(
            StateArchiveSegmentFormatV3.encodeSealedMapRecord(segment)));
      }
      return new Identity(head, writer.getResultHistoryDigest(), writer.getCurrentSegments(),
          sealed, writer.getLastDurabilityProof(), writer.getReopenSealedDataReadBytes());
    }
  }

  private static void assertSameIdentity(Identity expected, Identity actual) {
    assertEquals(expected.head, actual.head);
    assertArrayEquals(expected.resultHistoryDigest, actual.resultHistoryDigest);
    assertEquals(expected.sealedRecords, actual.sealedRecords);
    assertEquals(expected.current.size(), actual.current.size());
    for (int index = 0; index < expected.current.size(); index++) {
      CurrentSegment left = expected.current.get(index);
      CurrentSegment right = actual.current.get(index);
      assertEquals(left.getLaneId(), right.getLaneId());
      assertEquals(left.getSegmentSeq(), right.getSegmentSeq());
      assertEquals(left.getFirstBlock(), right.getFirstBlock());
      assertEquals(left.getCurrentLastBlock(), right.getCurrentLastBlock());
      assertEquals(left.getDataEndOffset(), right.getDataEndOffset());
      assertEquals(left.getBlockFrameCount(), right.getBlockFrameCount());
      assertArrayEquals(left.getHeaderDigest(), right.getHeaderDigest());
    }
    assertNotNull(expected.proof);
    assertNotNull(actual.proof);
    assertEquals(expected.proof.getCheckpointSequence(), actual.proof.getCheckpointSequence());
    assertSamePoint(expected.proof.getTarget(), actual.proof.getTarget());
    assertArrayEquals(expected.proof.getCommonTargetDigest(),
        actual.proof.getCommonTargetDigest());
    assertEquals(expected.proof.getFileTails().size(), actual.proof.getFileTails().size());
    for (int index = 0; index < expected.proof.getFileTails().size(); index++) {
      FileTailProof left = expected.proof.getFileTails().get(index);
      FileTailProof right = actual.proof.getFileTails().get(index);
      assertEquals(left.getLaneId(), right.getLaneId());
      assertEquals(left.getSegmentSeq(), right.getSegmentSeq());
      assertEquals(left.getMarkerOffset(), right.getMarkerOffset());
      assertEquals(left.getMarkerLength(), right.getMarkerLength());
      assertEquals(left.getMarkerEndOffset(), right.getMarkerEndOffset());
      assertArrayEquals(left.getMarkerDigest(), right.getMarkerDigest());
    }
  }

  private static void assertSamePoint(RecoveryPoint expected, RecoveryPoint actual) {
    assertEquals(expected.getEpoch(), actual.getEpoch());
    assertEquals(expected.getBlockNumber(), actual.getBlockNumber());
    assertEquals(expected.getTimestamp(), actual.getTimestamp());
    assertArrayEquals(expected.getBlockHash(), actual.getBlockHash());
    assertArrayEquals(expected.getParentHash(), actual.getParentHash());
    assertArrayEquals(expected.getResultHistoryDigest(), actual.getResultHistoryDigest());
  }

  /** Builds a library with several rotations, several checkpoints and a 3-block open tail. */
  private static Map<Integer, RecoveryPoint> buildLibrary(Path root, Path stash, int blocks)
      throws Exception {
    Map<Integer, RecoveryPoint> points = new HashMap<>();
    StateArchiveFiveLaneBlockCodecV3 codec = new StateArchiveFiveLaneBlockCodecV3();
    try (StateArchiveFiveLaneSegmentWriterV3 writer =
        new StateArchiveFiveLaneSegmentWriterV3(root, BASELINE,
            StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION)) {
      byte[] previous = BASELINE;
      for (int block = 1; block <= blocks; block++) {
        EncodedBundle bundle = codec.encode(diff(block, 200), previous,
            StateArchiveFileFormatV3.COMPRESSION_NONE);
        previous = bundle.getResultHistoryDigest();
        points.put(block, point(bundle));
        if (block > LAST_CHECKPOINT) {
          writer.appendFinalized(bundle);
          continue;
        }
        long checkpoint = (long) (block + CHECKPOINT_EVERY - 1) / CHECKPOINT_EVERY
            * CHECKPOINT_EVERY;
        writer.appendForCheckpoint(bundle, checkpoint, hash(50 + (int) checkpoint));
        if (block == checkpoint) {
          ArchiveDurabilityProof proof = writer.sync(checkpoint, point(bundle),
              hash(50 + (int) checkpoint));
          StateArchiveFiveLaneDurabilityProofV3.publish(root, proof);
          Files.copy(root.resolve(StateArchiveFiveLaneDurabilityProofV3.FILE_NAME),
              stash.resolve("proof-" + block));
        }
      }
    }
    return points;
  }

  private static Path segmentData(Path root, int laneId, long sequence) {
    return root.resolve("segments").resolve("shard-000000")
        .resolve(String.format("lane-%04d-seg-%020d.dat", laneId, sequence));
  }

  private static Path sealedDataFile(Path root) throws IOException {
    try (Stream<Path> paths = Files.walk(root.resolve("segments"))) {
      return paths.filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".dat"))
          .filter(path -> path.getFileName().toString().startsWith("lane-0000"))
          .sorted()
          .findFirst().orElseThrow(AssertionError::new);
    }
  }

  private static Map<String, Long> fileSizes(Path root) throws IOException {
    Map<String, Long> sizes = new HashMap<>();
    try (Stream<Path> files = Files.walk(root)) {
      for (Path path : files.filter(Files::isRegularFile).collect(Collectors.toList())) {
        sizes.put(root.relativize(path).toString(), Files.size(path));
      }
    }
    return sizes;
  }

  private static RecoveryPoint point(EncodedBundle bundle) {
    BlockSnapshotMeta meta = bundle.getDiff().getMeta();
    return new RecoveryPoint(meta.getEpoch(), meta.getBlockNumber(), meta.getTimestamp(),
        meta.getBlockHash(), meta.getParentHash(), bundle.getResultHistoryDigest());
  }

  private static BlockReverseDiff diff(int blockNumber, int valueLength) {
    byte[] value = new byte[valueLength];
    java.util.Arrays.fill(value, (byte) blockNumber);
    DbGroup group = new DbGroup(StateArchiveFileFormatV3.dbName(1),
        Collections.singletonList(new Entry(new byte[]{1}, OldValue.present(value))));
    return new BlockReverseDiff(new BlockSnapshotMeta(blockNumber, blockNumber,
        hash(blockNumber), hash(blockNumber - 1), blockNumber * 3_000L),
        Collections.singletonList(group));
  }

  private static byte[] hash(int suffix) {
    byte[] result = new byte[32];
    result[31] = (byte) suffix;
    return result;
  }
}
