package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
import org.tron.core.db2.archive.StateArchiveSegmentFormatV3.SealedSegment;

/**
 * Real process-kill evidence for five-lane segment rotation: the child JVM rotates every lane
 * under a tiny rotation target and halts with {@link Runtime#halt(int)} instead of a fault hook.
 */
public class StateArchiveRotationKillProcessTest {

  private static final int HALT_CODE = 92;
  private static final int CHILD_FAILURE_CODE = 3;
  private static final long ROTATION_TARGET_BYTES = 10_000L;
  private static final long SEAL_FRAME_BYTES = StateArchiveFileFormatV3.SEAL_HEADER_LENGTH
      + StateArchiveFileFormatV3.FRAME_TRAILER_LENGTH;
  private static final int BLOCK_LIMIT = 400;
  private static final byte[] BASELINE = hash(20);
  private static final byte[] COMMON_TARGET = hash(90);
  private static final String IDENTITY_FILE = "identity.properties";

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void jvmHaltRightAfterFinalSealKeepsIdentityAcrossTwoReopens() throws Exception {
    Path root = temporaryFolder.newFolder("rotation-halt-after-seal").toPath();
    runChild("halt-after-seal", root);
    Identity identity = Identity.load(root.resolve(IDENTITY_FILE));
    assertEquals(5, identity.sealedCount);
    assertTrue("rotation must advance the head beyond one block", identity.blockNumber > 1);
    // The final appended block was never marked: the recovered proof covers the previous sync.
    assertEquals(identity.blockNumber - 1, identity.lastSyncSequence);
    assertRecoveredIdentity(root, identity);
    assertSecondReopenIsZeroAction(root, identity);
  }

  @Test
  public void jvmHaltAfterPostRotationMarkerKeepsIdentityAcrossTwoReopens() throws Exception {
    Path root = temporaryFolder.newFolder("rotation-halt-after-sync").toPath();
    runChild("halt-after-rotation-sync", root);
    Identity identity = Identity.load(root.resolve(IDENTITY_FILE));
    assertEquals(5, identity.sealedCount);
    // The marker for the crash-time head was forced into the fresh segments before the halt.
    assertEquals(identity.blockNumber, identity.lastSyncSequence);
    assertRecoveredIdentity(root, identity);
    assertSecondReopenIsZeroAction(root, identity);
  }

  private static void assertRecoveredIdentity(Path root, Identity identity) throws Exception {
    try (StateArchiveFiveLaneSegmentWriterV3 reopened =
        new StateArchiveFiveLaneSegmentWriterV3(root, BASELINE,
            StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION_TARGET_BYTES)) {
      identity.assertSameHead(reopened);
      ArchiveDurabilityProof proof = reopened.getLastDurabilityProof();
      assertNotNull("rotation markers must rebuild a durability proof", proof);
      assertEquals(identity.lastSyncSequence, proof.getCheckpointSequence());
      assertEquals(identity.lastSyncSequence, proof.getTarget().getBlockNumber());
      assertEquals(5, proof.getFileTails().size());
      reopened.verifyDurabilityProof(proof);
      // The fast reopen trusts sealed history: only the terminal seal frame is read.
      assertEquals(identity.sealedCount * SEAL_FRAME_BYTES,
          reopened.getReopenSealedDataReadBytes());
    }
  }

  private static void assertSecondReopenIsZeroAction(Path root, Identity identity)
      throws Exception {
    List<String> before = fileListing(root);
    try (StateArchiveFiveLaneSegmentWriterV3 reopened =
        new StateArchiveFiveLaneSegmentWriterV3(root, BASELINE,
            StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION_TARGET_BYTES)) {
      identity.assertSameHead(reopened);
      ArchiveDurabilityProof proof = reopened.getLastDurabilityProof();
      assertNotNull(proof);
      assertEquals(identity.lastSyncSequence, proof.getCheckpointSequence());
      reopened.verifyDurabilityProof(proof);
      assertEquals(identity.sealedCount * SEAL_FRAME_BYTES,
          reopened.getReopenSealedDataReadBytes());
    }
    assertEquals("second reopen must not rewrite any archive file", before, fileListing(root));
  }

  private static List<String> fileListing(Path root) throws Exception {
    try (Stream<Path> paths = Files.walk(root)) {
      return paths.filter(Files::isRegularFile).sorted().map(path -> {
        try {
          return root.relativize(path) + ":" + Files.size(path);
        } catch (java.io.IOException failure) {
          throw new IllegalStateException(failure);
        }
      }).collect(Collectors.toList());
    }
  }

  private static void runChild(String mode, Path root) throws Exception {
    Process child = new ProcessBuilder(javaExecutable(), "-cp", runtimeClasspath(),
        StateArchiveRotationKillProcessTest.class.getName(), mode, root.toString())
        .redirectErrorStream(true)
        .redirectOutput(root.resolve(mode + ".log").toFile()).start();
    try {
      assertTrue("child process timed out", child.waitFor(60, TimeUnit.SECONDS));
      assertEquals(HALT_CODE, child.exitValue());
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        child.waitFor();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException("unknown rotation kill process mode");
    }
    boolean syncBeforeHalt;
    if ("halt-after-seal".equals(args[0])) {
      syncBeforeHalt = false;
    } else if ("halt-after-rotation-sync".equals(args[0])) {
      syncBeforeHalt = true;
    } else {
      throw new IllegalArgumentException("unknown rotation kill process mode");
    }
    Path root = Paths.get(args[1]);
    StateArchiveFiveLaneBlockCodecV3 codec = new StateArchiveFiveLaneBlockCodecV3();
    StateArchiveFiveLaneSegmentWriterV3 writer =
        new StateArchiveFiveLaneSegmentWriterV3(root, BASELINE,
            StateArchiveFileFormatV3.COMPRESSION_NONE, ROTATION_TARGET_BYTES);
    long lastSyncSequence = 0;
    for (int block = 1; block <= BLOCK_LIMIT; block++) {
      EncodedBundle bundle = codec.encode(diff(block), writer.getResultHistoryDigest(),
          StateArchiveFileFormatV3.COMPRESSION_NONE);
      writer.append(bundle);
      if (rotatedLaneCount(writer) == 5) {
        // The fifth lane just sealed inside append; halting now lands between seal and marker,
        // or right after the post-rotation marker was forced into the fresh segments.
        if (syncBeforeHalt) {
          StateArchiveFiveLaneDurabilityProofV3.publish(root,
              writer.sync(block, point(bundle), COMMON_TARGET));
          lastSyncSequence = block;
        } else {
          lastSyncSequence = block - 1;
        }
        Identity.capture(writer, lastSyncSequence).store(root.resolve(IDENTITY_FILE));
        Runtime.getRuntime().halt(HALT_CODE);
      }
      StateArchiveFiveLaneDurabilityProofV3.publish(root,
          writer.sync(block, point(bundle), COMMON_TARGET));
      lastSyncSequence = block;
    }
    System.exit(CHILD_FAILURE_CODE);
  }

  private static int rotatedLaneCount(StateArchiveFiveLaneSegmentWriterV3 writer) {
    Set<Integer> laneIds = new HashSet<>();
    for (SealedSegment segment : writer.getSealedSegments()) {
      laneIds.add(segment.getLaneId());
    }
    return laneIds.size();
  }

  private static BlockReverseDiff diff(int blockNumber) {
    byte[] value = new byte[10];
    java.util.Arrays.fill(value, (byte) blockNumber);
    DbGroup group = new DbGroup(StateArchiveFileFormatV3.dbName(1),
        Collections.singletonList(new Entry(new byte[]{1}, OldValue.present(value))));
    return new BlockReverseDiff(new BlockSnapshotMeta(blockNumber, blockNumber,
        hash(blockNumber), hash(blockNumber - 1), blockNumber * 3_000L),
        Collections.singletonList(group));
  }

  private static RecoveryPoint point(EncodedBundle bundle) {
    BlockSnapshotMeta meta = bundle.getDiff().getMeta();
    return new RecoveryPoint(meta.getEpoch(), meta.getBlockNumber(), meta.getTimestamp(),
        meta.getBlockHash(), meta.getParentHash(), bundle.getResultHistoryDigest());
  }

  private static String javaExecutable() {
    return Paths.get(System.getProperty("java.home"), "bin", "java").toString();
  }

  private static String runtimeClasspath() {
    Set<String> entries = new LinkedHashSet<>();
    String configured = System.getProperty("java.class.path", "");
    Collections.addAll(entries, configured.split(java.util.regex.Pattern.quote(
        File.pathSeparator)));
    for (ClassLoader loader = StateArchiveRotationKillProcessTest.class.getClassLoader();
        loader != null; loader = loader.getParent()) {
      if (loader instanceof URLClassLoader) {
        for (URL url : ((URLClassLoader) loader).getURLs()) {
          if ("file".equals(url.getProtocol())) {
            try {
              entries.add(Paths.get(url.toURI()).toString());
            } catch (java.net.URISyntaxException invalid) {
              throw new IllegalStateException("invalid test runtime classpath", invalid);
            }
          }
        }
      }
    }
    return String.join(File.pathSeparator, entries);
  }

  private static byte[] hash(int suffix) {
    byte[] result = new byte[32];
    result[31] = (byte) suffix;
    return result;
  }

  private static String hex(byte[] value) {
    StringBuilder builder = new StringBuilder(value.length * 2);
    for (byte current : value) {
      builder.append(Character.forDigit((current >> 4) & 0xF, 16));
      builder.append(Character.forDigit(current & 0xF, 16));
    }
    return builder.toString();
  }

  private static byte[] unhex(String value) {
    byte[] result = new byte[value.length() / 2];
    for (int index = 0; index < result.length; index++) {
      result[index] = (byte) Integer.parseInt(
          value.substring(index * 2, index * 2 + 2), 16);
    }
    return result;
  }

  /** Crash-time head identity persisted by the child right before the halt. */
  private static final class Identity {

    private final long epoch;
    private final long blockNumber;
    private final byte[] blockHash;
    private final byte[] parentHash;
    private final byte[] resultHistoryDigest;
    private final int sealedCount;
    private final long lastSyncSequence;

    private Identity(long epoch, long blockNumber, byte[] blockHash, byte[] parentHash,
        byte[] resultHistoryDigest, int sealedCount, long lastSyncSequence) {
      this.epoch = epoch;
      this.blockNumber = blockNumber;
      this.blockHash = blockHash;
      this.parentHash = parentHash;
      this.resultHistoryDigest = resultHistoryDigest;
      this.sealedCount = sealedCount;
      this.lastSyncSequence = lastSyncSequence;
    }

    private static Identity capture(StateArchiveFiveLaneSegmentWriterV3 writer,
        long lastSyncSequence) {
      BlockSnapshotMeta head = writer.getAppendHead();
      return new Identity(head.getEpoch(), head.getBlockNumber(), head.getBlockHash(),
          head.getParentHash(), writer.getResultHistoryDigest(),
          writer.getSealedSegments().size(), lastSyncSequence);
    }

    private void store(Path path) throws Exception {
      List<String> lines = new ArrayList<>();
      lines.add("epoch=" + epoch);
      lines.add("blockNumber=" + blockNumber);
      lines.add("blockHash=" + hex(blockHash));
      lines.add("parentHash=" + hex(parentHash));
      lines.add("resultHistoryDigest=" + hex(resultHistoryDigest));
      lines.add("sealedCount=" + sealedCount);
      lines.add("lastSyncSequence=" + lastSyncSequence);
      byte[] encoded = String.join("\n", lines).getBytes(StandardCharsets.UTF_8);
      try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
        channel.write(ByteBuffer.wrap(encoded));
        channel.force(true);
      }
      try (FileChannel directory = FileChannel.open(path.getParent(),
          StandardOpenOption.READ)) {
        directory.force(true);
      }
    }

    private static Identity load(Path path) throws Exception {
      java.util.Properties properties = new java.util.Properties();
      try (java.io.InputStream input = Files.newInputStream(path)) {
        properties.load(input);
      }
      return new Identity(Long.parseLong(properties.getProperty("epoch")),
          Long.parseLong(properties.getProperty("blockNumber")),
          unhex(properties.getProperty("blockHash")),
          unhex(properties.getProperty("parentHash")),
          unhex(properties.getProperty("resultHistoryDigest")),
          Integer.parseInt(properties.getProperty("sealedCount")),
          Long.parseLong(properties.getProperty("lastSyncSequence")));
    }

    private void assertSameHead(StateArchiveFiveLaneSegmentWriterV3 writer) {
      BlockSnapshotMeta head = writer.getAppendHead();
      assertNotNull(head);
      assertEquals(epoch, head.getEpoch());
      assertEquals(blockNumber, head.getBlockNumber());
      org.junit.Assert.assertArrayEquals(blockHash, head.getBlockHash());
      org.junit.Assert.assertArrayEquals(parentHash, head.getParentHash());
      org.junit.Assert.assertArrayEquals(resultHistoryDigest, writer.getResultHistoryDigest());
      assertEquals(sealedCount, writer.getSealedSegments().size());
    }
  }
}
