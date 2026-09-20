package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.TestConstants;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.core.CommonCheckpointCapture;
import org.tron.core.db2.core.CommonCheckpointFile;
import org.tron.core.db2.core.CommonCheckpointMaterializer;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Authority;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Status;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointRedoCoordinator;
import org.tron.core.db2.core.CommonCheckpointRedoCoordinator.RecoveryAction;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateFlushTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/**
 * Real process-kill evidence for the Common commit to serving-index chain: the child JVM runs
 * prepare, WAL apply, materializer afterCommit and the serving worker bulk build, then halts
 * with {@link Runtime#halt(int)}; the parent reopens and verifies W does not roll back, the
 * durable index boundary drives the replay of (I, W], and a second start is zero-action.
 */
public class StateArchiveServingChainKillProcessTest {

  private static final int HALT_CODE = 92;
  private static final int CHILD_FAILURE_CODE = 3;
  private static final long ROTATION_TARGET_BYTES = 10_000L;
  private static final long SERVING_TAIL_DELAY_MILLIS = 10L;
  private static final byte[] FORMAT = hash(71);
  private static final byte[] BASELINE = hash(81);

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void assumeLevelDbAvailable() {
    TestConstants.assumeLevelDbAvailable();
  }

  @Test
  public void jvmHaltWithCommittedCommonAndUnpublishedIndexReplaysOnRestart() throws Exception {
    Path root = temporaryFolder.newFolder("chain-halt-before-index").toPath();
    runChild("halt-before-index-publish", root);
    // The builder thread reached the serving build before the halt, so Common was already
    // committed and retired while the durable index still sits at its empty boundary.
    assertTrue(Files.isRegularFile(root.resolve("builder-entered.marker")));

    CommonCheckpointTarget target = expectedTarget();
    try (StateArchiveAppendCheckpointMaterializerV3 archive = materializer(root, () -> { });
        CommonCheckpointRedoCoordinator coordinator = coordinator(root, archive)) {
      // W does not roll back: the retired WAL stays retired and the target stays published.
      assertEquals(RecoveryAction.NO_CHECKPOINT, coordinator.recover());
      assertEquals(Status.PUBLISHED, archive.inspect(target));
      assertEquals(target, archive.loadPublishedTargetIfPresent().orElse(null));
      assertEquals(-1, archive.servingIndexStatus().getIndexedThrough());
      // Restart replays (I, W] from the durable five-lane segments.
      archive.afterCommit(target);
      archive.completeServingInitialSync(target);
      assertEquals(1, archive.servingIndexStatus().getIndexedThrough());
      assertEquals(0, archive.servingIndexStatus().getPendingBlocks());
    }
    // A second start over the same directories is zero-action: nothing is rebuilt.
    try (StateArchiveAppendCheckpointMaterializerV3 archive = materializer(root, () -> { });
        CommonCheckpointRedoCoordinator coordinator = coordinator(root, archive)) {
      assertEquals(RecoveryAction.NO_CHECKPOINT, coordinator.recover());
      archive.afterCommit(target);
      archive.completeServingInitialSync(target);
      assertEquals(1, archive.servingIndexStatus().getIndexedThrough());
      assertEquals(0, archive.servingIndexStatus().getBuildSequence());
    }
  }

  @Test
  public void jvmHaltAfterIndexPublishRestartsZeroAction() throws Exception {
    Path root = temporaryFolder.newFolder("chain-halt-after-index").toPath();
    runChild("halt-after-index-publish", root);
    List<String> marker = Files.readAllLines(root.resolve("index-published.marker"),
        StandardCharsets.UTF_8);
    assertEquals(Collections.singletonList("indexedThrough=1"), marker);

    CommonCheckpointTarget target = expectedTarget();
    try (StateArchiveAppendCheckpointMaterializerV3 archive = materializer(root, () -> { });
        CommonCheckpointRedoCoordinator coordinator = coordinator(root, archive)) {
      // The commit and the index publication were both durable before the halt.
      assertEquals(RecoveryAction.NO_CHECKPOINT, coordinator.recover());
      assertEquals(Status.PUBLISHED, archive.inspect(target));
      archive.afterCommit(target);
      archive.completeServingInitialSync(target);
      assertEquals(1, archive.servingIndexStatus().getIndexedThrough());
      assertEquals(0, archive.servingIndexStatus().getBuildSequence());
    }
    try (StateArchiveAppendCheckpointMaterializerV3 archive = materializer(root, () -> { });
        CommonCheckpointRedoCoordinator coordinator = coordinator(root, archive)) {
      assertEquals(RecoveryAction.NO_CHECKPOINT, coordinator.recover());
      archive.afterCommit(target);
      archive.completeServingInitialSync(target);
      assertEquals(1, archive.servingIndexStatus().getIndexedThrough());
      assertEquals(0, archive.servingIndexStatus().getBuildSequence());
    }
  }

  private static StateArchiveAppendCheckpointMaterializerV3 materializer(Path root,
      Runnable beforeServingBuild) throws Exception {
    return new StateArchiveAppendCheckpointMaterializerV3(root.resolve("history"), FORMAT,
        Engine.LEVELDB, BASELINE, StateArchiveFileFormatV3.COMPRESSION_NONE,
        ROTATION_TARGET_BYTES, beforeServingBuild, SERVING_TAIL_DELAY_MILLIS);
  }

  private static CommonCheckpointRedoCoordinator coordinator(Path root,
      StateArchiveAppendCheckpointMaterializerV3 archive) {
    return new CommonCheckpointRedoCoordinator(new CommonCheckpointFile(root.resolve("wal")),
        new FakeMaterializer(Authority.CHAINBASE), new FakeMaterializer(Authority.PATH_STATE),
        archive);
  }

  private static CommonCheckpointTarget expectedTarget() {
    List<BlockReverseDiff> diffs = Collections.singletonList(diff(1, 32));
    return CommonCheckpointTarget.from(payload(diffs, plannedDescriptor(diffs)));
  }

  private static StateArchiveHotBatchDescriptor plannedDescriptor(List<BlockReverseDiff> diffs) {
    return StateArchiveHotStore.planCheckpointDescriptor(Engine.LEVELDB, 0, hash(0),
        new byte[StateArchiveFileFormatV3.HASH_LENGTH], diffs);
  }

  private static void runChild(String mode, Path root) throws Exception {
    Process child = new ProcessBuilder(javaExecutable(), "-cp", runtimeClasspath(),
        StateArchiveServingChainKillProcessTest.class.getName(), mode, root.toString())
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
      throw new IllegalArgumentException("unknown serving chain kill process mode");
    }
    Path root = Paths.get(args[1]);
    List<BlockReverseDiff> diffs = Collections.singletonList(diff(1, 32));
    try (StateArchiveAppendCheckpointMaterializerV3 archive = materializer(root,
        buildHook(args[0], root));
        CommonCheckpointRedoCoordinator coordinator = coordinator(root, archive)) {
      StateArchiveHotBatchDescriptor descriptor = archive.planCheckpoint(diffs);
      CommonCheckpointPayload payload = payload(diffs, descriptor);
      CommonCheckpointTarget target = archive.prepare(
          CommonCheckpointCapture.create(payload, diffs, descriptor));
      coordinator.apply(payload);
      if ("halt-before-index-publish".equals(args[0])) {
        // apply returned while the builder thread is inside the serving build hook; the halt
        // arrives from that thread. Wait here so the JVM stays alive until then.
        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
      } else if ("halt-after-index-publish".equals(args[0])) {
        archive.completeServingInitialSync(target);
        if (archive.servingIndexStatus().getIndexedThrough() != 1) {
          System.exit(CHILD_FAILURE_CODE);
        }
        storeMarker(root.resolve("index-published.marker"), "indexedThrough=1");
        Runtime.getRuntime().halt(HALT_CODE);
      } else {
        throw new IllegalArgumentException("unknown serving chain kill process mode");
      }
    }
    System.exit(CHILD_FAILURE_CODE);
  }

  private static Runnable buildHook(String mode, Path root) {
    if (!"halt-before-index-publish".equals(mode)) {
      return () -> { };
    }
    return () -> {
      try {
        storeMarker(root.resolve("builder-entered.marker"), "entered");
      } catch (java.io.IOException failure) {
        throw new IllegalStateException(failure);
      }
      Runtime.getRuntime().halt(HALT_CODE);
    };
  }

  private static void storeMarker(Path path, String content) throws java.io.IOException {
    byte[] encoded = (content + "\n").getBytes(StandardCharsets.UTF_8);
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
      channel.write(ByteBuffer.wrap(encoded));
      channel.force(true);
    }
    try (FileChannel directory = FileChannel.open(path.getParent(), StandardOpenOption.READ)) {
      directory.force(true);
    }
  }

  private static CommonCheckpointPayload payload(List<BlockReverseDiff> diffs,
      StateArchiveHotBatchDescriptor descriptor) {
    List<PathStateFlushTarget.BlockBinding> bindings = new ArrayList<>();
    for (BlockReverseDiff diff : diffs) {
      BlockSnapshotMeta meta = diff.getMeta();
      PathStateFlushTarget.BlockBinding binding = mock(PathStateFlushTarget.BlockBinding.class);
      when(binding.getMeta()).thenReturn(meta);
      when(binding.getParentStateRoot()).thenReturn(hash(30 + (int) meta.getBlockNumber()));
      when(binding.getStateRoot()).thenReturn(hash(31 + (int) meta.getBlockNumber()));
      when(binding.getTransitionPayloadDigest()).thenReturn(hash(90));
      bindings.add(binding);
    }
    PathStateFlushTarget path = mock(PathStateFlushTarget.class);
    byte[] parentStateRoot = bindings.get(0).getParentStateRoot();
    byte[] stateRoot = bindings.get(bindings.size() - 1).getStateRoot();
    when(path.getBlocks()).thenReturn(bindings);
    when(path.getParentStateRoot()).thenReturn(parentStateRoot);
    when(path.getStateRoot()).thenReturn(stateRoot);
    when(path.getStores()).thenReturn(Collections.emptyList());
    when(path.getSuperNodeMutations()).thenReturn(Collections.emptyList());
    return CommonCheckpointPayload.createV2(FORMAT, path, descriptor, Collections.emptyList());
  }

  private static BlockReverseDiff diff(int blockNumber, int valueLength) {
    List<DbGroup> groups;
    if (valueLength == 0) {
      groups = Collections.emptyList();
    } else {
      byte[] value = new byte[valueLength];
      java.util.Arrays.fill(value, (byte) blockNumber);
      groups = Collections.singletonList(new DbGroup("code", Collections.singletonList(
          new Entry(new byte[]{1}, OldValue.present(value)))));
    }
    return new BlockReverseDiff(BlockSnapshotMeta.forBlock(blockNumber, hash(blockNumber),
        hash(blockNumber - 1), blockNumber * 3_000L), groups);
  }

  private static String javaExecutable() {
    return Paths.get(System.getProperty("java.home"), "bin", "java").toString();
  }

  private static String runtimeClasspath() {
    Set<String> entries = new LinkedHashSet<>();
    String configured = System.getProperty("java.class.path", "");
    Collections.addAll(entries, configured.split(java.util.regex.Pattern.quote(
        File.pathSeparator)));
    for (ClassLoader loader = StateArchiveServingChainKillProcessTest.class.getClassLoader();
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

  private static byte[] hash(int marker) {
    byte[] hash = new byte[32];
    hash[31] = (byte) marker;
    return hash;
  }

  /** In-memory stand-in for the non-archive authorities; the WAL retires before the halt. */
  private static final class FakeMaterializer implements CommonCheckpointMaterializer {

    private final Authority authority;
    private Status status = Status.NEEDS_MATERIALIZATION;

    private FakeMaterializer(Authority authority) {
      this.authority = authority;
    }

    @Override
    public Authority authority() {
      return authority;
    }

    @Override
    public Status inspect(CommonCheckpointTarget target) {
      return status;
    }

    @Override
    public void materialize(CommonCheckpointPayload payload, CommonCheckpointTarget target) {
      status = Status.MATERIALIZED;
    }

    @Override
    public void publish(CommonCheckpointTarget target) {
      status = Status.PUBLISHED;
    }
  }
}
