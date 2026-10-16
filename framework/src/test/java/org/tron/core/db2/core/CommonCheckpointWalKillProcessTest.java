package org.tron.core.db2.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Authority;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Status;
import org.tron.core.db2.core.CommonCheckpointRedoCoordinator.RecoveryAction;
import org.tron.core.db2.stateroot.PathStateFlushTarget;

/**
 * Real process-kill evidence for the Common checkpoint WAL: the child JVM halts with
 * {@link Runtime#halt(int)} at publish, redo and retire boundaries instead of throwing from a
 * fault hook, and the parent reopens against the same directory.
 */
public class CommonCheckpointWalKillProcessTest {

  private static final int HALT_CODE = 92;
  private static final int CHILD_FAILURE_CODE = 3;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void jvmHaltAfterTemporaryForceNeverAcceptsTemporaryAuthority() throws Exception {
    Path root = temporaryFolder.newFolder("wal-halt-temporary-force").toPath();
    runChild("publish-halt-after-temporary-force", root);

    Path walDirectory = root.resolve("wal");
    CommonCheckpointFile file = new CommonCheckpointFile(walDirectory);
    // The fully forced temporary file is not an authority: no checkpoint is visible.
    assertTrue(Files.isRegularFile(walDirectory.resolve(".COMMON_CHECKPOINT.tmp")));
    assertNull(file.loadIfPresent());
    try (DurableMaterializers materializers = new DurableMaterializers(root)) {
      CommonCheckpointRedoCoordinator coordinator = materializers.coordinator(file);
      assertEquals(RecoveryAction.NO_CHECKPOINT, coordinator.recover());
      // A clean retry publishes over the orphaned temporary file and completes redo.
      assertEquals(RecoveryAction.COMPLETED_REDO, coordinator.apply(payload()));
      assertFalse(Files.exists(walDirectory.resolve(".COMMON_CHECKPOINT.tmp")));
      assertFalse(Files.exists(walDirectory.resolve("COMMON_CHECKPOINT")));
      materializers.assertAllPublished();
      coordinator.requirePublished(CommonCheckpointTarget.from(payload()));
      assertEquals(RecoveryAction.NO_CHECKPOINT, coordinator.recover());
    }
  }

  @Test
  public void jvmHaltAfterAtomicPublishRedoesCheckpointOnRecovery() throws Exception {
    Path root = temporaryFolder.newFolder("wal-halt-atomic-publish").toPath();
    runChild("publish-halt-after-atomic-publish", root);

    CommonCheckpointFile file = new CommonCheckpointFile(root.resolve("wal"));
    assertNotNull(file.loadRequired());
    try (DurableMaterializers materializers = new DurableMaterializers(root)) {
      CommonCheckpointRedoCoordinator coordinator = materializers.coordinator(file);
      assertEquals(RecoveryAction.COMPLETED_REDO, coordinator.recover());
      materializers.assertAllPublished();
      assertFalse(Files.exists(root.resolve("wal").resolve("COMMON_CHECKPOINT")));
      coordinator.requirePublished(CommonCheckpointTarget.from(payload()));
      // A second recovery is zero-action.
      assertEquals(RecoveryAction.NO_CHECKPOINT, coordinator.recover());
      materializers.assertAllPublished();
    }
  }

  @Test
  public void jvmHaltAfterChainbaseMaterializeResumesRedoFromDurableProgress() throws Exception {
    Path root = temporaryFolder.newFolder("wal-halt-chainbase-materialize").toPath();
    runChild("redo-halt-after-chainbase-materialize", root);

    CommonCheckpointFile file = new CommonCheckpointFile(root.resolve("wal"));
    assertNotNull(file.loadRequired());
    try (DurableMaterializers materializers = new DurableMaterializers(root)) {
      // The child died after chainbase materialized durably but before any publication.
      materializers.assertStatus(Authority.CHAINBASE, Status.MATERIALIZED);
      materializers.assertStatus(Authority.PATH_STATE, Status.NEEDS_MATERIALIZATION);
      materializers.assertStatus(Authority.STATE_ARCHIVE, Status.NEEDS_MATERIALIZATION);
      CommonCheckpointRedoCoordinator coordinator = materializers.coordinator(file);
      assertEquals(RecoveryAction.COMPLETED_REDO, coordinator.recover());
      materializers.assertAllPublished();
      coordinator.requirePublished(CommonCheckpointTarget.from(payload()));
      assertEquals(RecoveryAction.NO_CHECKPOINT, coordinator.recover());
    }
  }

  @Test
  public void jvmHaltAfterDurablePublishInsideBarrierKeepsPublication() throws Exception {
    Path root = temporaryFolder.newFolder("wal-halt-inside-publish").toPath();
    runChild("redo-halt-inside-pathstate-publish", root);

    CommonCheckpointFile file = new CommonCheckpointFile(root.resolve("wal"));
    assertNotNull(file.loadRequired());
    try (DurableMaterializers materializers = new DurableMaterializers(root)) {
      // The child died after the path-state publication was durable but before its ack
      // returned to the coordinator: the durable publication must be kept, not redone blindly.
      materializers.assertStatus(Authority.CHAINBASE, Status.PUBLISHED);
      materializers.assertStatus(Authority.PATH_STATE, Status.PUBLISHED);
      materializers.assertStatus(Authority.STATE_ARCHIVE, Status.MATERIALIZED);
      CommonCheckpointRedoCoordinator coordinator = materializers.coordinator(file);
      assertEquals(RecoveryAction.COMPLETED_REDO, coordinator.recover());
      materializers.assertAllPublished();
      assertFalse(Files.exists(root.resolve("wal").resolve("COMMON_CHECKPOINT")));
      coordinator.requirePublished(CommonCheckpointTarget.from(payload()));
      assertEquals(RecoveryAction.NO_CHECKPOINT, coordinator.recover());
    }
  }

  @Test
  public void jvmHaltAfterRetireRestartsZeroAction() throws Exception {
    Path root = temporaryFolder.newFolder("wal-halt-after-retire").toPath();
    runChild("redo-halt-after-retire", root);

    CommonCheckpointFile file = new CommonCheckpointFile(root.resolve("wal"));
    assertNull(file.loadIfPresent());
    try (DurableMaterializers materializers = new DurableMaterializers(root)) {
      CommonCheckpointRedoCoordinator coordinator = materializers.coordinator(file);
      // The checkpoint retired before the halt: recovery is zero-action and nothing is redone.
      assertEquals(RecoveryAction.NO_CHECKPOINT, coordinator.recover());
      materializers.assertAllPublished();
      coordinator.requirePublished(CommonCheckpointTarget.from(payload()));
    }
  }

  private static void runChild(String mode, Path root) throws Exception {
    Process child = new ProcessBuilder(javaExecutable(), "-cp", runtimeClasspath(),
        CommonCheckpointWalKillProcessTest.class.getName(), mode, root.toString())
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
      throw new IllegalArgumentException("unknown common checkpoint wal kill process mode");
    }
    Path root = Paths.get(args[1]);
    CommonCheckpointPayload payload = payload();
    CommonCheckpointFile wal = new CommonCheckpointFile(root.resolve("wal"));
    switch (args[0]) {
      case "publish-halt-after-temporary-force":
        new CommonCheckpointFile(root.resolve("wal"),
            CommonCheckpointPayloadCodec.DEFAULT_MAX_ENCODED_LENGTH,
            haltAtFile(CommonCheckpointFile.Stage.AFTER_TEMPORARY_FORCE)).publish(payload);
        break;
      case "publish-halt-after-atomic-publish":
        new CommonCheckpointFile(root.resolve("wal"),
            CommonCheckpointPayloadCodec.DEFAULT_MAX_ENCODED_LENGTH,
            haltAtFile(CommonCheckpointFile.Stage.AFTER_ATOMIC_PUBLISH)).publish(payload);
        break;
      case "redo-halt-after-chainbase-materialize":
        try (DurableMaterializers materializers = new DurableMaterializers(root)) {
          materializers.coordinator(wal,
              haltAtCoordinator(
                  CommonCheckpointRedoCoordinator.Stage.AFTER_CHAINBASE_MATERIALIZE))
              .apply(payload);
        }
        break;
      case "redo-halt-inside-pathstate-publish":
        try (DurableMaterializers materializers = new DurableMaterializers(root)) {
          materializers.armHaltAfterDurablePublish(Authority.PATH_STATE);
          materializers.coordinator(wal).apply(payload);
        }
        break;
      case "redo-halt-after-retire":
        try (DurableMaterializers materializers = new DurableMaterializers(root)) {
          materializers.coordinator(wal,
              haltAtCoordinator(CommonCheckpointRedoCoordinator.Stage.AFTER_CHECKPOINT_RETIRE))
              .apply(payload);
        }
        break;
      default:
        throw new IllegalArgumentException("unknown common checkpoint wal kill process mode");
    }
    System.exit(CHILD_FAILURE_CODE);
  }

  private static CommonCheckpointFile.FaultHook haltAtFile(CommonCheckpointFile.Stage expected) {
    return (actual, path) -> {
      if (actual == expected) {
        Runtime.getRuntime().halt(HALT_CODE);
      }
    };
  }

  private static CommonCheckpointRedoCoordinator.FaultHook haltAtCoordinator(
      CommonCheckpointRedoCoordinator.Stage expected) {
    return actual -> {
      if (actual == expected) {
        Runtime.getRuntime().halt(HALT_CODE);
      }
    };
  }

  private static CommonCheckpointPayload payload() {
    BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(1, hash(1), hash(0), 3_000L);
    PathStateFlushTarget.BlockBinding binding = mock(PathStateFlushTarget.BlockBinding.class);
    when(binding.getMeta()).thenReturn(meta);
    when(binding.getParentStateRoot()).thenReturn(hash(5));
    when(binding.getStateRoot()).thenReturn(hash(6));
    when(binding.getTransitionPayloadDigest()).thenReturn(hash(3));
    PathStateFlushTarget target = mock(PathStateFlushTarget.class);
    when(target.getBlocks()).thenReturn(Collections.singletonList(binding));
    when(target.getParentStateRoot()).thenReturn(hash(5));
    when(target.getStateRoot()).thenReturn(hash(6));
    when(target.getStores()).thenReturn(Collections.emptyList());
    when(target.getSuperNodeMutations()).thenReturn(Collections.emptyList());
    return CommonCheckpointPayload.create(hash(7), target,
        Collections.singletonList(new BlockReverseDiff(meta, Collections.emptyList())),
        Collections.singletonList(new CommonCheckpointPayload.StoreMutations("code",
            Collections.singletonList(
                new CommonCheckpointPayload.Mutation(new byte[]{1}, new byte[]{2})))));
  }

  private static String javaExecutable() {
    return Paths.get(System.getProperty("java.home"), "bin", "java").toString();
  }

  private static String runtimeClasspath() {
    Set<String> entries = new LinkedHashSet<>();
    String configured = System.getProperty("java.class.path", "");
    Collections.addAll(entries, configured.split(java.util.regex.Pattern.quote(
        File.pathSeparator)));
    for (ClassLoader loader = CommonCheckpointWalKillProcessTest.class.getClassLoader();
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

  private static byte[] hash(int seed) {
    byte[] hash = new byte[32];
    for (int index = 0; index < hash.length; index++) {
      hash[index] = (byte) (seed + index);
    }
    return hash;
  }

  private static String hex(byte[] value) {
    StringBuilder builder = new StringBuilder(value.length * 2);
    for (byte current : value) {
      builder.append(Character.forDigit((current >> 4) & 0xF, 16));
      builder.append(Character.forDigit(current & 0xF, 16));
    }
    return builder.toString();
  }

  /** Three durable fake authorities whose status survives the child process halt. */
  private static final class DurableMaterializers implements AutoCloseable {

    private final java.util.EnumMap<Authority, DurableMaterializer> materializers =
        new java.util.EnumMap<>(Authority.class);

    private DurableMaterializers(Path root) {
      for (Authority authority : Authority.values()) {
        materializers.put(authority, new DurableMaterializer(
            root.resolve("authority-" + authority), authority));
      }
    }

    private CommonCheckpointRedoCoordinator coordinator(CommonCheckpointFile file) {
      return coordinator(file, stage -> { });
    }

    private CommonCheckpointRedoCoordinator coordinator(CommonCheckpointFile file,
        CommonCheckpointRedoCoordinator.FaultHook faultHook) {
      return new CommonCheckpointRedoCoordinator(file, materializers.get(Authority.CHAINBASE),
          materializers.get(Authority.PATH_STATE), materializers.get(Authority.STATE_ARCHIVE),
          faultHook);
    }

    private void armHaltAfterDurablePublish(Authority authority) {
      materializers.get(authority).haltAfterDurablePublish = true;
    }

    private void assertStatus(Authority authority, Status expected) throws Exception {
      assertEquals(expected, materializers.get(authority)
          .inspect(CommonCheckpointTarget.from(payload())));
    }

    private void assertAllPublished() throws Exception {
      for (Authority authority : Authority.values()) {
        assertStatus(authority, Status.PUBLISHED);
      }
    }

    @Override
    public void close() throws Exception {
      for (DurableMaterializer materializer : materializers.values()) {
        materializer.close();
      }
    }
  }

  /** File-backed authority: materialize/publish advance an atomically replaced status file. */
  private static final class DurableMaterializer implements CommonCheckpointMaterializer {

    private final Authority authority;
    private final Path directory;
    private final Path statusFile;
    private boolean haltAfterDurablePublish;

    private DurableMaterializer(Path directory, Authority authority) {
      this.directory = directory;
      this.authority = authority;
      this.statusFile = directory.resolve("status");
    }

    @Override
    public Authority authority() {
      return authority;
    }

    @Override
    public Status inspect(CommonCheckpointTarget target) throws java.io.IOException {
      if (!Files.isRegularFile(statusFile)) {
        return Status.NEEDS_MATERIALIZATION;
      }
      List<String> lines = Files.readAllLines(statusFile, StandardCharsets.UTF_8);
      if (lines.size() != 2) {
        throw new java.io.IOException("durable authority status is corrupt");
      }
      if (!lines.get(0).equals(hex(target.getPayloadDigest()))) {
        throw new java.io.IOException("durable authority status belongs to a different target");
      }
      return Status.valueOf(lines.get(1));
    }

    @Override
    public void materialize(CommonCheckpointPayload payload, CommonCheckpointTarget target)
        throws java.io.IOException {
      if (inspect(target) == Status.NEEDS_MATERIALIZATION) {
        writeDurable(Status.MATERIALIZED, target);
      }
    }

    @Override
    public void publish(CommonCheckpointTarget target) throws java.io.IOException {
      if (inspect(target) != Status.MATERIALIZED) {
        throw new java.io.IOException("publish without exact materialization");
      }
      writeDurable(Status.PUBLISHED, target);
      if (haltAfterDurablePublish) {
        // The publication is durable; the ack to the coordinator is lost with the process.
        Runtime.getRuntime().halt(HALT_CODE);
      }
    }

    private void writeDurable(Status status, CommonCheckpointTarget target)
        throws java.io.IOException {
      Files.createDirectories(directory);
      byte[] encoded = (hex(target.getPayloadDigest()) + "\n" + status + "\n")
          .getBytes(StandardCharsets.UTF_8);
      Path temporary = directory.resolve(".status.tmp");
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
        channel.write(ByteBuffer.wrap(encoded));
        channel.force(true);
      }
      Files.move(temporary, statusFile, StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      try (FileChannel directoryChannel = FileChannel.open(directory,
          StandardOpenOption.READ)) {
        directoryChannel.force(true);
      }
    }
  }
}
