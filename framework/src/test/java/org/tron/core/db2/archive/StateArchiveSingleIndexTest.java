package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.PersistentServingKeyIndexGeneration.MutableIndex;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class StateArchiveSingleIndexTest {
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void processHaltKeepsPagesAndProgressAtTheSameBoundary() throws Exception {
    for (String stage : new String[]{"before", "after"}) {
      Path root = temporaryFolder.newFolder().toPath();
      Process child = new ProcessBuilder(java.nio.file.Paths.get(
          System.getProperty("java.home"), "bin", "java").toString(), "-cp", runtimeClasspath(),
          getClass().getName(), stage, root.toString()).redirectErrorStream(true)
          .redirectOutput(root.resolve("child.log").toFile()).start();
      try {
        org.junit.Assert.assertTrue(child.waitFor(30, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(92, child.exitValue());
      } finally {
        if (child.isAlive()) {
          child.destroyForcibly();
          child.waitFor();
        }
      }
      for (int restart = 0; restart < 2; restart++) {
        try (MutableIndex recovered = new MutableIndex(root.resolve("index"), Engine.ROCKSDB);
            PersistentServingKeyIndexGeneration view = recovered.pin()) {
          long last = "after".equals(stage) ? 102 : 101;
          assertEquals(100, view.getIndexedFrom());
          assertEquals(last, view.getIndexedThrough());
          assertEquals(last - 100, view.getKeyChangeCount());
          assertEquals("after".equals(stage) ? OptionalLong.of(102) : OptionalLong.empty(),
              view.firstChangeAfter("code", new byte[]{7}, 101, last));
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    MutableIndex index = new MutableIndex(java.nio.file.Paths.get(args[1]).resolve("index"),
        Engine.ROCKSDB);
    index.append("first", plan(101, 1), hash(1), () -> { }, () -> { });
    index.append("second", plan(102, 1), hash(2), () -> {
      if ("before".equals(args[0])) {
        Runtime.getRuntime().halt(92);
      }
    }, () -> Runtime.getRuntime().halt(92));
  }

  private static String runtimeClasspath() throws Exception {
    java.util.Set<String> entries = new java.util.LinkedHashSet<>();
    Collections.addAll(entries, System.getProperty("java.class.path", "").split(
        java.util.regex.Pattern.quote(java.io.File.pathSeparator)));
    for (ClassLoader loader = StateArchiveSingleIndexTest.class.getClassLoader(); loader != null;
        loader = loader.getParent()) {
      if (loader instanceof java.net.URLClassLoader) {
        for (java.net.URL url : ((java.net.URLClassLoader) loader).getURLs()) {
          if ("file".equals(url.getProtocol())) {
            entries.add(java.nio.file.Paths.get(url.toURI()).toString());
          }
        }
      }
    }
    return String.join(java.io.File.pathSeparator, entries);
  }

  @Test
  public void nonzeroStartSurvivesPagesSnapshotsAndReopen() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    List<BlockReverseDiff> first = diffs(101, 512);
    List<BlockReverseDiff> next = diffs(613, 3);
    try (StateArchiveServingIndexBuildCoordinatorV3 owner =
        new StateArchiveServingIndexBuildCoordinatorV3(root, Engine.ROCKSDB, 512)) {
      owner.offerCommittedRange(first, target(first));
      assertEquals(100, owner.getIndexedFrom());
      assertEquals(100, owner.status().getIndexedFrom());
      assertThrows(IOException.class, () -> owner.pinIndexed(612));
      StateArchiveServingIndexBuildCoordinatorV3.LiveServingIndexer live =
          owner.completeInitialSync(target(first));
      try (PersistentServingKeyIndexGeneration pinned = owner.pinIndexed(612)) {
        live.indexNow(next, target(next));
        assertEquals(612, pinned.getIndexedThrough());
        assertEquals(OptionalLong.empty(), pinned.firstChangeAfter("code", new byte[]{7},
            612, 612));
        assertThrows(IllegalArgumentException.class,
            () -> pinned.firstChangeAfter("code", new byte[]{7}, 99, 612));
        assertThrows(IllegalArgumentException.class,
            () -> pinned.firstChangeAfter("code", new byte[]{7}, 100, 615));
      }
      assertThrows(IOException.class, () -> owner.pinIndexed(616));
      try (PersistentServingKeyIndexGeneration current = owner.pinIndexed(615)) {
        assertEquals(2, StateArchiveIndexDatabase.openReferenceCount(
            root.resolve("serving-index-v3/single-v1/keys"), Engine.ROCKSDB));
        for (int block = 100; block < 615; block++) {
          assertEquals(OptionalLong.of(block + 1),
              current.firstChangeAfter("code", new byte[]{7}, block, 615));
        }
      }
    }
    assertFalse(Files.exists(root.resolve("serving-index-v3/current")));
    assertFalse(Files.exists(root.resolve("serving-index-v3/generations")));
    try (StateArchiveServingIndexBuildCoordinatorV3 reopened =
        new StateArchiveServingIndexBuildCoordinatorV3(root, Engine.ROCKSDB, 512)) {
      assertEquals(100, reopened.getIndexedFrom());
      assertEquals(615, reopened.status().getIndexedThrough());
      reopened.recoverCommittedRange(Collections.emptyList(), target(next), true);
      reopened.completeInitialSync(target(next));
      try (PersistentServingKeyIndexGeneration view = reopened.pinIndexed(615)) {
        assertEquals(515, view.getKeyChangeCount());
      }
    }
  }

  @Test
  public void atomicProgressResolvesFailureBeforeWriteAndLostAcknowledgement() throws Exception {
    for (boolean committed : new boolean[]{false, true}) {
      Path directory = temporaryFolder.newFolder().toPath();
      ServingIndexIncrementalPlan first = plan(101, 1);
      try (MutableIndex index = new MutableIndex(directory, Engine.ROCKSDB)) {
        index.append("first", first, hash(1), () -> { }, () -> { });
        assertThrows(IOException.class, () -> index.append("second", plan(102, 1), hash(2),
            () -> {
              if (!committed) {
                throw new IOException("before batch");
              }
            }, () -> {
              if (committed) {
                throw new IOException("lost acknowledgement");
              }
            }));
        assertThrows(IOException.class, index::pin);
      }
      try (MutableIndex reopened = new MutableIndex(directory, Engine.ROCKSDB)) {
        assertEquals(100, reopened.indexedFrom());
        assertEquals(committed ? 102 : 101, reopened.indexedThrough());
        if (!committed) {
          reopened.append("second", plan(102, 1), hash(2), () -> { }, () -> { });
        }
        try (PersistentServingKeyIndexGeneration view = reopened.pin()) {
          assertEquals(2, view.getKeyChangeCount());
          assertEquals(OptionalLong.of(102), view.firstChangeAfter("code", new byte[]{7},
              101, 102));
        }
      }
    }
  }

  @Test
  public void legacyCatalogIsNotSilentlyConverted() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    Path current = root.resolve("serving-index-v3/current");
    Files.createDirectories(current.getParent());
    Files.write(current, new byte[]{1, 2, 3});
    assertThrows(IOException.class, () ->
        new StateArchiveServingIndexBuildCoordinatorV3(root, Engine.ROCKSDB, 1000));
    assertEquals(3, Files.size(current));
    assertFalse(Files.exists(current.getParent().resolve("single-v1")));
  }

  @Test
  public void oversizedPlanIsRejectedBeforeGroupingAndCommit() throws Exception {
    List<BlockReverseDiff.Entry> entries = new ArrayList<>();
    for (int key = 0; key < 1000; key++) {
      entries.add(new BlockReverseDiff.Entry(hash(key), OldValue.absent()));
    }
    List<BlockReverseDiff> input = new ArrayList<>();
    for (int block = 101; block <= 351; block++) {
      input.add(new BlockReverseDiff(BlockSnapshotMeta.forBlock(block, hash(block),
          hash(block - 1), block * 3000L), Collections.singletonList(
              new BlockReverseDiff.DbGroup("code", entries))));
    }
    IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
        () -> ServingIndexIncrementalPlan.planCommittedDiffs(100, hash(100), input));
    assertEquals("Serving plan input budget exceeded", rejected.getMessage());
  }

  private static ServingIndexIncrementalPlan plan(int first, int count) {
    return ServingIndexIncrementalPlan.planCommittedDiffs(first - 1, hash(first - 1),
        diffs(first, count));
  }

  private static List<BlockReverseDiff> diffs(int first, int count) {
    List<BlockReverseDiff> result = new ArrayList<>();
    for (int block = first; block < first + count; block++) {
      result.add(new BlockReverseDiff(BlockSnapshotMeta.forBlock(block, hash(block),
          hash(block - 1), block * 3000L), Collections.singletonList(
              new BlockReverseDiff.DbGroup("code", Collections.singletonList(
                  new BlockReverseDiff.Entry(new byte[]{7}, OldValue.present(hash(block))))))));
    }
    return result;
  }

  private static CommonCheckpointTarget target(List<BlockReverseDiff> diffs) {
    return CommonCheckpointTarget.restore(hash(70), hash(80), diffs.get(0).getMeta(),
        diffs.get(diffs.size() - 1).getMeta(), hash(90), hash(91));
  }

  private static byte[] hash(int value) {
    return java.nio.ByteBuffer.allocate(32).putInt(value).array();
  }
}
