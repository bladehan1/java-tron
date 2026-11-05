package org.tron.core.db2.archive.serving;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.tron.core.db2.NativeCheckpointTestSupport.engine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.serving.index.ServingIndexIncrementalPlan;

public class ServingRuntimeAccessTest {

  @Rule public final TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void buildsPublishesAndReopensThroughRuntimeBoundary() throws Exception {
    Path root = temporary.getRoot().toPath();
    Path catalogRoot = root.resolve("catalog");
    Path initial = root.resolve("initial");
    build(initial, "g1", plan(0, 1));
    List<ServingRuntimeAccess.PublicationStage> stages = new ArrayList<>();
    try (PersistentServingKeyIndexCatalog catalog =
            ServingRuntimeAccess.createCatalog(catalogRoot, initial, stages::add);
        PersistentServingKeyIndexGeneration current = catalog.pin();
        ServingRuntimeAccess.Builder builder =
            ServingRuntimeAccess.openRuntimeBuilder(current, root.resolve("builder"))) {
      Path shadow = root.resolve("next");
      try (PersistentServingKeyIndexGeneration candidate =
          builder.extendExact(shadow, "g2", plan(1, 2), hash(20))) {
        assertEquals(0, candidate.getIndexedFrom());
        assertEquals(2, candidate.getIndexedThrough());
        assertFalse(candidate.firstChangeAfter("code", new byte[] {1}, 0, 2).isPresent());
      }
      assertTrue(catalog.publish("g1", shadow));
      builder.close();
      assertThrows(IllegalStateException.class, () -> builder.extendExact(
          root.resolve("closed"), "g3", plan(2, 3), hash(20)));
      assertEquals(
          Arrays.asList(
              ServingRuntimeAccess.PublicationStage.GENERATION_INSTALLED,
              ServingRuntimeAccess.PublicationStage.CURRENT_PUBLISHED,
              ServingRuntimeAccess.PublicationStage.GENERATION_INSTALLED,
              ServingRuntimeAccess.PublicationStage.CURRENT_PUBLISHED),
          stages);
    }
    try (PersistentServingKeyIndexCatalog reopened =
            ServingRuntimeAccess.openCatalog(catalogRoot, engine(), stage -> { });
        PersistentServingKeyIndexGeneration pin = reopened.pin()) {
      assertEquals("g2", pin.getGenerationId());
      assertEquals(2, pin.getIndexedThrough());
    }
  }

  @Test
  public void failureAfterGenerationInstallLeavesPublishedCurrentRecoverable() throws Exception {
    Path root = temporary.getRoot().toPath();
    Path catalogRoot = root.resolve("catalog");
    Path initial = root.resolve("initial");
    build(initial, "g1", plan(0, 1));
    try (PersistentServingKeyIndexCatalog ignored =
        ServingRuntimeAccess.createCatalog(catalogRoot, initial, stage -> { })) {
      // Close the first owner before opening one with a publication fault.
    }
    Path next = root.resolve("next");
    build(next, "g2", plan(0, 2));
    try (PersistentServingKeyIndexCatalog faulted =
        ServingRuntimeAccess.openCatalog(
            catalogRoot,
            engine(),
            stage -> {
              if (stage == ServingRuntimeAccess.PublicationStage.GENERATION_INSTALLED) {
                throw new IOException("publication boundary fault");
              }
            })) {
      assertThrows(IOException.class, () -> faulted.publish("g1", next));
    }
    try (PersistentServingKeyIndexCatalog reopened =
            ServingRuntimeAccess.openCatalog(catalogRoot, engine(), stage -> { });
        PersistentServingKeyIndexGeneration pin = reopened.pin()) {
      assertEquals("g1", pin.getGenerationId());
      assertEquals(1, pin.getIndexedThrough());
    }
  }

  private static void build(Path path, String id, ServingIndexIncrementalPlan plan)
      throws IOException {
    try (PersistentServingKeyIndexGeneration ignored =
        PersistentServingKeyIndexGeneration.buildExact(
            path, id, plan, hash(20), engine(), () -> { })) {
      // The runtime opens its own immutable pin from the durable generation.
    }
  }

  private static ServingIndexIncrementalPlan plan(int parent, int head) {
    List<BlockReverseDiff> blocks = new ArrayList<>();
    for (int block = parent + 1; block <= head; block++) {
      blocks.add(
          new BlockReverseDiff(
              BlockSnapshotMeta.forBlock(block, hash(block), hash(block - 1), block * 3000L),
              Collections.emptyList()));
    }
    return ServingIndexIncrementalPlan.planCommittedDiffs(parent, hash(parent), blocks);
  }

  private static byte[] hash(int value) {
    byte[] hash = new byte[32];
    hash[31] = (byte) value;
    return hash;
  }
}
