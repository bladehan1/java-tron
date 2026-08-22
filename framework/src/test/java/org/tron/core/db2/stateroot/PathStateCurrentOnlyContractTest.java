package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.arch.Arch;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateCurrentOnlyContractTest {

  private static final Class<?>[] DURABLE_API = new Class<?>[]{
      PathStateBasePublication.class,
      PathStateCurrentStore.class,
      PathStateLayer.class,
      PathStateLayerLimits.class,
      PathStateLayerPublication.class,
      PathStateLayerRetirement.class,
      PathStateNodeStoreSet.class,
      PathStateRootMetadata.class,
      PathStateStoreManifest.class
  };

  private static final String[] FORBIDDEN_API_TOKENS = new String[]{
      "getrootat", "histor", "proof", "segment"
  };

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void durablePublicApiExposesCurrentAuthorityButNoHistoricalService() throws Exception {
    assertEquals(setOf("appendLayer", "current", "isInitialized", "publishBase"),
        publicMethodNames(PathStateCurrentStore.class));
    assertEquals(setOf("close", "commit", "createRoot", "getDirectory", "getProgress",
        "openBase", "openCurrent", "openLayer"),
        publicMethodNames(PathStateNodeStoreSet.class));
    assertEquals(setOf("recover", "switchToAncestor"),
        publicMethodNames(PathStateLayerRetirement.class));
    for (Class<?> type : DURABLE_API) {
      for (Method method : type.getDeclaredMethods()) {
        if (Modifier.isPublic(method.getModifiers())) {
          String name = method.getName().toLowerCase(java.util.Locale.ROOT);
          for (String forbidden : FORBIDDEN_API_TOKENS) {
            assertFalse(type.getSimpleName() + "." + method.getName(),
                name.contains(forbidden));
          }
        }
      }
    }

    assertFalse(Modifier.isPublic(PathStateNodeStoreSet.class.getDeclaredMethod(
        "openPublished", PathStateStoreManifest.class, PathStateRootMetadata.class)
        .getModifiers()));
    assertFalse(Modifier.isPublic(PathStateCurrentStore.class.getDeclaredMethod(
        "layersAboveAncestor", PathStateRootMetadata.class, PathStateLayerLimits.class)
        .getModifiers()));
    assertFalse(Modifier.isPublic(PathStateCurrentStore.class.getDeclaredMethod(
        "switchToAncestor", PathStateRootMetadata.class, PathStateLayerLimits.class)
        .getModifiers()));
  }

  @Test
  public void completedForkLifecycleKeepsExactCurrentOnlyRootLayout() throws Exception {
    for (Engine engine : availableEngines()) {
      PathStateStoreManifest manifest = PathStateStoreManifest.createOrOpen(
          new File(temporaryFolder.getRoot(), "layout-" + engine).toPath(), engine);
      PathStateRootMetadata base = publishBase(manifest);
      PathStateLayerLimits limits = new PathStateLayerLimits(2, Long.MAX_VALUE);
      PathStateRootMetadata firstA = append(manifest, base, 101, 11, limits);
      PathStateRootMetadata secondA = append(manifest, firstA, 102, 13, limits);
      new PathStateLayerRetirement(manifest, limits).switchToAncestor(base);
      PathStateRootMetadata current = append(manifest, base, 101, 21, limits);

      assertEquals(setOf("CURRENT", "MANIFEST", "base", "layers"),
          children(manifest.getDirectory()));
      assertEquals(setOf("METADATA", "nodes"), children(manifest.getBaseDirectory()));
      assertEquals(Collections.singleton(layerDirectory(manifest, current)
              .getFileName().toString()),
          children(manifest.getLayersDirectory()));
      assertEquals(setOf("METADATA", "nodes"), children(
          manifest.getLayerDirectory(current.getBlockNumber(), current.getBlockHash())));
      assertFalse(Files.exists(manifest.getDirectory().resolve("history")));
      assertFalse(Files.exists(manifest.getDirectory().resolve("segments")));
      assertFalse(Files.exists(manifest.getDirectory().resolve("index")));
      assertFalse(Files.exists(manifest.getDirectory().resolve("proofs")));
      assertFalse(Files.exists(layerDirectory(manifest, firstA)));
      assertFalse(Files.exists(layerDirectory(manifest, secondA)));
      assertArrayEquals(current.encode(), new PathStateCurrentStore(manifest).current().encode());
    }
  }

  private static PathStateRootMetadata publishBase(PathStateStoreManifest manifest)
      throws Exception {
    try (PathStateNodeStoreSet stores = PathStateNodeStoreSet.openBase(manifest)) {
      PathStateRoot root = stores.createRoot();
      root.apply(Arrays.asList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{2}),
          PathStateMutation.put("account", new byte[]{3}, new byte[]{4})));
      PathStateRootMetadata base = PathStateRootMetadata.base(100, bytes(1), bytes(2), 300,
          P66Phase.P66_ON, manifest.getIdentityDigest(), root.rootHash(), bytes(3));
      new PathStateBasePublication(manifest).publish(stores, base);
      return base;
    }
  }

  private static PathStateRootMetadata append(PathStateStoreManifest manifest,
      PathStateRootMetadata parent, long blockNumber, int seed, PathStateLayerLimits limits)
      throws Exception {
    try (PathStateLayer layer = PathStateLayer.begin(manifest, parent, blockNumber, bytes(seed),
        parent.getBlockHash(), blockNumber * 3, P66Phase.P66_ON, bytes(seed + 1), limits)) {
      layer.apply(Collections.singletonList(
          PathStateMutation.put("proposal", new byte[]{1}, new byte[]{(byte) seed})));
      return layer.commit();
    }
  }

  private static Set<String> children(Path directory) throws Exception {
    try (Stream<Path> paths = Files.list(directory)) {
      return paths.map(path -> path.getFileName().toString()).collect(
          Collectors.toCollection(TreeSet::new));
    }
  }

  private static Set<String> setOf(String... values) {
    return new TreeSet<>(Arrays.asList(values));
  }

  private static Set<String> publicMethodNames(Class<?> type) {
    return Arrays.stream(type.getDeclaredMethods())
        .filter(method -> Modifier.isPublic(method.getModifiers()))
        .map(Method::getName)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private static Path layerDirectory(PathStateStoreManifest manifest,
      PathStateRootMetadata metadata) {
    return manifest.getLayerDirectory(metadata.getBlockNumber(), metadata.getBlockHash());
  }

  private static Engine[] availableEngines() {
    return Arch.isArm64() ? new Engine[]{Engine.ROCKSDB}
        : new Engine[]{Engine.LEVELDB, Engine.ROCKSDB};
  }

  private static byte[] bytes(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }
}
