package org.tron.core.db2.stateroot.storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Cache;
import org.tron.core.config.args.StorageConfig.NativeDbConfig;
import org.tron.core.db2.stateroot.format.PathStateStoreManifest.Engine;

public class PathStateSharedBlockCacheTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void tearDown() {
    PathStateSharedBlockCache.resetForTest();
  }

  @Test
  public void acquireReturnsSameHandleAndTracksCapacity() {
    Cache first = PathStateSharedBlockCache.acquire(64L * 1024 * 1024, -1);
    Cache second = PathStateSharedBlockCache.acquire(64L * 1024 * 1024, -1);
    assertSame(first, second);
    assertTrue(PathStateSharedBlockCache.isActive());
    assertEquals(64L * 1024 * 1024, PathStateSharedBlockCache.capacityBytes());
  }

  @Test
  public void storesShareOneCacheAndStoreCloseKeepsPoolOpen() throws Exception {
    Cache shared = PathStateSharedBlockCache.acquire(96L * 1024 * 1024, -1);
    NativeDbConfig config = NativeDbConfig.small();
    Path firstDir = temporaryFolder.newFolder("store-a").toPath();
    Path secondDir = temporaryFolder.newFolder("store-b").toPath();
    PathStateNativeNodeStore first = PathStateNativeNodeStore.open(firstDir, Engine.ROCKSDB,
        "small", config, -1, false, config.getCacheSize(), shared);
    PathStateNativeNodeStore second = PathStateNativeNodeStore.open(secondDir, Engine.ROCKSDB,
        "small", config, -1, false, config.getCacheSize(), shared);
    first.put("k1".getBytes(UTF_8), "v1".getBytes(UTF_8));
    second.put("k2".getBytes(UTF_8), "v2".getBytes(UTF_8));
    assertArrayEquals("v1".getBytes(UTF_8), first.get("k1".getBytes(UTF_8)));
    assertArrayEquals("v2".getBytes(UTF_8), second.get("k2".getBytes(UTF_8)));
    first.close();
    second.close();
    assertTrue(PathStateSharedBlockCache.isActive());
    assertSame(shared, PathStateSharedBlockCache.acquire(96L * 1024 * 1024, -1));
  }

  @Test
  public void rejectsNonPositiveCapacity() {
    assertThrows(IllegalArgumentException.class, () -> PathStateSharedBlockCache.acquire(0, -1));
  }

  @Test
  public void storeOpensWithoutFilterWhenBloomBitsPerKeyIsZero() throws Exception {
    NativeDbConfig config = NativeDbConfig.small();
    config.setBloomBitsPerKey(0);
    Path dir = temporaryFolder.newFolder("no-filter").toPath();
    PathStateNativeNodeStore store = PathStateNativeNodeStore.open(dir, Engine.ROCKSDB,
        "small", config, -1, false, config.getCacheSize(), null);
    store.put("k".getBytes(UTF_8), "v".getBytes(UTF_8));
    assertArrayEquals("v".getBytes(UTF_8), store.get("k".getBytes(UTF_8)));
    store.close();
    PathStateNativeNodeStore reopened = PathStateNativeNodeStore.open(dir, Engine.ROCKSDB,
        "small", config, -1, false, config.getCacheSize(), null);
    assertArrayEquals("v".getBytes(UTF_8), reopened.get("k".getBytes(UTF_8)));
    reopened.close();
  }
}
