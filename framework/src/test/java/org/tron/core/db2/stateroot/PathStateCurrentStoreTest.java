package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateRootMetadata.Kind;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStateCurrentStoreTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void publishesBaseAndLayersThenVerifiesCurrentAfterReopen() throws Exception {
    PathStateStoreManifest manifest = manifest("normal-chain");
    PathStateCurrentStore store = new PathStateCurrentStore(manifest);
    assertFalse(store.isInitialized());

    PathStateRootMetadata base = base(manifest, 100);
    PathStateRootMetadata first = layer(manifest, 101, base, 11);
    PathStateRootMetadata second = layer(manifest, 102, first, 12);
    store.publishBase(base);
    store.appendLayer(first);
    store.appendLayer(second);

    PathStateCurrentStore reopened = new PathStateCurrentStore(
        PathStateStoreManifest.validateExisting(manifest.getDirectory(), Engine.LEVELDB));
    PathStateRootMetadata current = reopened.current();
    assertTrue(reopened.isInitialized());
    assertEquals(102, current.getBlockNumber());
    assertEquals(Kind.LAYER, current.getKind());
    assertArrayEquals(second.getStateRoot(), current.getStateRoot());
    assertEquals(2, childDirectoryCount(manifest.getLayersDirectory()));
  }

  @Test
  public void exactPublicationRetriesAreIdempotent() throws Exception {
    PathStateStoreManifest manifest = manifest("idempotent");
    PathStateCurrentStore store = new PathStateCurrentStore(manifest);
    PathStateRootMetadata base = base(manifest, 100);
    PathStateRootMetadata child = layer(manifest, 101, base, 11);

    store.publishBase(base);
    store.publishBase(base);
    store.appendLayer(child);
    store.appendLayer(child);

    assertEquals(101, store.current().getBlockNumber());
    assertEquals(1, childDirectoryCount(manifest.getLayersDirectory()));
  }

  @Test
  public void rejectsLayerThatDoesNotExtendCurrentBeforePublication() throws Exception {
    PathStateStoreManifest manifest = manifest("discontinuous");
    PathStateCurrentStore store = new PathStateCurrentStore(manifest);
    PathStateRootMetadata base = base(manifest, 100);
    store.publishBase(base);
    PathStateRootMetadata wrongParent = PathStateRootMetadata.layer(101, hash(101), hash(99),
        303, P66Phase.P66_ON, manifest.getIdentityDigest(), root(100), root(101), digest(101));

    assertThrows(IOException.class, () -> store.appendLayer(wrongParent));
    assertEquals(100, store.current().getBlockNumber());
    assertEquals(0, childDirectoryCount(manifest.getLayersDirectory()));
  }

  @Test
  public void rejectsMetadataFromAnotherManifestIdentity() throws Exception {
    PathStateStoreManifest manifest = manifest("foreign-format");
    PathStateCurrentStore store = new PathStateCurrentStore(manifest);
    PathStateRootMetadata foreign = PathStateRootMetadata.base(100, hash(100), hash(99), 300,
        P66Phase.P66_ON, digest(17), root(100), digest(100));

    assertThrows(IOException.class, () -> store.publishBase(foreign));
    assertFalse(store.isInitialized());
  }

  @Test
  public void startupFailsClosedWhenMiddleLayerIsMissing() throws Exception {
    PathStateStoreManifest manifest = manifest("missing-middle");
    PathStateCurrentStore store = new PathStateCurrentStore(manifest);
    PathStateRootMetadata base = base(manifest, 100);
    PathStateRootMetadata first = layer(manifest, 101, base, 11);
    PathStateRootMetadata second = layer(manifest, 102, first, 12);
    store.publishBase(base);
    store.appendLayer(first);
    store.appendLayer(second);

    Path middle = onlyMetadataForHeight(manifest.getLayersDirectory(), 101);
    Files.delete(middle);

    assertThrows(IOException.class, store::current);
  }

  @Test
  public void immutableMetadataRejectsDifferentRetryWithoutRewrite() throws Exception {
    Path path = new File(temporaryFolder.getRoot(), "immutable/METADATA").toPath();
    PathStateRootMetadata original = rawBase(100);
    PathStateMetadataFile.publishImmutable(path, original);
    byte[] encoded = Files.readAllBytes(path);

    assertThrows(IOException.class,
        () -> PathStateMetadataFile.publishImmutable(path, rawBase(101)));
    assertArrayEquals(encoded, Files.readAllBytes(path));
  }

  @Test
  public void failedCurrentReplacementPreservesPreviousAuthorityAndCleansTemporary()
      throws Exception {
    PathStateStoreManifest manifest = manifest("current-fault");
    PathStateCurrentStore store = new PathStateCurrentStore(manifest);
    PathStateRootMetadata base = base(manifest, 100);
    store.publishBase(base);
    Path current = manifest.getDirectory().resolve(PathStateCurrentStore.CURRENT_FILE);
    byte[] previous = Files.readAllBytes(current);
    PathStateRootMetadata child = layer(manifest, 101, base, 11);

    assertThrows(IOException.class, () -> PathStateMetadataFile.replaceCurrent(current, child,
        temporary -> {
          assertTrue(Files.exists(temporary));
          throw new IOException("injected after temporary force");
        }));

    assertArrayEquals(previous, Files.readAllBytes(current));
    try (Stream<Path> paths = Files.list(manifest.getDirectory())) {
      assertFalse(paths.anyMatch(path -> path.getFileName().toString().startsWith(".CURRENT-")));
    }
    assertEquals(100, store.current().getBlockNumber());
  }

  private PathStateStoreManifest manifest(String name) throws Exception {
    Path root = temporaryFolder.newFolder(name).toPath();
    return PathStateStoreManifest.createOrOpen(root, Engine.LEVELDB);
  }

  private static PathStateRootMetadata base(PathStateStoreManifest manifest, long blockNumber) {
    return PathStateRootMetadata.base(blockNumber, hash(blockNumber), hash(blockNumber - 1),
        blockNumber * 3, P66Phase.P66_ON, manifest.getIdentityDigest(), root(blockNumber),
        digest(blockNumber));
  }

  private static PathStateRootMetadata layer(PathStateStoreManifest manifest, long blockNumber,
      PathStateRootMetadata parent, int salt) {
    return PathStateRootMetadata.layer(blockNumber, hash(blockNumber), parent.getBlockHash(),
        blockNumber * 3, P66Phase.P66_ON, manifest.getIdentityDigest(), parent.getStateRoot(),
        root(blockNumber), digest(salt));
  }

  private static PathStateRootMetadata rawBase(long blockNumber) {
    return PathStateRootMetadata.base(blockNumber, hash(blockNumber), hash(blockNumber - 1),
        blockNumber * 3, P66Phase.P66_ON, digest(17), root(blockNumber), digest(blockNumber));
  }

  private static Path onlyMetadataForHeight(Path layers, long blockNumber) throws Exception {
    String prefix = String.format(Locale.ROOT, "%020d-", blockNumber);
    try (Stream<Path> paths = Files.list(layers)) {
      Path directory = paths.filter(path -> path.getFileName().toString().startsWith(prefix))
          .findFirst()
          .orElseThrow(() -> new AssertionError("layer directory not found"));
      return directory.resolve(PathStateCurrentStore.METADATA_FILE);
    }
  }

  private static long childDirectoryCount(Path path) throws Exception {
    try (Stream<Path> paths = Files.list(path)) {
      return paths.filter(Files::isDirectory).count();
    }
  }

  private static byte[] hash(long value) {
    return bytes((int) value);
  }

  private static byte[] root(long value) {
    return bytes((int) value + 64);
  }

  private static byte[] digest(long value) {
    return bytes((int) value + 96);
  }

  private static byte[] bytes(int seed) {
    byte[] value = new byte[32];
    for (int i = 0; i < value.length; i++) {
      value[i] = (byte) (seed + i);
    }
    return value;
  }
}
