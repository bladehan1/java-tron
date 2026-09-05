package org.tron.core.db2.stateroot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.hash.Hashing;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.utils.ByteArray;
import org.tron.core.db2.stateroot.PathStateCanonicalizer.P66Phase;
import org.tron.core.db2.stateroot.PathStateRootMetadata.Kind;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

public class PathStatePersistentFormatTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void manifestCreatesIndependentCurrentOnlyLayoutAndReopensWithoutRewrite()
      throws Exception {
    Path root = new File(temporaryFolder.getRoot(), "path-state-root").toPath();
    PathStateStoreManifest created = PathStateStoreManifest.createOrOpen(root, Engine.LEVELDB);
    Path manifest = root.resolve(PathStateStoreManifest.MANIFEST_FILE);
    byte[] original = Files.readAllBytes(manifest);

    PathStateStoreManifest reopened = PathStateStoreManifest.createOrOpen(root, Engine.LEVELDB);

    assertEquals(root.toAbsolutePath(), created.getDirectory());
    assertEquals(Engine.LEVELDB, reopened.getEngine());
    assertTrue(Files.isDirectory(created.getBaseDirectory()));
    assertTrue(Files.isDirectory(created.getLayersDirectory()));
    assertArrayEquals(original, Files.readAllBytes(manifest));
    assertEquals(
        "37fc0b69dae958872a3088ee060353e370c4ad7b9ff1804e5e151b47da1efa20",
        ByteArray.toHexString(Hashing.sha256().hashBytes(original).asBytes()));
    assertFalse(Files.exists(root.resolve("history")));
  }

  @Test
  public void manifestRejectsEngineDriftAndCorruptionWithoutRepair() throws Exception {
    Path root = temporaryFolder.newFolder("manifest-fail-closed").toPath();
    PathStateStoreManifest.createOrOpen(root, Engine.LEVELDB);
    Path manifest = root.resolve(PathStateStoreManifest.MANIFEST_FILE);
    byte[] expected = Files.readAllBytes(manifest);

    assertThrows(IOException.class,
        () -> PathStateStoreManifest.createOrOpen(root, Engine.ROCKSDB));
    assertArrayEquals(expected, Files.readAllBytes(manifest));

    byte[] corrupt = Arrays.copyOf(expected, expected.length);
    corrupt[corrupt.length - 1] ^= 1;
    Files.write(manifest, corrupt);
    assertThrows(IOException.class,
        () -> PathStateStoreManifest.validateExisting(root, Engine.LEVELDB));
    assertArrayEquals(corrupt, Files.readAllBytes(manifest));
  }

  @Test
  public void validateExistingHasNoCreationSideEffects() {
    Path absent = new File(temporaryFolder.getRoot(), "absent").toPath();

    assertThrows(IOException.class,
        () -> PathStateStoreManifest.validateExisting(absent, Engine.LEVELDB));
    assertFalse(Files.exists(absent));
  }

  @Test
  public void baseMetadataRoundTripsWithoutInventingAParentRoot() {
    PathStateRootMetadata base = PathStateRootMetadata.base(100, bytes(1), bytes(33), 9000,
        P66Phase.P66_ACTIVATION, bytes(17), bytes(65), bytes(97));

    PathStateRootMetadata decoded = PathStateRootMetadata.decode(base.encode());

    assertEquals(Kind.BASE, decoded.getKind());
    assertEquals(100, decoded.getBlockNumber());
    assertEquals(P66Phase.P66_ACTIVATION, decoded.getPhase());
    assertNull(decoded.getParentStateRoot());
    assertArrayEquals(bytes(65), decoded.getStateRoot());
    assertEquals(
        "ab8833ebb7c43812cd6f041aae7796da3c97c51ae3c8890636d2d6b45a471e0d",
        ByteArray.toHexString(Hashing.sha256().hashBytes(base.encode()).asBytes()));
  }

  @Test
  public void layerMetadataBindsParentRootAndTransitionDigest() {
    PathStateRootMetadata layer = PathStateRootMetadata.layer(101, bytes(2), bytes(1), 12000,
        P66Phase.P66_ON, bytes(17), bytes(65), bytes(66), bytes(98));

    PathStateRootMetadata decoded = PathStateRootMetadata.decode(layer.encode());

    assertEquals(Kind.LAYER, decoded.getKind());
    assertArrayEquals(bytes(65), decoded.getParentStateRoot());
    assertArrayEquals(bytes(66), decoded.getStateRoot());
    assertArrayEquals(bytes(98), decoded.getPayloadDigest());
    assertNotNull(decoded.getBlockHash());
  }

  @Test
  public void metadataRejectsKindAmbiguityAndCorruption() {
    assertThrows(IllegalArgumentException.class,
        () -> PathStateRootMetadata.layer(1, bytes(1), bytes(2), 3, P66Phase.P66_ON,
            bytes(17), null, bytes(3), bytes(4)));
    byte[] encoded = PathStateRootMetadata.base(1, bytes(1), bytes(2), 3, P66Phase.P66_OFF,
        bytes(17), bytes(3), bytes(4)).encode();
    encoded[encoded.length - 1] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> PathStateRootMetadata.decode(encoded));
  }

  @Test
  public void metadataOwnsAllByteArrays() {
    byte[] format = bytes(17);
    byte[] root = bytes(65);
    PathStateRootMetadata metadata = PathStateRootMetadata.base(1, bytes(1), bytes(2), 3,
        P66Phase.P66_ON, format, root, bytes(97));
    format[0] = 0;
    root[0] = 0;
    byte[] returnedFormat = metadata.getFormatDigest();
    byte[] returned = metadata.getStateRoot();
    returnedFormat[0] = 0;
    returned[0] = 0;

    assertArrayEquals(bytes(17), metadata.getFormatDigest());
    assertArrayEquals(bytes(65), metadata.getStateRoot());
  }

  private static byte[] bytes(int seed) {
    byte[] value = new byte[32];
    for (int i = 0; i < value.length; i++) {
      value[i] = (byte) (seed + i);
    }
    return value;
  }
}
