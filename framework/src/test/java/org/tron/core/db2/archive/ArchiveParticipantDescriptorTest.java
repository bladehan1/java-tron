package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.hash.Hashing;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ArchiveParticipantDescriptorTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void definesExact26WithStableAbiTombstoneAndLegacyAsset() {
    ArchiveParticipantDescriptor descriptor = ArchiveParticipantDescriptor.current();

    assertEquals(26, descriptor.getParticipants().size());
    assertEquals(ArchiveParticipantDescriptor.ABI_STORE_ID,
        descriptor.getStoreId("abi"));
    assertEquals(6, descriptor.getStoreId("asset-issue"));
    assertEquals(7, descriptor.getStoreId("asset-issue-v2"));
    assertEquals("abi", descriptor.getTombstonesByStoreId().get(1));
    assertTrue(descriptor.getParticipants().contains("asset-issue"));
    assertTrue(descriptor.getParticipants().contains("asset-issue-v2"));
    assertFalse(descriptor.getParticipants().contains("abi"));
    assertEquals("archive-state/exact-26-abi-tombstone/v1",
        ArchiveParticipantDescriptor.FORMAT_ID);
  }

  @Test
  public void rejectsOldExact27AndV2OnlyExact25ParticipantSets() {
    ArchiveParticipantDescriptor descriptor = ArchiveParticipantDescriptor.current();
    List<String> exact27 = new ArrayList<>(descriptor.getParticipants());
    exact27.add("abi");
    List<String> exact25 = new ArrayList<>(descriptor.getParticipants());
    exact25.remove("asset-issue");

    assertThrows(ArchivePersistenceException.class,
        () -> descriptor.requireExactParticipants(exact27));
    assertThrows(ArchivePersistenceException.class,
        () -> descriptor.requireExactParticipants(exact25));
    descriptor.requireExactParticipants(descriptor.getParticipants());
  }

  @Test
  public void manifestBindsApprovedScopeAndRejectsLegacyVersion() throws Exception {
    List<String> participants = ArchiveParticipantDescriptor.current().getParticipants();
    Path archive = temporaryFolder.newFolder("exact-26-manifest").toPath();
    ArchiveBaseManifest manifest = new ArchiveBaseManifest(archive, participants);
    manifest.ensureBase(meta(1));

    byte[] encoded = Files.readAllBytes(archive.resolve("MANIFEST"));
    assertEquals(2, ByteBuffer.wrap(encoded, Integer.BYTES, Short.BYTES).getShort());
    new ArchiveBaseManifest(archive, participants);

    List<String> oldExact27 = new ArrayList<>(participants);
    oldExact27.add("abi");
    Collections.sort(oldExact27);
    assertThrows(ArchivePersistenceException.class,
        () -> new ArchiveBaseManifest(archive, oldExact27));

    ByteBuffer.wrap(encoded).putShort(Integer.BYTES, (short) 1);
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    ByteBuffer.wrap(encoded, encoded.length - Integer.BYTES, Integer.BYTES)
        .putInt(Hashing.crc32c().hashBytes(payload).asInt());
    Files.write(archive.resolve("MANIFEST"), encoded);
    assertThrows(ArchivePersistenceException.class,
        () -> new ArchiveBaseManifest(archive, participants));
  }

  private static BlockSnapshotMeta meta(long epoch) {
    return new BlockSnapshotMeta(epoch, epoch, hash((int) epoch),
        hash((int) epoch - 1), epoch * 1_000L);
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }
}
