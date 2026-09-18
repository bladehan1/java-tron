package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.StateArchiveCataloglessFormatV4.FileDescriptor;

public class StateArchiveFilesMetaV4Test {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void appendsReopensAndReadsOnlyCommittedPrefix() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    long firstCommittedLength;
    try (StateArchiveFilesMetaV4 meta = StateArchiveFilesMetaV4.openOrCreate(root, 4)) {
      assertEquals(4, meta.getLaneId());
      meta.append(descriptor(0, 100, 102));
      firstCommittedLength = meta.committedLength();
      meta.append(descriptor(1, 103, 104));
      meta.force();
      assertEquals(2, meta.snapshot().size());
      assertEquals(1, meta.readCommitted(firstCommittedLength).size());
    }

    try (StateArchiveFilesMetaV4 reopened = StateArchiveFilesMetaV4.openOrCreate(root, 4)) {
      assertEquals(2, reopened.snapshot().size());
      assertEquals(104, reopened.snapshot().get(1).getEndBlockNumber());
      assertThrows(IllegalArgumentException.class,
          () -> reopened.readCommitted(firstCommittedLength + 1));
    }
  }

  @Test
  public void rejectsDiscontinuousAndCorruptMetadata() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    Path file;
    try (StateArchiveFilesMetaV4 meta = StateArchiveFilesMetaV4.openOrCreate(root, 0)) {
      meta.append(descriptor(0, 10, 10));
      assertThrows(IllegalArgumentException.class,
          () -> meta.append(descriptor(2, 11, 11)));
      assertThrows(IllegalArgumentException.class,
          () -> meta.append(descriptor(1, 12, 12)));
      meta.force();
      file = root.resolve("lane-0000.files.meta");
    }

    byte[] bytes = Files.readAllBytes(file);
    bytes[bytes.length - 1] ^= 1;
    Files.write(file, bytes);
    assertThrows(IllegalArgumentException.class,
        () -> StateArchiveFilesMetaV4.openOrCreate(root, 0));
  }

  @Test
  public void rejectsPartialPhysicalTailAndWrongLane() throws Exception {
    Path root = temporaryFolder.newFolder().toPath();
    Path file;
    try (StateArchiveFilesMetaV4 meta = StateArchiveFilesMetaV4.openOrCreate(root, 22)) {
      meta.append(descriptor(0, 1, 1));
      meta.force();
      file = root.resolve("lane-0022.files.meta");
    }
    Files.write(file, new byte[]{1}, java.nio.file.StandardOpenOption.APPEND);
    assertThrows(IllegalArgumentException.class,
        () -> StateArchiveFilesMetaV4.openOrCreate(root, 22));
    assertThrows(IllegalArgumentException.class,
        () -> StateArchiveFilesMetaV4.openOrCreate(root, 6));
  }

  private static FileDescriptor descriptor(long fileId, long first, long end) {
    long count = end - first + 1;
    return new FileDescriptor(fileId, 0, first, end,
        StateArchiveFileFormatV3.PART_HEADER_LENGTH + count
            * (StateArchiveFileFormatV3.BLOCK_HEADER_LENGTH
            + StateArchiveFileFormatV3.PAYLOAD_HEADER_LENGTH
            + StateArchiveFileFormatV3.FRAME_TRAILER_LENGTH), count, 0, 0);
  }
}
