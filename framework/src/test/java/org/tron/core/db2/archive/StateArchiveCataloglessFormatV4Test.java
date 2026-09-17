package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import org.junit.Test;
import org.tron.core.db2.archive.StateArchiveCataloglessFormatV4.FileDescriptor;

public class StateArchiveCataloglessFormatV4Test {

  @Test
  public void roundTripsFixedFileDescriptorAndRejectsDrift() {
    FileDescriptor input = new FileDescriptor(7, 0, 100, 102, 8_192, 3, 2, 19);
    byte[] encoded = StateArchiveCataloglessFormatV4.encodeFileDescriptor(input);
    assertEquals(64, encoded.length);
    assertEquals(7, Integer.toUnsignedLong(ByteBuffer.wrap(encoded).getInt(0)));
    assertEquals(102, ByteBuffer.wrap(encoded).getLong(16));
    assertEquals(3, ByteBuffer.wrap(encoded).getLong(32));

    FileDescriptor decoded = StateArchiveCataloglessFormatV4.decodeFileDescriptor(encoded);
    assertEquals(input.getFileId(), decoded.getFileId());
    assertEquals(input.getFirstRecordBlockNumber(), decoded.getFirstRecordBlockNumber());
    assertEquals(input.getEndBlockNumber(), decoded.getEndBlockNumber());
    assertEquals(input.getDataFileBytes(), decoded.getDataFileBytes());
    assertEquals(input.getRecordCount(), decoded.getRecordCount());
    assertEquals(input.getChangedFrameCount(), decoded.getChangedFrameCount());
    assertEquals(input.getEntryCount(), decoded.getEntryCount());
    assertArrayEquals(encoded, StateArchiveCataloglessFormatV4.encodeFileDescriptor(decoded));

    for (int offset : new int[]{0, 8, 24, 40, 55, 56, 63}) {
      byte[] corrupt = encoded.clone();
      corrupt[offset] ^= 1;
      assertThrows(IllegalArgumentException.class,
          () -> StateArchiveCataloglessFormatV4.decodeFileDescriptor(corrupt));
    }
  }

  @Test
  public void rejectsInvalidFileDescriptorInvariants() {
    assertThrows(IllegalArgumentException.class,
        () -> new FileDescriptor(0x1_0000_0000L, 0, 1, 1, 512, 1, 0, 0));
    assertThrows(IllegalArgumentException.class,
        () -> new FileDescriptor(0, 1, 1, 1, 512, 1, 0, 0));
    assertThrows(IllegalArgumentException.class,
        () -> new FileDescriptor(0, 0, 1, 2, 1_024, 1, 0, 0));
    assertThrows(IllegalArgumentException.class,
        () -> new FileDescriptor(0, 0, 1, 1, 512, 1, 2, 0));
  }
}
