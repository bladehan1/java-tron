package org.tron.core.db2.archive;

import com.google.common.hash.Hashing;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/** Atomic identity which makes a synthetic empty first H a non-queryable bootstrap anchor. */
final class ArchiveBootstrapAnchor {

  private static final int MAGIC = 0x54414241; // TABA
  private static final short VERSION = 1;
  private static final int HEADER_LENGTH = 16;
  private static final int MAX_LENGTH = 1024 * 1024;
  private static final String FILE_NAME = "bootstrap.anchor";

  private ArchiveBootstrapAnchor() {
  }

  static void store(Path archiveDirectory, HistoryCommitMarker marker, List<String> stores)
      throws IOException {
    HistoryCommitMarkerCodec markerCodec = new HistoryCommitMarkerCodec();
    byte[] markerBytes = markerCodec.encode(marker);
    requireStoreScope(marker, stores);
    byte[] encoded = encode(markerBytes);
    Files.createDirectories(archiveDirectory);
    Path temporary = archiveDirectory.resolve("." + FILE_NAME + "-" + UUID.randomUUID());
    try {
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE)) {
        writeFully(channel, ByteBuffer.wrap(encoded));
        channel.force(true);
      }
      try {
        Files.move(temporary, archiveDirectory.resolve(FILE_NAME),
            StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException unsupported) {
        throw new ArchivePersistenceException(
            "Archive filesystem does not support atomic bootstrap anchor replacement",
            unsupported);
      }
      HistorySegmentStore.syncDirectory(archiveDirectory);
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  static HistoryCommitMarker loadAndValidateIfPresent(Path archiveDirectory,
      CommittedHistoryAuthority history, List<String> stores) throws IOException {
    Path path = archiveDirectory.resolve(FILE_NAME);
    if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
      return null;
    }
    if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
      throw new ArchivePersistenceException("Archive bootstrap anchor is not a regular file");
    }
    HistoryCommitMarkerCodec markerCodec = new HistoryCommitMarkerCodec();
    HistoryCommitMarker anchor;
    try {
      anchor = markerCodec.decode(decode(Files.readAllBytes(path)));
    } catch (IllegalArgumentException invalid) {
      throw new ArchivePersistenceException("Archive bootstrap anchor is corrupt", invalid);
    }
    HistoryCommitMarker first = history.get(history.firstEpoch());
    if (first == null) {
      throw new ArchivePersistenceException("Archive bootstrap anchor has no history marker");
    }
    requireStoreScope(anchor, stores);
    if (!Arrays.equals(markerCodec.encode(anchor), markerCodec.encode(first))) {
      throw new ArchivePersistenceException(
          "Archive bootstrap anchor does not match the first history marker");
    }
    if (history instanceof ArchiveHistoryWriter) {
      BlockReverseDiff diff = ((ArchiveHistoryWriter) history)
          .readCommitted(first.getMeta().getEpoch());
      if (!diff.getGroups().isEmpty()) {
        throw new ArchivePersistenceException("Archive bootstrap anchor history is not empty");
      }
    }
    return first;
  }

  private static byte[] encode(byte[] markerBytes) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      output.writeInt(MAGIC);
      output.writeShort(VERSION);
      output.writeShort(0);
      output.writeInt(0);
      output.writeInt(markerBytes.length);
      output.write(markerBytes);
      output.flush();
      byte[] payload = bytes.toByteArray();
      int length = payload.length + Integer.BYTES;
      if (length > MAX_LENGTH) {
        throw new IllegalArgumentException("Archive bootstrap anchor is too large");
      }
      ByteBuffer.wrap(payload).putInt(8, length);
      bytes.reset();
      output = new DataOutputStream(bytes);
      output.write(payload);
      output.writeInt(Hashing.crc32c().hashBytes(payload).asInt());
      output.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new IllegalStateException("Unexpected bootstrap anchor encoding failure", impossible);
    }
  }

  private static byte[] decode(byte[] encoded) {
    if (encoded == null || encoded.length < HEADER_LENGTH + Integer.BYTES
        || encoded.length > MAX_LENGTH) {
      throw new IllegalArgumentException("Archive bootstrap anchor length is invalid");
    }
    byte[] payload = Arrays.copyOf(encoded, encoded.length - Integer.BYTES);
    int checksum = ByteBuffer.wrap(encoded, payload.length, Integer.BYTES).getInt();
    if (checksum != Hashing.crc32c().hashBytes(payload).asInt()) {
      throw new IllegalArgumentException("Archive bootstrap anchor checksum mismatch");
    }
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(encoded))) {
      if (input.readInt() != MAGIC || input.readShort() != VERSION || input.readShort() != 0
          || input.readInt() != encoded.length) {
        throw new IllegalArgumentException("Unsupported archive bootstrap anchor header");
      }
      int markerLength = input.readInt();
      if (markerLength <= 0 || markerLength != input.available() - Integer.BYTES) {
        throw new IllegalArgumentException("Archive bootstrap anchor marker length is invalid");
      }
      byte[] marker = new byte[markerLength];
      input.readFully(marker);
      if (input.available() != Integer.BYTES) {
        throw new IllegalArgumentException("Archive bootstrap anchor payload mismatch");
      }
      return marker;
    } catch (IOException invalid) {
      throw new IllegalArgumentException("Archive bootstrap anchor is truncated", invalid);
    }
  }

  private static void requireStoreScope(HistoryCommitMarker marker, List<String> stores) {
    List<String> expected = new ArrayList<>(stores);
    Collections.sort(expected);
    if (!marker.getDatabases().equals(expected)) {
      throw new ArchivePersistenceException("Archive bootstrap anchor Store scope mismatch");
    }
  }

  private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }
}
