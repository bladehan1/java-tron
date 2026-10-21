package org.tron.core.db2.archive;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

/** Directory creation whose new entries are durable before the call returns (ch02 §3.14). */
final class ArchiveDirectories {

  private ArchiveDirectories() {
  }

  /**
   * Creates {@code directory} and any missing ancestors; after each new level is created its
   * parent directory is fsynced, so the new entry survives power loss (I18).
   *
   * @return true when at least one directory was created
   */
  static boolean createDurably(Path directory) throws IOException {
    Path admitted = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
    Deque<Path> missing = new ArrayDeque<>();
    Path current = admitted;
    while (current != null && !Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
      missing.push(current);
      current = current.getParent();
    }
    if (current != null && !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("State Archive path is not a directory: " + current);
    }
    boolean created = false;
    while (!missing.isEmpty()) {
      Path next = missing.pop();
      try {
        Files.createDirectory(next);
        created = true;
      } catch (FileAlreadyExistsException raced) {
        if (!Files.isDirectory(next, LinkOption.NOFOLLOW_LINKS)) {
          throw raced;
        }
      }
      if (next.getParent() != null) {
        syncDirectory(next.getParent());
      }
    }
    return created;
  }

  /** fsyncs one directory so entries created or removed in it are durable (I18). */
  static void syncDirectory(Path directory) throws IOException {
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }
}
