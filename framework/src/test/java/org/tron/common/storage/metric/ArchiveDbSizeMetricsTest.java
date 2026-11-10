package org.tron.common.storage.metric;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.Test;

public class ArchiveDbSizeMetricsTest {

  @Test
  public void directorySizeSumsNestedFiles() throws Exception {
    Path root = Files.createTempDirectory("archive-size-test");
    try {
      Files.write(root.resolve("a.sst"), new byte[10]);
      Path nested = Files.createDirectories(root.resolve("nested"));
      Files.write(nested.resolve("b.sst"), new byte[7]);
      assertEquals(17L, ArchiveDbSizeMetrics.directorySize(root));
    } finally {
      deleteRecursively(root);
    }
  }

  @Test
  public void directorySizeEmptyDirectoryIsZero() throws Exception {
    Path root = Files.createTempDirectory("archive-size-test");
    try {
      assertEquals(0L, ArchiveDbSizeMetrics.directorySize(root));
    } finally {
      deleteRecursively(root);
    }
  }

  @Test
  public void reportSkipsMissingDirectory() {
    Path missing = java.nio.file.Paths.get("definitely-not-existing-archive-db");
    ArchiveDbSizeMetrics.report(missing, "ROCKSDB", "missing");
  }

  private static void deleteRecursively(Path root) throws IOException {
    try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }
}
