package org.tron.core.db2.snapshot;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.tron.common.utils.FileUtil;

public class CatchUpSnapshotAnchorTest {

  private Path root;
  private Path outputDirectory;

  @Before
  public void setUp() throws IOException {
    root = Files.createTempDirectory("catchup-anchor-test");
    outputDirectory = Files.createDirectories(root.resolve("output-directory"));
  }

  @After
  public void tearDown() {
    FileUtil.recursiveDelete(root.toString());
  }

  private CatchUpSnapshotAnchor newAnchor() {
    // Interval 0 keeps the timer permanently due so tests need not wait.
    return CatchUpSnapshotAnchor.createForTest(true, 0, "", outputDirectory);
  }

  private Path anchorParent() {
    return root.resolve("catchup-anchor");
  }

  private Path anchorDir() {
    return anchorParent().resolve(CatchUpSnapshotAnchor.ANCHOR_DIR);
  }

  private void writeOutput(String name, String content) throws IOException {
    Path target = outputDirectory.resolve(name);
    Files.createDirectories(target.getParent());
    Files.write(target, content.getBytes(StandardCharsets.UTF_8));
  }

  private String readAnchor(String name) throws IOException {
    return new String(Files.readAllBytes(anchorDir().resolve(name)), StandardCharsets.UTF_8);
  }

  /** Rewrites the marker's utc line into the past so a fresh instance sees the timer as due. */
  private void ageMarkerUtc() throws IOException {
    Path marker = anchorDir().resolve(CatchUpSnapshotAnchor.MARKER);
    String content = new String(Files.readAllBytes(marker), StandardCharsets.UTF_8);
    String aged = content.replaceFirst("utc=.*", "utc=2000-01-01T00:00:00Z");
    Files.write(marker, aged.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void disabledAnchorDoesNothing() throws IOException {
    CatchUpSnapshotAnchor anchor =
        CatchUpSnapshotAnchor.createForTest(false, 5, "", outputDirectory);
    writeOutput("a.txt", "live");
    anchor.maybeAnchor(100);
    Assert.assertFalse(Files.exists(anchorParent()));
    Assert.assertEquals(-1, anchor.restoreIfUnclean());
    anchor.markRunning();
    anchor.markCleanShutdown();
    Assert.assertEquals("live", new String(Files.readAllBytes(outputDirectory.resolve("a.txt")),
        StandardCharsets.UTF_8));
  }

  @Test
  public void anchorCreatedAndRefreshedByTimer() throws IOException {
    CatchUpSnapshotAnchor anchor = newAnchor();
    writeOutput("a.txt", "v1");
    anchor.maybeAnchor(100);
    Assert.assertTrue(Files.isRegularFile(anchorDir().resolve(CatchUpSnapshotAnchor.MARKER)));
    Assert.assertEquals("v1", readAnchor("a.txt"));

    // Fresh instance, timer not yet due (5-minute interval, marker just written): no re-anchor.
    writeOutput("a.txt", "v2");
    CatchUpSnapshotAnchor fresh =
        CatchUpSnapshotAnchor.createForTest(true, 5, "", outputDirectory);
    fresh.maybeAnchor(105);
    Assert.assertEquals("v1", readAnchor("a.txt"));

    // Age the marker past the interval: re-anchor; tmp/old leftovers are gone.
    ageMarkerUtc();
    CatchUpSnapshotAnchor aged =
        CatchUpSnapshotAnchor.createForTest(true, 5, "", outputDirectory);
    aged.maybeAnchor(120);
    Assert.assertEquals("v2", readAnchor("a.txt"));
    Assert.assertFalse(Files.exists(anchorParent().resolve(CatchUpSnapshotAnchor.TMP_DIR)));
    Assert.assertFalse(Files.exists(anchorParent().resolve(CatchUpSnapshotAnchor.OLD_DIR)));
  }

  @Test
  public void hybridCopyLinksImmutableFilesAndCopiesMutableOnes() throws IOException {
    writeOutput("00001.sst", "sst-a");
    writeOutput("00061.log", "log-a");
    writeOutput("sub/00002.sst", "sst-b");
    CatchUpSnapshotAnchor anchor = CatchUpSnapshotAnchor.createForTest(true, 0, "",
        outputDirectory, CatchUpSnapshotAnchor.CopyMode.HYBRID_LINK);
    anchor.maybeAnchor(100);

    Assert.assertTrue(Files.isSameFile(outputDirectory.resolve("00001.sst"),
        anchorDir().resolve("00001.sst")));
    Assert.assertTrue(Files.isSameFile(outputDirectory.resolve("sub/00002.sst"),
        anchorDir().resolve("sub/00002.sst")));
    Assert.assertFalse(Files.isSameFile(outputDirectory.resolve("00061.log"),
        anchorDir().resolve("00061.log")));
    Assert.assertEquals("log-a", readAnchor("00061.log"));
  }

  @Test
  public void restoreReplacesTornState() throws IOException {
    CatchUpSnapshotAnchor anchor = newAnchor();
    writeOutput("a.txt", "anchored");
    writeOutput("sub/b.txt", "b");
    anchor.maybeAnchor(100);
    long anchorHeight = 100;

    // Simulate an unclean stop: RUNNING left behind, live state moved ahead and torn.
    anchor.markRunning();
    writeOutput("a.txt", "torn");
    Files.delete(outputDirectory.resolve("sub/b.txt"));

    Assert.assertEquals(anchorHeight, anchor.restoreIfUnclean());
    Assert.assertEquals("anchored", new String(
        Files.readAllBytes(outputDirectory.resolve("a.txt")), StandardCharsets.UTF_8));
    Assert.assertEquals("b", new String(
        Files.readAllBytes(outputDirectory.resolve("sub/b.txt")), StandardCharsets.UTF_8));
    Assert.assertFalse(Files.exists(anchorParent().resolve(CatchUpSnapshotAnchor.RUNNING)));
  }

  @Test
  public void uncleanWithoutAnchorKeepsLiveState() throws IOException {
    CatchUpSnapshotAnchor anchor = newAnchor();
    writeOutput("a.txt", "live");
    anchor.markRunning();
    Assert.assertEquals(-1, anchor.restoreIfUnclean());
    Assert.assertEquals("live", new String(
        Files.readAllBytes(outputDirectory.resolve("a.txt")), StandardCharsets.UTF_8));
    Assert.assertFalse(Files.exists(anchorParent().resolve(CatchUpSnapshotAnchor.RUNNING)));
  }

  @Test
  public void cleanShutdownSkipsRestore() throws IOException {
    CatchUpSnapshotAnchor anchor = newAnchor();
    writeOutput("a.txt", "v1");
    anchor.maybeAnchor(100);
    anchor.markRunning();
    anchor.markCleanShutdown();
    writeOutput("a.txt", "newer");
    Assert.assertEquals(-1, anchor.restoreIfUnclean());
    Assert.assertEquals("newer", new String(
        Files.readAllBytes(outputDirectory.resolve("a.txt")), StandardCharsets.UTF_8));
  }

  @Test(expected = IllegalArgumentException.class)
  public void anchorInsideOutputDirectoryRejected() {
    CatchUpSnapshotAnchor.createForTest(true, 5, outputDirectory.toString(),
        outputDirectory);
  }

  @Test
  public void markerHeightReadable() throws IOException {
    CatchUpSnapshotAnchor anchor = newAnchor();
    writeOutput("a.txt", "v1");
    anchor.maybeAnchor(42);
    Path marker = anchorDir().resolve(CatchUpSnapshotAnchor.MARKER);
    String content = new String(Files.readAllBytes(marker), StandardCharsets.UTF_8);
    Assert.assertTrue(content.contains("height=42"));
  }
}
