package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.config.args.Args;
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

/** Actual JVM halt with an open native redo database, rather than a clean-close simulation. */
public class SharedCheckpointKillProcessTest {
  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  @Test
  public void committedAndAppliedGenerationsSurviveJvmHalt() throws Exception {
    for (String stage : new String[]{"commit", "applied"}) {
      Path root = temporary.newFolder(stage).toPath();
      runChild(stage, root);
      try (SharedCheckpointDatabase database = new SharedCheckpointDatabase(root, Engine.LEVELDB)) {
        assertEquals(2, database.retained().size());
        assertEquals("commit".equals(stage), database.isPending());
        assertEquals(2, database.latest().getBlocks().get(0).getMeta().getBlockNumber());
        database.applied();
      }
    }
  }

  private static void runChild(String stage, Path root) throws Exception {
    Process child = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java")
        .toString(), "-cp", runtimeClasspath(), SharedCheckpointKillProcessTest.class.getName(),
        stage, root.toString()).redirectErrorStream(true)
        .redirectOutput(root.getParent().resolve(stage + ".log").toFile()).start();
    try {
      assertTrue("native database child timed out", child.waitFor(60, TimeUnit.SECONDS));
      assertEquals(92, child.exitValue());
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        child.waitFor();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Args.setParam(new String[]{"--output-directory", args[1] + "-config"}, "config-test.conf");
    SharedCheckpointDatabase database = new SharedCheckpointDatabase(Paths.get(args[1]),
        Engine.LEVELDB);
    database.publish(SharedCheckpointDatabaseTest.payload(1));
    database.applied();
    database.publish(SharedCheckpointDatabaseTest.payload(2));
    if ("applied".equals(args[0])) {
      database.applied();
    }
    Runtime.getRuntime().halt(92);
  }

  private static String runtimeClasspath() {
    Set<String> entries = new LinkedHashSet<>();
    Collections.addAll(entries, System.getProperty("java.class.path", "").split(
        java.util.regex.Pattern.quote(File.pathSeparator)));
    for (ClassLoader loader = SharedCheckpointKillProcessTest.class.getClassLoader();
        loader != null; loader = loader.getParent()) {
      if (loader instanceof URLClassLoader) {
        for (URL url : ((URLClassLoader) loader).getURLs()) {
          if ("file".equals(url.getProtocol())) {
            try {
              entries.add(Paths.get(url.toURI()).toString());
            } catch (java.net.URISyntaxException invalid) {
              throw new IllegalStateException("invalid runtime classpath", invalid);
            }
          }
        }
      }
    }
    return String.join(File.pathSeparator, entries);
  }
}
