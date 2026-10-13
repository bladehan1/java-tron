package org.tron.core.db2.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.nio.file.Path;
import java.util.Collections;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.storage.leveldb.LevelDbDataSourceImpl;
import org.tron.core.config.args.Args;
import org.tron.core.db2.common.LevelDB;
import org.tron.core.db2.common.WrappedByteArray;

public class OutOfBlockWriteGuardTest {
  @Rule
  public final TemporaryFolder temporary = new TemporaryFolder();

  @BeforeClass
  public static void setup() {
    Args.setParam(new String[]{"--output-directory", "output_root_guard_test"}, "config-test.conf");
  }

  @Test
  public void refusesUnknownRootChangesAndAllowsOnlyExplicitCheckpointEntry() throws Exception {
    Path path = temporary.newFolder().toPath();
    CommonParameter parameter = CommonParameter.getInstance();
    String previousOutput = parameter.outputDirectory;
    SnapshotRoot root = new SnapshotRoot(new LevelDB(
        new LevelDbDataSourceImpl(path.toString(), "properties")));
    try {
      parameter.outputDirectory = path.toString();
      OutOfBlockWriteGuard.reset();
      byte[] key = {1};
      root.put(key, new byte[]{7});
      String common = parameter.getStorage().getCommonCheckpointDirectory();
      OutOfBlockWriteGuard.markCommonEstablished(path.resolve(
          common == null || common.isEmpty() ? "common-checkpoint" : common));
      root.put(key, new byte[]{7}); // Same-state restatement remains harmless.
      assertThrows(OutOfBlockWriteGuard.OutOfBlockWriteException.class,
          () -> root.put(key, new byte[]{8}));
      assertThrows(OutOfBlockWriteGuard.OutOfBlockWriteException.class, () -> root.remove(key));
      assertArrayEquals(new byte[]{7}, root.get(key));
      root.applyCheckpointMutations(Collections.singletonMap(WrappedByteArray.of(key),
          WrappedByteArray.of(new byte[]{8})), false);
      assertArrayEquals(new byte[]{8}, root.get(key));
      root.applyCheckpointMutations(Collections.singletonMap(WrappedByteArray.of(key),
          WrappedByteArray.of(null)), true);
      assertNull(root.get(key));
      assertThrows(OutOfBlockWriteGuard.OutOfBlockWriteException.class,
          () -> root.put(key, new byte[0])); // Present-empty differs from absent.
    } finally {
      root.close();
      parameter.outputDirectory = previousOutput;
      OutOfBlockWriteGuard.reset();
    }
  }
}
