package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.BlockReverseDiff.DbGroup;
import org.tron.core.db2.archive.BlockReverseDiff.Entry;
import org.tron.core.db2.archive.HistoricalQueryException.Reason;
import org.tron.core.db2.core.CommonCheckpointFile;
import org.tron.core.db2.core.CommonCheckpointMaterializer;
import org.tron.core.db2.core.CommonCheckpointMaterializer.Authority;
import org.tron.core.db2.core.CommonCheckpointPayload;
import org.tron.core.db2.core.CommonCheckpointRedoCoordinator;
import org.tron.core.db2.core.CommonCheckpointRuntimeOwner;
import org.tron.core.db2.core.CommonCheckpointTarget;
import org.tron.core.db2.stateroot.PathStateFlushTarget;
import org.tron.protos.Protocol.Account;

public class HistoricalQuerySessionBudgetTest {

  private static final byte[] CODE_KEY = {3};
  private static final byte[] CODE_VALUE = {1, 2, 3, 4};
  private static final byte[] MATCH_KEY = {7};
  private static final byte[] MISMATCH_KEY = {9};
  private static final byte[] MISMATCH_ADDRESS = {8};

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void readBudgetExceededFailsClosed() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("reads").toPath())) {
      HistoricalQuerySession session = fixture.open(new HistoricalQuerySession.Limits(
          1, 1L << 20, 10_000));
      assertTrue(session.getCode(CODE_KEY).isPresent());
      assertThrows(HistoricalQueryBudgetException.class, () -> session.getCode(CODE_KEY));
      session.close();
    }
  }

  @Test
  public void byteBudgetExceededFailsClosed() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("bytes").toPath())) {
      HistoricalQuerySession session = fixture.open(new HistoricalQuerySession.Limits(
          100, CODE_VALUE.length - 1L, 10_000));
      assertThrows(HistoricalQueryBudgetException.class, () -> session.getCode(CODE_KEY));
      session.close();
    }
  }

  @Test
  public void cancelledSessionRejectsReadsAndCheckActive() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("cancel").toPath())) {
      HistoricalQuerySession session = fixture.open(HistoricalQuerySession.Limits.defaults());
      session.cancel();
      HistoricalQueryException cancelled = assertThrows(HistoricalQueryException.class,
          session::checkActive);
      assertEquals(Reason.CANCELLED, cancelled.getReason());
      cancelled = assertThrows(HistoricalQueryException.class,
          () -> session.getCode(CODE_KEY));
      assertEquals(Reason.CANCELLED, cancelled.getReason());
      session.close();
      assertEquals(1, fixture.releases.get());
    }
  }

  @Test
  public void closedSessionRejectsFurtherReads() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("closed").toPath())) {
      HistoricalQuerySession session = fixture.open(HistoricalQuerySession.Limits.defaults());
      session.close();
      assertThrows(IllegalStateException.class, () -> session.getCode(CODE_KEY));
    }
  }

  @Test
  public void closeFromForeignThreadIsRejected() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("foreign").toPath())) {
      HistoricalQuerySession session = fixture.open(HistoricalQuerySession.Limits.defaults());
      AtomicReference<Throwable> failure = new AtomicReference<>();
      Thread foreign = new Thread(() -> {
        try {
          session.close();
        } catch (Throwable throwable) {
          failure.set(throwable);
        }
      });
      foreign.start();
      foreign.join(5_000);
      assertTrue(failure.get() instanceof IllegalStateException);
      assertTrue(failure.get().getMessage().contains("released by their owner"));
      session.close();
      session.close();
      assertEquals(1, fixture.releases.get());
    }
  }

  @Test
  public void openWithCancelledControlKeepsAdmissionWithCallerAndClosesSnapshot()
      throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("open-cancel").toPath())) {
      HistoricalQueryControl control = new HistoricalQueryControl(10_000);
      control.cancel();
      StateArchiveCheckpointReadSnapshot snapshot = fixture.pin();
      HistoricalQueryException cancelled = assertThrows(HistoricalQueryException.class,
          () -> HistoricalQuerySession.open(snapshot, hash(2),
              HistoricalQuerySession.Limits.defaults(), control,
              fixture.releases::incrementAndGet));
      assertEquals(Reason.CANCELLED, cancelled.getReason());
      assertEquals(0, fixture.releases.get());
      assertThrows(IllegalStateException.class,
          () -> snapshot.get("account", MATCH_KEY));
      assertFalse(fixture.latest.closed);
    }
  }

  @Test
  public void accountAddressMustMatchThePhysicalKey() throws Exception {
    try (Fixture fixture = new Fixture(temporaryFolder.newFolder("identity").toPath())) {
      HistoricalQuerySession session = fixture.open(HistoricalQuerySession.Limits.defaults());
      Account stored = session.getAccount(MATCH_KEY).orElseThrow(AssertionError::new);
      assertTrue(Arrays.equals(MATCH_KEY, stored.getAddress().toByteArray()));
      assertThrows(ArchivePersistenceException.class, () -> session.getAccount(MISMATCH_KEY));
      session.close();
    }
  }

  private static byte[] hash(int seed) {
    byte[] value = new byte[32];
    for (int index = 0; index < value.length; index++) {
      value[index] = (byte) (seed + index);
    }
    return value;
  }

  private static Account account(byte[] address) {
    return Account.newBuilder().setAddress(ByteString.copyFrom(address))
        .setBalance(1).build();
  }

  private static final class Fixture implements AutoCloseable {

    private final Path root;
    private final byte[] format = hash(90);
    private final CommonCheckpointRuntimeOwner owner;
    private final AtomicInteger releases = new AtomicInteger();
    private FakeLatest latest;

    private Fixture(Path root) throws IOException {
      this.root = root;
      CommonCheckpointPayload payload = payload(format);
      CommonCheckpointTarget target = CommonCheckpointTarget.from(payload);
      StateArchiveCheckpointMaterializer materializer =
          new StateArchiveCheckpointMaterializer(root, format);
      materializer.materialize(payload, target);
      materializer.publish(target);
      owner = new CommonCheckpointRuntimeOwner(new CommonCheckpointRedoCoordinator(
          new CommonCheckpointFile(root.resolve("runtime")), materializer(Authority.CHAINBASE),
          materializer(Authority.PATH_STATE), materializer(Authority.STATE_ARCHIVE)));
      owner.recoverBeforeServing();
    }

    private StateArchiveCheckpointReadSnapshot pin() throws IOException {
      latest = new FakeLatest(2, hash(2));
      return StateArchiveCheckpointReadSnapshot.pin(1, owner, root, format,
          (blockNumber, blockHash) -> latest);
    }

    private HistoricalQuerySession open(HistoricalQuerySession.Limits limits)
        throws IOException {
      return HistoricalQuerySession.open(pin(), hash(2), limits,
          new HistoricalQueryControl(10_000), releases::incrementAndGet);
    }

    @Override
    public void close() {
      owner.close();
    }

    private static CommonCheckpointMaterializer materializer(Authority authority) {
      CommonCheckpointMaterializer materializer = mock(CommonCheckpointMaterializer.class);
      when(materializer.authority()).thenReturn(authority);
      return materializer;
    }

    private static CommonCheckpointPayload payload(byte[] format) {
      List<PathStateFlushTarget.BlockBinding> bindings = new ArrayList<>();
      List<BlockReverseDiff> archives = new ArrayList<>();
      byte[] priorHash = hash(0);
      byte[] priorRoot = hash(30);
      for (int number = 1; number <= 2; number++) {
        byte[] blockHash = hash(number);
        byte[] nextRoot = hash(30 + number);
        BlockSnapshotMeta meta = BlockSnapshotMeta.forBlock(number, blockHash, priorHash,
            number * 3_000L);
        PathStateFlushTarget.BlockBinding binding = mock(PathStateFlushTarget.BlockBinding.class);
        when(binding.getMeta()).thenReturn(meta);
        when(binding.getParentStateRoot()).thenReturn(priorRoot);
        when(binding.getStateRoot()).thenReturn(nextRoot);
        when(binding.getTransitionPayloadDigest()).thenReturn(hash(70 + number));
        bindings.add(binding);
        archives.add(new BlockReverseDiff(meta, Arrays.asList(
            new DbGroup("code", Collections.singletonList(
                new Entry(CODE_KEY, OldValue.present(CODE_VALUE)))),
            new DbGroup("account", Arrays.asList(
                new Entry(MATCH_KEY, OldValue.present(account(MATCH_KEY).toByteArray())),
                new Entry(MISMATCH_KEY,
                    OldValue.present(account(MISMATCH_ADDRESS).toByteArray())))))));
        priorHash = blockHash;
        priorRoot = nextRoot;
      }
      PathStateFlushTarget pathState = mock(PathStateFlushTarget.class);
      when(pathState.getBlocks()).thenReturn(bindings);
      when(pathState.getParentStateRoot()).thenReturn(hash(30));
      when(pathState.getStateRoot()).thenReturn(priorRoot);
      when(pathState.getStores()).thenReturn(Collections.emptyList());
      when(pathState.getSuperNodeMutations()).thenReturn(Collections.emptyList());
      return CommonCheckpointPayload.create(format, pathState, archives,
          Collections.emptyList());
    }
  }

  private static final class FakeLatest implements PinnedLatestState {

    private final long blockNumber;
    private final byte[] blockHash;
    private boolean closed;

    private FakeLatest(long blockNumber, byte[] blockHash) {
      this.blockNumber = blockNumber;
      this.blockHash = blockHash;
    }

    @Override
    public long getBlockNumber() {
      return blockNumber;
    }

    @Override
    public byte[] getBlockHash() {
      return blockHash;
    }

    @Override
    public OldValue get(String dbName, byte[] physicalRawKey) {
      return OldValue.absent();
    }

    @Override
    public List<HistoricalRangeOverlay.Entry> range(String dbName, byte[] lowerInclusive,
        byte[] upperExclusive, int maxEntries) {
      throw new UnsupportedOperationException("point-only checkpoint snapshot");
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
