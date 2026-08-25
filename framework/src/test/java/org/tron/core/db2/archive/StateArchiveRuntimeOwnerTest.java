package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedHistory;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.ArchiveRuntimeQueryGate.Lease;
import org.tron.core.db2.archive.StateArchiveRuntimeOwner.State;
import org.tron.core.db2.core.SnapshotManager;

public class StateArchiveRuntimeOwnerTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void freshBootstrapPublishesRecoverableExact27FixedPoint() throws Exception {
    for (String engine : Arrays.asList("LEVELDB", "ROCKSDB")) {
      Path parent = temporaryFolder.newFolder("bootstrap-" + engine.toLowerCase()).toPath();
      Path archive = parent.resolve("state-archive");
      BlockSnapshotMeta head = BlockSnapshotMeta.forBlock(123, hash(123), hash(122), 456_000L);
      SnapshotManager snapshots = new SnapshotManager("");

      try (StateArchiveRuntimeOwner owner = StateArchiveRuntimeOwner.bootstrapAndRecover(
          snapshots, archive, 4096, head)) {
        assertEquals(head, owner.getRecoveredHead());
        assertEquals(0, owner.getStartupRecoveryActionCount());
        assertEquals(State.RECOVERED, owner.getState());
      }

      assertTrue(Files.isRegularFile(archive.resolve("MANIFEST")));
      assertTrue(Files.isRegularFile(archive.resolve("bootstrap.anchor")));
      assertFalse(Files.exists(archive.resolve("progress")));
      assertFalse(Files.exists(archive.resolve("participants")));
      try (java.util.stream.Stream<Path> entries = Files.list(parent)) {
        assertFalse(entries.anyMatch(path -> path.getFileName().toString()
            .startsWith(".state-archive.bootstrap-")));
      }
      try (StateArchiveRuntimeOwner reopened = StateArchiveRuntimeOwner.recover(
          snapshots, archive, 4096)) {
        assertEquals(head, reopened.getRecoveredHead());
        assertEquals(0, reopened.getStartupRecoveryActionCount());
      }
      try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(
          archive, 4096, ArchiveStoreScope.getStateDatabases())) {
        byte[] address = new byte[21];
        assertThrows(IllegalArgumentException.class,
            () -> writer.readAccountAt(122, address, null));
        assertFalse(writer.readAccountAt(123, address, null).isPresent());
      }
      assertThrows(ArchivePersistenceException.class,
          () -> StateArchiveRuntimeOwner.bootstrapAndRecover(snapshots, archive, 4096,
              head));
    }
  }

  @Test
  public void activeQueryStopsCloseAfterDetachAndDrainedRetryClosesInOrder()
      throws Exception {
    List<String> order = new ArrayList<>();
    Fixture fixture = fixture(order, null, null, null);
    Lease lease = fixture.queryGate.pin(0);

    assertThrows(IllegalStateException.class, fixture.owner::close);

    assertEquals(State.QUIESCING, fixture.owner.getState());
    assertTrue(order.isEmpty());
    assertThrows(IllegalStateException.class, () -> fixture.queryGate.pin(0));
    verify(fixture.manager).detachArchiveRuntime(fixture.attachment);

    lease.close();
    fixture.owner.close();
    fixture.owner.close();

    assertEquals(State.CLOSED, fixture.owner.getState());
    verify(fixture.manager, times(1)).detachArchiveRuntime(fixture.attachment);
    assertEquals(Arrays.asList("latest", "catalog", "participant-2", "participant-1",
        "participant-0", "sink"), order);
  }

  @Test
  public void independentCloseFailuresAreSuppressedAndNotRetried() throws Exception {
    List<String> order = new ArrayList<>();
    IOException latestFailure = new IOException("latest failure");
    Fixture fixture = fixture(order, latestFailure,
        new IllegalStateException("participant failure"), new IOException("sink failure"));

    IOException failure = assertThrows(IOException.class, fixture.owner::close);

    assertSame(latestFailure, failure);
    assertEquals(2, failure.getSuppressed().length);
    assertEquals("Failed to close archive participant 1",
        failure.getSuppressed()[0].getMessage());
    assertEquals("sink failure", failure.getSuppressed()[1].getMessage());
    assertEquals(Arrays.asList("latest", "catalog", "participant-2", "participant-1",
        "participant-0", "sink"), order);
    assertEquals(State.FAILED_CLOSED, fixture.owner.getState());

    assertSame(failure, assertThrows(IOException.class, fixture.owner::close));
    assertEquals(6, order.size());
  }

  @Test
  public void rejectsNonCloseableSinkAndDuplicateOwnedResource() {
    SnapshotManager manager = manager();
    ArchiveRuntimeQueryGate queryGate = new ArchiveRuntimeQueryGate(target -> snapshot());
    Closeable latest = () -> { };
    Closeable catalog = () -> { };
    DurableBlockReverseDiffSink nonCloseable = mock(DurableBlockReverseDiffSink.class);
    ArchiveRuntimeAttachment invalid = attachment(nonCloseable);

    assertThrows(IllegalArgumentException.class, () -> new StateArchiveRuntimeOwner(manager,
        invalid, queryGate, latest, catalog, Collections.emptyList()));

    TrackingSink sink = new TrackingSink(new ArrayList<>(), null);
    ArchiveRuntimeAttachment valid = attachment(sink);
    assertThrows(IllegalArgumentException.class, () -> new StateArchiveRuntimeOwner(manager,
        valid, queryGate, latest, catalog, Collections.singletonList(latest)));
  }

  private static Fixture fixture(List<String> order, IOException latestFailure,
      RuntimeException participantFailure, IOException sinkFailure) {
    SnapshotManager manager = manager();
    TrackingSink sink = new TrackingSink(order, sinkFailure);
    ArchiveRuntimeAttachment attachment = attachment(sink);
    when(manager.detachArchiveRuntime(attachment)).thenReturn(attachment);
    manager.attachArchiveRuntime(attachment);
    ArchiveRuntimeQueryGate queryGate = new ArchiveRuntimeQueryGate(target -> snapshot());
    TrackingCloseable latest = new TrackingCloseable("latest", order, latestFailure, null);
    TrackingCloseable catalog = new TrackingCloseable("catalog", order, null, null);
    List<Closeable> participants = Arrays.asList(
        new TrackingCloseable("participant-0", order, null, null),
        new TrackingCloseable("participant-1", order, null, participantFailure),
        new TrackingCloseable("participant-2", order, null, null));
    StateArchiveRuntimeOwner owner = new StateArchiveRuntimeOwner(manager, attachment, queryGate,
        latest, catalog, participants);
    return new Fixture(manager, attachment, queryGate, owner);
  }

  private static SnapshotManager manager() {
    return mock(SnapshotManager.class);
  }

  private static ArchiveRuntimeAttachment attachment(DurableBlockReverseDiffSink sink) {
    return new ArchiveRuntimeAttachment(mock(OldValueCollector.class), sink);
  }

  private static ArchiveReadSnapshot snapshot() throws IOException {
    byte[] hash = new byte[32];
    ServingKeyIndex serving = mock(ServingKeyIndex.class);
    when(serving.getIndexedFrom()).thenReturn(0L);
    when(serving.getIndexedThrough()).thenReturn(0L);
    when(serving.getHeadHash()).thenReturn(hash);
    when(serving.getAuthoritativePrefixDigest()).thenReturn(new byte[0]);
    when(serving.firstChangeAfter(org.mockito.ArgumentMatchers.anyString(),
        org.mockito.ArgumentMatchers.any(byte[].class), org.mockito.ArgumentMatchers.anyLong(),
        org.mockito.ArgumentMatchers.anyLong())).thenReturn(OptionalLong.empty());
    PinnedLatestState latest = mock(PinnedLatestState.class);
    when(latest.getBlockNumber()).thenReturn(0L);
    when(latest.getBlockHash()).thenReturn(hash);
    PinnedHistory history = mock(PinnedHistory.class);
    when(history.getIndexedFrom()).thenReturn(0L);
    when(history.getIndexedThrough()).thenReturn(0L);
    when(history.getHeadHash()).thenReturn(hash);
    when(history.getAuthoritativePrefixDigest()).thenReturn(new byte[0]);
    return ArchiveReadSnapshot.pin(0, 0, hash, serving, latest, history);
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static final class Fixture {
    private final SnapshotManager manager;
    private final ArchiveRuntimeAttachment attachment;
    private final ArchiveRuntimeQueryGate queryGate;
    private final StateArchiveRuntimeOwner owner;

    private Fixture(SnapshotManager manager, ArchiveRuntimeAttachment attachment,
        ArchiveRuntimeQueryGate queryGate, StateArchiveRuntimeOwner owner) {
      this.manager = manager;
      this.attachment = attachment;
      this.queryGate = queryGate;
      this.owner = owner;
    }
  }

  private static final class TrackingCloseable implements Closeable {
    private final String name;
    private final List<String> order;
    private final IOException ioFailure;
    private final RuntimeException runtimeFailure;

    private TrackingCloseable(String name, List<String> order, IOException ioFailure,
        RuntimeException runtimeFailure) {
      this.name = name;
      this.order = order;
      this.ioFailure = ioFailure;
      this.runtimeFailure = runtimeFailure;
    }

    @Override
    public void close() throws IOException {
      order.add(name);
      if (ioFailure != null) {
        throw ioFailure;
      }
      if (runtimeFailure != null) {
        throw runtimeFailure;
      }
    }
  }

  private static final class TrackingSink implements DurableBlockReverseDiffSink, Closeable {
    private final List<String> order;
    private final IOException failure;

    private TrackingSink(List<String> order, IOException failure) {
      this.order = order;
      this.failure = failure;
    }

    @Override
    public void accept(BlockReverseDiff diff) {
    }

    @Override
    public void awaitCommitted(long epoch) {
    }

    @Override
    public DurableHistoryMarkerRangeEvidence createMarkerRangeEvidence(int maxMarkers) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void releaseThrough(long epoch) {
    }

    @Override
    public void close() throws IOException {
      order.add("sink");
      if (failure != null) {
        throw failure;
      }
    }
  }

}
