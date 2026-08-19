package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedHistory;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.HistoryIndexRecord.KeyGroup;

public class PersistentServingKeyIndexGenerationTest {

  private static final List<String> PARTICIPANTS = Arrays.asList("account", "properties");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void persistsExactKeyChangesAndSourceIdentityAcrossReopen() throws Exception {
    Path root = temporaryFolder.newFolder("persistent-serving").toPath();
    try (Fixture fixture = new Fixture(root.resolve("authoritative"))) {
      fixture.append(1, group("account", bytes("cold"), bytes("hot")));
      fixture.append(2, group("properties", bytes("same")));
      fixture.append(3, group("account", bytes("hot")));
      fixture.sync();

      Path generationPath = root.resolve("generation-1");
      byte[] expectedDigest = ServingKeyIndexGeneration.rebuild("memory", 0, hash(0),
          fixture.markers, fixture.index::read, PARTICIPANTS,
          ServingKeyIndexGeneration.IndexLayout.prototypeDefaults())
          .getAuthoritativePrefixDigest();
      try (PersistentServingKeyIndexGeneration generation =
          PersistentServingKeyIndexGeneration.build(generationPath, "generation-1", 0, hash(0),
              fixture.markers, fixture.index::read, PARTICIPANTS, hash(77))) {
        assertEquals(0, generation.getIndexedFrom());
        assertEquals(3, generation.getIndexedThrough());
        assertArrayEquals(hash(3), generation.getHeadHash());
        assertArrayEquals(expectedDigest, generation.getAuthoritativePrefixDigest());
        assertArrayEquals(hash(77), generation.getLatestSourceIdentityDigest());
        assertTrue(generation.isLatestSourceIdentityBound());
        assertEquals(4, generation.getKeyChangeCount());
        assertEquals(1, change(generation, "account", bytes("hot"), 0, 3));
        assertEquals(3, change(generation, "account", bytes("hot"), 1, 3));
        assertEquals(2, change(generation, "properties", bytes("same"), 0, 3));
        assertFalse(generation.firstChangeAfter("account", bytes("missing"), 0, 3)
            .isPresent());
        assertThrows(IllegalArgumentException.class,
            () -> generation.firstChangeAfter("account-asset", bytes("hot"), 0, 3));
      }

      try (PersistentServingKeyIndexGeneration reopened =
          PersistentServingKeyIndexGeneration.open(generationPath)) {
        assertArrayEquals(hash(77), reopened.getLatestSourceIdentityDigest());
        assertEquals(3, change(reopened, "account", bytes("hot"), 2, 3));
        assertThrows(UnsupportedOperationException.class,
            () -> reopened.changesInRange("account", new byte[0], null, 0, 3, 10));
      }
    }
  }

  @Test
  public void catalogPinsOldGenerationUntilLastReaderReleasesIt() throws Exception {
    Path root = temporaryFolder.newFolder("catalog").toPath();
    Path catalogRoot = root.resolve("catalog");
    try (Fixture fixture = new Fixture(root.resolve("authoritative"))) {
      fixture.append(1, group("account", bytes("one")));
      fixture.sync();
      Path firstShadow = catalogRoot.resolve("shadow-1");
      try (PersistentServingKeyIndexGeneration ignored =
          PersistentServingKeyIndexGeneration.build(firstShadow, "generation-1", 0, hash(0),
              fixture.markers, fixture.index::read, PARTICIPANTS)) {
        // Close the shadow engine before its directory is atomically installed.
      }

      try (PersistentServingKeyIndexCatalog catalog =
          PersistentServingKeyIndexCatalog.create(catalogRoot, firstShadow,
              reader(1, "generation-1"))) {
        PersistentServingKeyIndexGeneration firstPin = catalog.pin(reader(1, "generation-1"));
        assertEquals(1, catalog.getReferenceCount("generation-1"));

        fixture.append(2, group("properties", bytes("two")));
        fixture.sync();
        Path secondShadow = catalogRoot.resolve("shadow-2");
        try (PersistentServingKeyIndexGeneration ignored =
            PersistentServingKeyIndexGeneration.build(secondShadow, "generation-2", 0, hash(0),
                fixture.markers, fixture.index::read, PARTICIPANTS)) {
          // Close before publication.
        }
        assertThrows(ArchivePersistenceException.class,
            () -> catalog.publish("generation-1", secondShadow,
                reader(1, "reader-behind")));
        assertThrows(ArchivePersistenceException.class,
            () -> catalog.publish("generation-1", secondShadow,
                reader(2, hash(99), "wrong-hash")));
        assertTrue(catalog.publish("generation-1", secondShadow,
            reader(2, "generation-2")));
        assertEquals("generation-2", catalog.getCurrentGenerationId());
        assertThrows(ArchivePersistenceException.class,
            () -> catalog.pin(reader(1, "reader-behind")));
        assertEquals(0, catalog.getReferenceCount("generation-2"));
        assertTrue(catalog.generationExists("generation-1"));
        assertEquals(1, change(firstPin, "account", bytes("one"), 0, 1));

        firstPin.close();
        assertEquals(0, catalog.getReferenceCount("generation-1"));
        assertFalse(catalog.generationExists("generation-1"));
        try (PersistentServingKeyIndexGeneration secondPin =
            catalog.pin(reader(2, "generation-2"))) {
          assertEquals(2, change(secondPin, "properties", bytes("two"), 0, 2));
        }
      }

      try (PersistentServingKeyIndexCatalog reopened =
          PersistentServingKeyIndexCatalog.open(catalogRoot);
          PersistentServingKeyIndexGeneration pin =
              reopened.pin(reader(2, "generation-2"))) {
        assertEquals("generation-2", pin.getGenerationId());
        assertEquals(2, pin.getIndexedThrough());
      }
    }
  }

  @Test
  public void readSnapshotOwnsCatalogHistoryAndLatestPinsAsOneUnit() throws Exception {
    Path root = temporaryFolder.newFolder("snapshot-pins").toPath();
    Path catalogRoot = root.resolve("catalog");
    try (Fixture fixture = new Fixture(root.resolve("authoritative"))) {
      fixture.append(1, group("account", bytes("key")));
      fixture.sync();
      Path shadow = catalogRoot.resolve("shadow");
      try (PersistentServingKeyIndexGeneration ignored =
          PersistentServingKeyIndexGeneration.build(shadow, "generation", 0, hash(0),
              fixture.markers, fixture.index::read, PARTICIPANTS)) {
        // Close before publication.
      }
      try (PersistentServingKeyIndexCatalog catalog =
          PersistentServingKeyIndexCatalog.create(catalogRoot, shadow,
              reader(1, "generation"))) {
        AtomicBoolean historyClosed = new AtomicBoolean();
        AtomicBoolean latestClosed = new AtomicBoolean();
        try (ArchiveReadSnapshot snapshot = ArchiveReadSnapshot.pin(0, catalog,
            reader(1, "generation"),
            serving -> history(serving, historyClosed),
            serving -> latest(serving, latestClosed))) {
          assertEquals(1, catalog.getReferenceCount("generation"));
          assertArrayEquals(bytes("old"), snapshot.get("account", bytes("key")).getValue());
        }
        assertTrue(historyClosed.get());
        assertTrue(latestClosed.get());
        assertEquals(0, catalog.getReferenceCount("generation"));
      }
    }
  }

  @Test
  public void catalogRejectsCorruptCurrentPointer() throws Exception {
    Path root = temporaryFolder.newFolder("corrupt-current").toPath();
    Path catalogRoot = root.resolve("catalog");
    try (Fixture fixture = new Fixture(root.resolve("authoritative"))) {
      fixture.append(1, group("account", bytes("key")));
      fixture.sync();
      Path shadow = catalogRoot.resolve("shadow");
      try (PersistentServingKeyIndexGeneration ignored =
          PersistentServingKeyIndexGeneration.build(shadow, "generation", 0, hash(0),
              fixture.markers, fixture.index::read, PARTICIPANTS)) {
        // Close before publication.
      }
      try (PersistentServingKeyIndexCatalog ignored =
          PersistentServingKeyIndexCatalog.create(catalogRoot, shadow,
              reader(1, "generation"))) {
        // Persist a valid initial catalog first.
      }
      Path current = catalogRoot.resolve("current");
      byte[] corrupt = Files.readAllBytes(current);
      corrupt[corrupt.length - 1] ^= 1;
      Files.write(current, corrupt);
      assertThrows(ArchivePersistenceException.class,
          () -> PersistentServingKeyIndexCatalog.open(catalogRoot));
    }
  }

  @Test
  public void snapshotPinsRealCommitIndexAndSegmentHandles() throws Exception {
    Path root = temporaryFolder.newFolder("real-history-pin").toPath();
    Path archive = root.resolve("archive");
    Path catalogRoot = root.resolve("catalog");
    Path shadow = catalogRoot.resolve("shadow");
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096,
        new LinkedHashSet<>(PARTICIPANTS))) {
      BlockReverseDiff diff = new BlockReverseDiff(
          new BlockSnapshotMeta(1, 1, hash(1), hash(0), 1_000),
          Collections.singletonList(new BlockReverseDiff.DbGroup("account",
              Collections.singletonList(new BlockReverseDiff.Entry(bytes("key"),
                  OldValue.present(bytes("old")))))));
      writer.accept(diff);
      try (PersistentServingKeyIndexGeneration ignored =
          writer.buildServingGeneration(shadow, "generation", hash(77))) {
        // Close before catalog publication.
      }
    }

    try (PersistentServingKeyIndexCatalog catalog =
        PersistentServingKeyIndexCatalog.create(catalogRoot, shadow,
            reader(1, "generation"));
        ArchiveReadSnapshot snapshot = ArchiveReadSnapshot.pin(0, catalog,
            reader(1, "generation"), archive, 4096,
            serving -> latest(serving, new AtomicBoolean()))) {
      assertArrayEquals(bytes("old"), snapshot.get("account", bytes("key")).getValue());
      assertEquals(1, catalog.getReferenceCount("generation"));
    }
  }

  @Test
  public void capsuleLoadsDurableReaderHeadAndReleasesPartialAcquireFailures() throws Exception {
    Path root = temporaryFolder.newFolder("durable-capsule").toPath();
    Path archive = root.resolve("archive");
    Path catalogRoot = root.resolve("catalog");
    Path shadow = catalogRoot.resolve("shadow");
    Path unboundShadow = root.resolve("unbound-shadow");
    Path readerVisible = root.resolve("progress/reader-visible.progress");
    HistoryCommitMarker marker;
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096,
        new LinkedHashSet<>(PARTICIPANTS))) {
      writer.accept(new BlockReverseDiff(new BlockSnapshotMeta(1, 1, hash(1), hash(0), 1_000),
          Collections.singletonList(new BlockReverseDiff.DbGroup("account",
              Collections.singletonList(new BlockReverseDiff.Entry(bytes("key"),
                  OldValue.present(bytes("old"))))))));
      marker = writer.committedHead();
      try (PersistentServingKeyIndexGeneration ignored =
          writer.buildServingGeneration(shadow, "generation", hash(77))) {
        // Close before publication.
      }
      try (PersistentServingKeyIndexGeneration ignored =
          writer.buildServingGeneration(unboundShadow, "unbound-generation")) {
        // Legacy/unbound generations remain readable only outside the strict capsule.
      }
    }
    ArchiveProgressEnvelope durableReader = new ArchiveProgressEnvelope(
        ArchiveProgressEnvelope.Kind.READER_VISIBLE, null, 1, marker.getMeta().getBlockHash(),
        marker.getBatchId(), marker.getHistoryLocation().getBodyDigest(), PARTICIPANTS);
    new ArchiveProgressFile(readerVisible, new ArchiveProgressEnvelopeCodec())
        .store(durableReader);

    try (PersistentServingKeyIndexCatalog unboundCatalog =
        PersistentServingKeyIndexCatalog.create(root.resolve("unbound-catalog"), unboundShadow,
            durableReader)) {
      ArchiveGenerationCapsule unbound = new ArchiveGenerationCapsule(unboundCatalog,
          readerVisible, archive, 4096, serving -> latest(serving, new AtomicBoolean()));
      assertThrows(ArchivePersistenceException.class, () -> unbound.pin(0));
      assertEquals(0, unboundCatalog.getReferenceCount("unbound-generation"));
    }

    try (PersistentServingKeyIndexCatalog catalog =
        PersistentServingKeyIndexCatalog.create(catalogRoot, shadow, durableReader)) {
      ArchiveGenerationCapsule failing = new ArchiveGenerationCapsule(catalog, readerVisible,
          archive, 4096, PersistentServingKeyIndexGenerationTest::failLatest);
      assertThrows(IOException.class, () -> failing.pin(0));
      assertEquals(0, catalog.getReferenceCount("generation"));

      AtomicBoolean mismatchClosed = new AtomicBoolean();
      ArchiveGenerationCapsule mismatch = new ArchiveGenerationCapsule(catalog, readerVisible,
          archive, 4096, serving -> latest(serving, mismatchClosed, hash(88)));
      assertThrows(ArchivePersistenceException.class, () -> mismatch.pin(0));
      assertTrue(mismatchClosed.get());
      assertEquals(0, catalog.getReferenceCount("generation"));

      AtomicBoolean latestClosed = new AtomicBoolean();
      ArchiveGenerationCapsule capsule = new ArchiveGenerationCapsule(catalog, readerVisible,
          archive, 4096, serving -> latest(serving, latestClosed));
      try (ArchiveReadSnapshot snapshot = capsule.pin(0)) {
        assertEquals(1, catalog.getReferenceCount("generation"));
        assertArrayEquals(bytes("old"), snapshot.get("account", bytes("key")).getValue());
      }
      assertTrue(latestClosed.get());
      assertEquals(0, catalog.getReferenceCount("generation"));
    }
  }

  @Test
  public void publicationCrashReopensAtOldOrNewAtomicGeneration() throws Exception {
    for (PersistentServingKeyIndexCatalog.PublicationStage failedStage
        : PersistentServingKeyIndexCatalog.PublicationStage.values()) {
      Path root = temporaryFolder.newFolder("publish-" + failedStage).toPath();
      Path catalogRoot = root.resolve("catalog");
      try (Fixture fixture = new Fixture(root.resolve("authoritative"))) {
        fixture.append(1, group("account", bytes("one")));
        fixture.sync();
        Path firstShadow = catalogRoot.resolve("shadow-1");
        try (PersistentServingKeyIndexGeneration ignored =
            PersistentServingKeyIndexGeneration.build(firstShadow, "generation-1", 0, hash(0),
                fixture.markers, fixture.index::read, PARTICIPANTS)) {
          // Close before publication.
        }
        try (PersistentServingKeyIndexCatalog ignored =
            PersistentServingKeyIndexCatalog.create(catalogRoot, firstShadow,
                reader(1, "generation-1"))) {
          // Establish old authority.
        }

        fixture.append(2, group("properties", bytes("two")));
        fixture.sync();
        Path secondShadow = catalogRoot.resolve("shadow-2");
        try (PersistentServingKeyIndexGeneration ignored =
            PersistentServingKeyIndexGeneration.build(secondShadow, "generation-2", 0, hash(0),
                fixture.markers, fixture.index::read, PARTICIPANTS)) {
          // Close before publication.
        }
        AtomicReference<PersistentServingKeyIndexCatalog.PublicationStage> observed =
            new AtomicReference<>();
        try (PersistentServingKeyIndexCatalog failing =
            PersistentServingKeyIndexCatalog.open(catalogRoot, stage -> {
              observed.set(stage);
              if (stage == failedStage) {
                throw new IOException("injected after " + stage);
              }
            })) {
          assertThrows(IOException.class, () -> failing.publish("generation-1", secondShadow,
              reader(2, "generation-2")));
          assertEquals(failedStage, observed.get());
        }

        try (PersistentServingKeyIndexCatalog reopened =
            PersistentServingKeyIndexCatalog.open(catalogRoot)) {
          String expected = failedStage
              == PersistentServingKeyIndexCatalog.PublicationStage.GENERATION_INSTALLED
              ? "generation-1" : "generation-2";
          String retired = expected.equals("generation-1") ? "generation-2" : "generation-1";
          assertEquals(expected, reopened.getCurrentGenerationId());
          assertTrue(reopened.generationExists(expected));
          assertFalse(reopened.generationExists(retired));
        }
      }
    }
  }

  private static PinnedHistory history(PersistentServingKeyIndexGeneration serving,
      AtomicBoolean closed) {
    return new PinnedHistory() {
      @Override
      public long getIndexedFrom() {
        return serving.getIndexedFrom();
      }

      @Override
      public long getIndexedThrough() {
        return serving.getIndexedThrough();
      }

      @Override
      public byte[] getHeadHash() {
        return serving.getHeadHash();
      }

      @Override
      public byte[] getAuthoritativePrefixDigest() {
        return serving.getAuthoritativePrefixDigest();
      }

      @Override
      public OldValue read(String dbName, byte[] rawKey, long firstChangeBlock) {
        return OldValue.present(bytes("old"));
      }

      @Override
      public void close() {
        closed.set(true);
      }
    };
  }

  private static PinnedLatestState failLatest(
      PersistentServingKeyIndexGeneration serving) throws IOException {
    throw new IOException("injected latest snapshot failure for " + serving.getGenerationId());
  }

  private static ArchiveProgressEnvelope reader(int epoch, String generationId) {
    return reader(epoch, hash(epoch), generationId);
  }

  private static ArchiveProgressEnvelope reader(int epoch, byte[] blockHash,
      String generationId) {
    byte[] batch = new byte[16];
    byte[] digest = new byte[32];
    byte[] encoded = bytes(generationId);
    System.arraycopy(encoded, 0, batch, 0, Math.min(batch.length, encoded.length));
    return new ArchiveProgressEnvelope(ArchiveProgressEnvelope.Kind.READER_VISIBLE, null, epoch,
        blockHash, batch, digest, PARTICIPANTS);
  }

  private static PinnedLatestState latest(PersistentServingKeyIndexGeneration serving,
      AtomicBoolean closed) {
    return latest(serving, closed, serving.getLatestSourceIdentityDigest());
  }

  private static PinnedLatestState latest(PersistentServingKeyIndexGeneration serving,
      AtomicBoolean closed, byte[] sourceIdentityDigest) {
    return new PinnedLatestState() {
      @Override
      public long getBlockNumber() {
        return serving.getIndexedThrough();
      }

      @Override
      public byte[] getBlockHash() {
        return serving.getHeadHash();
      }

      @Override
      public byte[] getSourceIdentityDigest() {
        return Arrays.copyOf(sourceIdentityDigest, sourceIdentityDigest.length);
      }

      @Override
      public OldValue get(String dbName, byte[] physicalRawKey) {
        return OldValue.absent();
      }

      @Override
      public List<HistoricalRangeOverlay.Entry> range(String dbName, byte[] lowerInclusive,
          byte[] upperExclusive) {
        return Collections.emptyList();
      }

      @Override
      public void close() {
        closed.set(true);
      }
    };
  }

  private static long change(ServingKeyIndex generation, String database, byte[] key,
      long target, long upper) throws IOException {
    OptionalLong changed = generation.firstChangeAfter(database, key, target, upper);
    assertTrue(changed.isPresent());
    return changed.getAsLong();
  }

  private static KeyGroup group(String database, byte[]... keys) {
    return new KeyGroup(database, Arrays.asList(keys));
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] hash(int suffix) {
    byte[] hash = new byte[32];
    hash[31] = (byte) suffix;
    return hash;
  }

  private static final class Fixture implements AutoCloseable {
    private final HistoryIndexStore index;
    private final List<HistoryCommitMarker> markers = new ArrayList<>();

    private Fixture(Path directory) throws IOException {
      index = new HistoryIndexStore(directory, new HistoryIndexCodec());
    }

    private void append(int block, KeyGroup... groups) throws IOException {
      BlockSnapshotMeta meta = new BlockSnapshotMeta(block, block, hash(block), hash(block - 1),
          block * 1_000L);
      HistoryLocation body = new HistoryLocation(0, block * 100L, 80, block, hash(block));
      HistoryIndexLocation location = index.append(
          new HistoryIndexRecord(meta, body, Arrays.asList(groups)));
      markers.add(new HistoryCommitMarker(meta, block - 1L, body, location, new byte[16],
          PARTICIPANTS));
    }

    private void sync() throws IOException {
      index.sync();
    }

    @Override
    public void close() throws IOException {
      index.close();
    }
  }
}
