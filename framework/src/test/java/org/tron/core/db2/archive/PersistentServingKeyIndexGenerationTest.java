package org.tron.core.db2.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.tron.core.db2.stateroot.PathStateStoreManifest.Engine;

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
        assertEquals(ArchiveParticipantDescriptor.scopeIdentity(PARTICIPANTS),
            generation.getScopeIdentity());
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
        assertTrue(reopened.supportsRangeQueries());
        assertEquals(ArchiveParticipantDescriptor.scopeIdentity(PARTICIPANTS),
            reopened.getScopeIdentity());
        assertArrayEquals(hash(77), reopened.getLatestSourceIdentityDigest());
        assertEquals(3, change(reopened, "account", bytes("hot"), 2, 3));
        List<ServingKeyIndexGeneration.ChangedKey> changed = reopened.changesInRange(
            "account", bytes("h"), bytes("z"), 0, 3, 10);
        assertEquals(1, changed.size());
        assertArrayEquals(bytes("hot"), changed.get(0).getKey());
        assertEquals(1, changed.get(0).getFirstChangeBlock());
        assertThrows(ArchiveQueryLimitExceededException.class,
            () -> reopened.changesInRange("account", new byte[0], null, 0, 3, 1));
      }

      byte[] legacyRangeManifest = Files.readAllBytes(generationPath.resolve("generation.meta"));
      ByteBuffer.wrap(legacyRangeManifest).putShort(4, (short) 3);
      refreshChecksum(legacyRangeManifest);
      Files.write(generationPath.resolve("generation.meta"), legacyRangeManifest);
      try (PersistentServingKeyIndexGeneration legacyRange =
          PersistentServingKeyIndexGeneration.open(generationPath)) {
        assertFalse(legacyRange.supportsRangeQueries());
        assertThrows(ArchivePersistenceException.class,
            () -> legacyRange.changesInRange("account", new byte[0], null, 0, 3, 10));
      }
    }
  }

  @Test
  public void incrementallyPublishesExact27PagesAndCoverageFromCheckpoint() throws Exception {
    Path root = temporaryFolder.newFolder("persistent-exact-v5").toPath();
    Path archive = root.resolve("archive");
    Path catalogRoot = root.resolve("catalog");
    byte[] hot = bytes("hot");
    try (ArchiveHistoryWriter writer = new ArchiveHistoryWriter(archive, 4096,
        ArchiveStoreScope.getStateDatabases())) {
      for (int epoch = 1; epoch <= 6; epoch++) {
        writer.accept(exactDiff(epoch, "account", hot));
      }
      ServingIndexIncrementalPlan initial = writer.planServingIncrement(0, hash(0));
      Path firstShadow = root.resolve("shadow-1");
      try (PersistentServingKeyIndexGeneration first =
          PersistentServingKeyIndexGeneration.buildExact(firstShadow, "exact-1", initial,
              hash(77))) {
        assertTrue(first.isExactOnlyFormat());
        assertFalse(first.supportsRangeQueries());
        assertEquals(6, first.getKeyChangeCount());
        assertEquals(5, change(first, "account", hot, 4, 6));
        assertThrows(ArchivePersistenceException.class, () -> first.changesInRange(
            "account", new byte[0], null, 0, 6, 10));
        assertCoverage(first, "abi", 0, 6, "UNSIGNED_RAW_V1", "exact-1");
        assertCoverage(first, "market_pair_price_to_order", 0, 6,
            "MARKET_PRICE_V1", "exact-1");
        PersistentServingKeyIndexGeneration.GenerationStatistics statistics =
            first.inspectStatistics();
        assertEquals(27, statistics.getStores().size());
        assertTrue(statistics.getApparentBytes() > 0);
        assertTrue(statistics.getAllocatedBytes() > 0);
        assertEquals(1, statistics.getStores().get("account").getKeyMetadataCount());
        assertEquals(0, statistics.getStores().get("account").getInlineKeyCount());
        assertEquals(1, statistics.getStores().get("account").getPagedKeyCount());
        assertEquals(1, statistics.getStores().get("account").getPageCount());
        assertEquals(6, statistics.getStores().get("account").getChangeEntryCount());
        assertEquals(0, statistics.getStores().get("abi").getChangeEntryCount());
        assertTrue(statistics.getStores().get("abi").getLogicalBytes() > 0);
        if (first.getEngine() == Engine.ROCKSDB) {
          assertTrue(statistics.getEngine().getEstimatedLiveDataBytes().isAvailable());
          assertTrue(statistics.getEngine().getTotalSstBytes().isAvailable());
          assertTrue(statistics.getEngine().getPendingCompactionBytes().isAvailable());
          assertTrue(statistics.getEngine().getEstimatedLiveDataBytes().getValue() >= 0);
        } else {
          assertFalse(statistics.getEngine().getEstimatedLiveDataBytes().isAvailable());
          assertFalse(statistics.getEngine().getTotalSstBytes().isAvailable());
          assertFalse(statistics.getEngine().getPendingCompactionBytes().isAvailable());
        }
        PersistentServingKeyIndexGeneration.GenerationStatistics unavailable =
            first.inspectStatistics(ignored -> OptionalLong.empty());
        assertFalse(unavailable.getEngine().getEstimatedLiveDataBytes().isAvailable());
        assertFalse(unavailable.getEngine().getTotalSstBytes().isAvailable());
        assertFalse(unavailable.getEngine().getPendingCompactionBytes().isAvailable());
        assertThrows(IllegalStateException.class,
            () -> unavailable.getEngine().getTotalSstBytes().getValue());
        assertThrows(ArchivePersistenceException.class,
            () -> first.inspectStatistics(ignored -> OptionalLong.of(-1)));
      }

      try (PersistentServingKeyIndexCatalog catalog =
          PersistentServingKeyIndexCatalog.create(catalogRoot, firstShadow)) {
        PersistentServingKeyIndexGeneration pinnedOld = catalog.pin();
        writer.accept(exactDiff(7, "witness", bytes("witness")));
        ServingIndexIncrementalPlan increment = writer.planServingIncrement(6, hash(6));

        Path failedShadow = root.resolve("shadow-failed");
        assertThrows(IOException.class, () -> pinnedOld.extendExact(failedShadow,
            "exact-failed", increment, hash(78), () -> {
              throw new IOException("injected disk-full before exact batch");
            }));
        assertEquals("exact-1", catalog.getCurrentGenerationId());
        assertFalse(catalog.generationExists("exact-failed"));

        Path replacementShadow = root.resolve("shadow-2");
        try (PersistentServingKeyIndexGeneration replacement = pinnedOld.extendExact(
            replacementShadow, "exact-2", increment, hash(78))) {
          assertEquals(7, replacement.getIndexedThrough());
          assertEquals(7, replacement.getKeyChangeCount());
          assertEquals(5, change(replacement, "account", hot, 4, 7));
          assertEquals(7, change(replacement, "witness", bytes("witness"), 6, 7));
          assertCoverage(replacement, "abi", 0, 7, "UNSIGNED_RAW_V1", "exact-2");
          PersistentServingKeyIndexGeneration.GenerationStatistics statistics =
              replacement.inspectStatistics();
          assertEquals(1, statistics.getStores().get("witness").getInlineKeyCount());
          assertEquals(1, statistics.getStores().get("witness").getChangeEntryCount());
          assertEquals(0, statistics.getStores().get("abi").getChangeEntryCount());
        }
        assertTrue(catalog.publish("exact-1", replacementShadow));
        assertEquals("exact-2", catalog.getCurrentGenerationId());
        assertTrue(catalog.generationExists("exact-1"));
        assertEquals(5, change(pinnedOld, "account", hot, 4, 6));
        pinnedOld.close();
        assertFalse(catalog.generationExists("exact-1"));

        ServingIndexIncrementalPlan zeroAction = writer.planServingIncrement(7, hash(7));
        assertEquals(7, zeroAction.getIndexedFrom());
        assertEquals(7, zeroAction.getIndexedThrough());
        assertEquals("exact-2", catalog.getCurrentGenerationId());
      }

      try (PersistentServingKeyIndexCatalog reopened =
          PersistentServingKeyIndexCatalog.open(catalogRoot);
          PersistentServingKeyIndexGeneration current = reopened.pin()) {
        assertTrue(current.isExactOnlyFormat());
        assertEquals(7, current.getIndexedThrough());
        assertEquals(7, change(current, "witness", bytes("witness"), 6, 7));
        assertCoverage(current, "market_pair_price_to_order", 0, 7,
            "MARKET_PRICE_V1", "exact-2");
        Path rebuiltPath = root.resolve("rebuilt");
        try (PersistentServingKeyIndexGeneration rebuilt =
            PersistentServingKeyIndexGeneration.buildExact(rebuiltPath, "rebuilt",
                writer.planServingRebuild(), hash(78))) {
          assertArrayEquals(rebuilt.getAuthoritativePrefixDigest(),
              current.getAuthoritativePrefixDigest());
        }
      }
    }
  }

  @Test
  public void rangeIndexPreservesUnsignedBinaryKeyOrderAndPrefixBoundaries() throws Exception {
    Path root = temporaryFolder.newFolder("persistent-binary-range").toPath();
    try (Fixture fixture = new Fixture(root.resolve("authoritative"))) {
      byte[] zero = new byte[]{0};
      byte[] zeroZero = new byte[]{0, 0};
      byte[] zeroFf = new byte[]{0, (byte) 0xff};
      byte[] one = new byte[]{1};
      fixture.append(1, group("account", zero, zeroZero, zeroFf, one,
          new byte[]{(byte) 0xff}));
      fixture.sync();

      try (PersistentServingKeyIndexGeneration generation =
          PersistentServingKeyIndexGeneration.build(root.resolve("generation"), "binary", 0,
              hash(0), fixture.markers, fixture.index::read, PARTICIPANTS)) {
        List<ServingKeyIndexGeneration.ChangedKey> changed = generation.changesInRange(
            "account", zero, one, 0, 1, 10);
        assertEquals(3, changed.size());
        assertArrayEquals(zero, changed.get(0).getKey());
        assertArrayEquals(zeroZero, changed.get(1).getKey());
        assertArrayEquals(zeroFf, changed.get(2).getKey());

        List<ServingKeyIndexGeneration.ChangedKey> strictUpper = generation.changesInRange(
            "account", zero, zeroFf, 0, 1, 10);
        assertEquals(2, strictUpper.size());
        assertArrayEquals(zero, strictUpper.get(0).getKey());
        assertArrayEquals(zeroZero, strictUpper.get(1).getKey());
        assertTrue(generation.changesInRange("account", zero, one, 1, 1, 10).isEmpty());
      }
    }
  }

  @Test
  public void rejectsLegacyAndSubstitutedGenerationManifestScope() throws Exception {
    Path root = temporaryFolder.newFolder("generation-scope").toPath();
    Path approvedPath = root.resolve("approved-generation");
    try (PersistentServingKeyIndexGeneration approved =
        PersistentServingKeyIndexGeneration.build(approvedPath, "approved", 0, hash(0),
            Collections.emptyList(), location -> {
              throw new AssertionError("empty prefix must not read an index record");
            }, ArchiveParticipantDescriptor.current().getParticipants())) {
      assertEquals(ArchiveParticipantDescriptor.FORMAT_ID, approved.getScopeIdentity());
    }
    try (Fixture fixture = new Fixture(root.resolve("authoritative"))) {
      fixture.append(1, group("account", bytes("key")));
      fixture.sync();
      Path generationPath = root.resolve("generation");
      try (PersistentServingKeyIndexGeneration ignored =
          PersistentServingKeyIndexGeneration.build(generationPath, "generation", 0, hash(0),
              fixture.markers, fixture.index::read, PARTICIPANTS)) {
        // Close before mutating the durable manifest.
      }
      Path manifest = generationPath.resolve("generation.meta");
      byte[] valid = Files.readAllBytes(manifest);

      byte[] legacy = Arrays.copyOf(valid, valid.length);
      ByteBuffer.wrap(legacy).putShort(4, (short) 2);
      refreshChecksum(legacy);
      Files.write(manifest, legacy);
      assertThrows(ArchivePersistenceException.class,
          () -> PersistentServingKeyIndexGeneration.open(generationPath));

      byte[] substituted = Arrays.copyOf(valid, valid.length);
      substituted[10] ^= 1;
      refreshChecksum(substituted);
      Files.write(manifest, substituted);
      assertThrows(ArchivePersistenceException.class,
          () -> PersistentServingKeyIndexGeneration.open(generationPath));
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
          byte[] upperExclusive, int maxEntries) {
        return Collections.emptyList();
      }

      @Override
      public void close() {
        closed.set(true);
      }
    };
  }

  private static void assertCoverage(PersistentServingKeyIndexGeneration generation,
      String database, long from, long through, String comparatorId, String generationId)
      throws Exception {
    PersistentServingKeyIndexGeneration.PersistentStoreCoverage coverage =
        generation.getPersistentStoreCoverage(database);
    assertEquals(database, coverage.getDbName());
    assertEquals(from, coverage.getIndexedFrom());
    assertEquals(through, coverage.getIndexedThrough());
    assertEquals(comparatorId, coverage.getComparatorId());
    assertEquals(generationId, coverage.getGenerationId());
    assertArrayEquals(generation.getHeadHash(), coverage.getHeadHash());
    assertArrayEquals(generation.getAuthoritativePrefixDigest(), coverage.getSourceDigest());
  }

  private static BlockReverseDiff exactDiff(int epoch, String database, byte[] key) {
    return new BlockReverseDiff(new BlockSnapshotMeta(epoch, epoch, hash(epoch), hash(epoch - 1),
        epoch * 3_000L), Collections.singletonList(new BlockReverseDiff.DbGroup(database,
        Collections.singletonList(new BlockReverseDiff.Entry(key,
            OldValue.present(bytes("old-" + epoch)))))));
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

  private static void refreshChecksum(byte[] encoded) {
    int payloadLength = encoded.length - Integer.BYTES;
    int checksum = Hashing.crc32c().hashBytes(encoded, 0, payloadLength).asInt();
    ByteBuffer.wrap(encoded, payloadLength, Integer.BYTES).putInt(checksum);
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
