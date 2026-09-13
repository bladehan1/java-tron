package org.tron.core.db2.archive;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.tron.core.db2.archive.ArchiveProgressEnvelope.Kind;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestState;
import org.tron.core.db2.archive.ArchiveReadSnapshot.PinnedLatestStateFactory;
import org.tron.core.db2.archive.LatestStateGenerationAdapter.SnapshotCapableStore;

/** Two-phase publisher for a latest-state generation acquired under one global state barrier. */
public final class LatestStateGenerationCoordinator
    implements PinnedLatestStateFactory, Closeable {

  private final List<String> participants;
  private final Map<String, SnapshotCapableStore> stores;
  private final ArchiveStateBarrier barrier;
  private final AuthorityReader authorityReader;
  private final List<PublishedGeneration> retired = new ArrayList<>();
  private PublishedGeneration current;
  private boolean closed;

  public LatestStateGenerationCoordinator(List<String> participants,
      Map<String, SnapshotCapableStore> stores, ArchiveStateBarrier barrier,
      AuthorityReader authorityReader) {
    this.participants = validateParticipants(participants);
    TreeMap<String, SnapshotCapableStore> sorted = new TreeMap<>(
        Objects.requireNonNull(stores, "stores"));
    if (!new ArrayList<>(sorted.keySet()).equals(this.participants)
        || sorted.containsValue(null)) {
      throw new IllegalArgumentException("Latest-state coordinator Store set mismatch");
    }
    this.stores = Collections.unmodifiableMap(sorted);
    this.barrier = Objects.requireNonNull(barrier, "barrier");
    this.authorityReader = Objects.requireNonNull(authorityReader, "authorityReader");
  }

  /** Acquires a complete immutable candidate while the caller-supplied global barrier is held. */
  public synchronized Candidate acquire(String generationId) throws IOException {
    ensureOpen();
    if (generationId == null || generationId.isEmpty()) {
      throw new IllegalArgumentException("Latest-state generation id must not be empty");
    }
    Acquisition acquisition = new Acquisition();
    try {
      barrier.run(() -> {
        ArchiveProgressEnvelope before = readAuthority();
        LatestStateGenerationAdapter adapter = new LatestStateGenerationAdapter(participants,
            stores);
        acquisition.pinned = adapter.pin(generationId, before.getEpoch(), before.getBlockHash(),
            participants);
        ArchiveProgressEnvelope after = readAuthority();
        if (!sameAuthority(before, after)) {
          throw new ArchivePersistenceException(
              "Reader-visible authority drifted while latest generation was pinned");
        }
        acquisition.authority = before;
        acquisition.sourceIdentityDigest = adapter.getSourceIdentityDigest();
      });
    } catch (IOException | RuntimeException failure) {
      if (acquisition.pinned != null) {
        try {
          acquisition.pinned.close();
        } catch (IOException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      }
      throw failure;
    }
    return new Candidate(this, generationId, acquisition.authority,
        acquisition.sourceIdentityDigest, acquisition.pinned);
  }

  /** CAS-publishes a fully acquired candidate after its digest has been bound into serving data. */
  public synchronized boolean publish(String expectedGenerationId, Candidate candidate,
      PersistentServingKeyIndexGeneration serving) throws IOException {
    ensureOpen();
    Objects.requireNonNull(candidate, "candidate");
    Objects.requireNonNull(serving, "serving");
    String currentId = current == null ? null : current.generationId;
    if (!Objects.equals(expectedGenerationId, currentId)) {
      return false;
    }
    candidate.validateOwner(this);
    candidate.validateServing(serving);
    PublishedGeneration replacement = candidate.transfer();
    PublishedGeneration previous = current;
    current = replacement;
    if (previous != null) {
      previous.retired = true;
      retired.add(previous);
      reap(previous);
    }
    return true;
  }

  @Override
  public synchronized PinnedLatestState pin(PersistentServingKeyIndexGeneration serving)
      throws IOException {
    Objects.requireNonNull(serving, "serving");
    return pin(serving.getGenerationId(), serving.getIndexedThrough(), serving.getHeadHash(),
        serving.getLatestSourceIdentityDigest(), serving.getParticipatingDatabases());
  }

  synchronized PinnedLatestState pin(String generationId, long blockNumber, byte[] blockHash,
      byte[] sourceIdentityDigest, List<String> expectedParticipants) throws IOException {
    ensureOpen();
    if (current == null || !current.generationId.equals(generationId)
        || current.blockNumber != blockNumber || !Arrays.equals(current.blockHash, blockHash)
        || !Arrays.equals(current.sourceIdentityDigest, sourceIdentityDigest)
        || !participants.equals(expectedParticipants)) {
      throw new ArchivePersistenceException("Published latest generation identity mismatch");
    }
    current.references++;
    return new RequestPin(this, current);
  }

  public synchronized String getCurrentGenerationId() {
    return current == null ? null : current.generationId;
  }

  synchronized int getReferenceCount(String generationId) {
    if (current != null && current.generationId.equals(generationId)) {
      return current.references;
    }
    return 0;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    if ((current != null && current.references != 0)
        || retired.stream().anyMatch(generation -> generation.references != 0)) {
      throw new IOException("Cannot close latest generation coordinator with pinned readers");
    }
    closed = true;
    if (current != null) {
      current.root.close();
      current = null;
    }
    for (PublishedGeneration generation : new ArrayList<>(retired)) {
      if (!generation.closed) {
        generation.closed = true;
        generation.root.close();
      }
    }
    retired.clear();
  }

  private ArchiveProgressEnvelope readAuthority() throws IOException {
    ArchiveProgressEnvelope authority = Objects.requireNonNull(authorityReader.read(),
        "reader-visible authority");
    if (authority.getKind() != Kind.READER_VISIBLE
        || !ArchiveParticipantDescriptor.scopeIdentity(participants)
            .equals(authority.getScopeIdentity())
        || !participants.equals(authority.getParticipants())) {
      throw new ArchivePersistenceException("Invalid reader-visible generation authority");
    }
    return authority;
  }

  private synchronized void release(PublishedGeneration generation) throws IOException {
    if (generation.references <= 0) {
      throw new IllegalStateException("Latest generation reference count underflow");
    }
    generation.references--;
    reap(generation);
  }

  private void reap(PublishedGeneration generation) throws IOException {
    if (generation.retired && generation.references == 0 && !generation.closed) {
      generation.closed = true;
      generation.root.close();
      retired.remove(generation);
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Latest generation coordinator is closed");
    }
  }

  private static boolean sameAuthority(ArchiveProgressEnvelope left,
      ArchiveProgressEnvelope right) {
    return left.getKind() == right.getKind() && left.getEpoch() == right.getEpoch()
        && Arrays.equals(left.getBlockHash(), right.getBlockHash())
        && Arrays.equals(left.getBatchId(), right.getBatchId())
        && Arrays.equals(left.getPayloadDigest(), right.getPayloadDigest())
        && left.getScopeIdentity().equals(right.getScopeIdentity())
        && left.getParticipants().equals(right.getParticipants());
  }

  private static List<String> validateParticipants(List<String> participants) {
    List<String> copy = new ArrayList<>(Objects.requireNonNull(participants, "participants"));
    if (copy.isEmpty()) {
      throw new IllegalArgumentException("Latest-state participant set must not be empty");
    }
    String previous = null;
    for (String participant : copy) {
      if (participant == null || participant.isEmpty()
          || previous != null && previous.compareTo(participant) >= 0) {
        throw new IllegalArgumentException(
            "Latest-state participants must be non-empty, unique, and sorted");
      }
      previous = participant;
    }
    return Collections.unmodifiableList(copy);
  }

  @FunctionalInterface
  public interface AuthorityReader {
    ArchiveProgressEnvelope read() throws IOException;
  }

  private static final class Acquisition {
    private ArchiveProgressEnvelope authority;
    private byte[] sourceIdentityDigest;
    private PinnedLatestState pinned;
  }

  /** Acquired native snapshots owned by the caller until successful publication. */
  public static final class Candidate implements Closeable {
    private final LatestStateGenerationCoordinator owner;
    private final String generationId;
    private final ArchiveProgressEnvelope authority;
    private final byte[] sourceIdentityDigest;
    private PinnedLatestState root;
    private boolean published;

    private Candidate(LatestStateGenerationCoordinator owner, String generationId,
        ArchiveProgressEnvelope authority, byte[] sourceIdentityDigest, PinnedLatestState root) {
      this.owner = owner;
      this.generationId = generationId;
      this.authority = authority;
      this.sourceIdentityDigest = Arrays.copyOf(sourceIdentityDigest,
          sourceIdentityDigest.length);
      this.root = root;
    }

    public long getBlockNumber() {
      return authority.getEpoch();
    }

    public String getGenerationId() {
      return generationId;
    }

    public byte[] getBlockHash() {
      return authority.getBlockHash();
    }

    public byte[] getSourceIdentityDigest() {
      return Arrays.copyOf(sourceIdentityDigest, sourceIdentityDigest.length);
    }

    @Override
    public synchronized void close() throws IOException {
      if (!published && root != null) {
        PinnedLatestState releasing = root;
        root = null;
        releasing.close();
      }
    }

    private synchronized void validateOwner(LatestStateGenerationCoordinator expected) {
      if (owner != expected || published || root == null) {
        throw new IllegalStateException("Latest generation candidate is not publishable");
      }
    }

    private synchronized void validateServing(PersistentServingKeyIndexGeneration serving) {
      if (!generationId.equals(serving.getGenerationId())
          || authority.getEpoch() != serving.getIndexedThrough()
          || !Arrays.equals(authority.getBlockHash(), serving.getHeadHash())
          || !Arrays.equals(sourceIdentityDigest, serving.getLatestSourceIdentityDigest())
          || !authority.getScopeIdentity().equals(serving.getScopeIdentity())
          || !authority.getParticipants().equals(serving.getParticipatingDatabases())) {
        throw new IllegalArgumentException(
            "Serving generation does not match latest-state candidate");
      }
    }

    private synchronized PublishedGeneration transfer() {
      published = true;
      PublishedGeneration result = new PublishedGeneration(generationId, authority.getEpoch(),
          authority.getBlockHash(), sourceIdentityDigest, root);
      root = null;
      return result;
    }
  }

  private static final class PublishedGeneration {
    private final String generationId;
    private final long blockNumber;
    private final byte[] blockHash;
    private final byte[] sourceIdentityDigest;
    private final PinnedLatestState root;
    private int references;
    private boolean retired;
    private boolean closed;

    private PublishedGeneration(String generationId, long blockNumber, byte[] blockHash,
        byte[] sourceIdentityDigest, PinnedLatestState root) {
      this.generationId = generationId;
      this.blockNumber = blockNumber;
      this.blockHash = Arrays.copyOf(blockHash, blockHash.length);
      this.sourceIdentityDigest = Arrays.copyOf(sourceIdentityDigest,
          sourceIdentityDigest.length);
      this.root = root;
    }
  }

  private static final class RequestPin implements PinnedLatestState {
    private final LatestStateGenerationCoordinator owner;
    private final PublishedGeneration generation;
    private boolean closed;

    private RequestPin(LatestStateGenerationCoordinator owner,
        PublishedGeneration generation) {
      this.owner = owner;
      this.generation = generation;
    }

    @Override
    public long getBlockNumber() {
      return generation.blockNumber;
    }

    @Override
    public byte[] getBlockHash() {
      return Arrays.copyOf(generation.blockHash, generation.blockHash.length);
    }

    @Override
    public byte[] getSourceIdentityDigest() {
      return Arrays.copyOf(generation.sourceIdentityDigest,
          generation.sourceIdentityDigest.length);
    }

    @Override
    public synchronized OldValue get(String dbName, byte[] physicalRawKey) throws IOException {
      ensureOpen();
      return generation.root.get(dbName, physicalRawKey);
    }

    @Override
    public List<HistoricalRangeOverlay.Entry> range(String dbName, byte[] lowerInclusive,
        byte[] upperExclusive, int maxEntries) throws IOException {
      ensureOpen();
      return generation.root.range(dbName, lowerInclusive, upperExclusive, maxEntries);
    }

    @Override
    public synchronized void close() throws IOException {
      if (!closed) {
        closed = true;
        owner.release(generation);
      }
    }

    private void ensureOpen() {
      if (closed) {
        throw new IllegalStateException("Pinned latest generation is closed");
      }
    }
  }
}
