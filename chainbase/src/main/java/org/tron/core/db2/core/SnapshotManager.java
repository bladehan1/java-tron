package org.tron.core.db2.core;

import com.google.common.collect.Maps;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.tron.common.error.TronDBException;
import org.tron.common.es.ExecutorServiceManager;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.utils.FileUtil;
import org.tron.common.utils.StorageUtils;
import org.tron.core.db.RevokingDatabase;
import org.tron.core.db.TronDatabase;
import org.tron.core.db2.ISession;
import org.tron.core.db2.archive.ArchivePersistenceException;
import org.tron.core.db2.archive.ArchiveRuntimeAttachment;
import org.tron.core.db2.archive.ArchiveStateBarrier.ArchiveStateAction;
import org.tron.core.db2.archive.ArchiveStoreScope;
import org.tron.core.db2.archive.ArchiveWalBinding;
import org.tron.core.db2.archive.ArchiveWalBindingCodec;
import org.tron.core.db2.archive.BlockChangeView;
import org.tron.core.db2.archive.BlockReverseDiff;
import org.tron.core.db2.archive.BlockReverseDiffSink;
import org.tron.core.db2.archive.BlockSnapshotMeta;
import org.tron.core.db2.archive.DurableBlockReverseDiffSink;
import org.tron.core.db2.archive.DurableHistoryMarkerRangeEvidence;
import org.tron.core.db2.archive.HistoryCommitMarker;
import org.tron.core.db2.archive.OldValueCollector;
import org.tron.core.db2.common.DB;
import org.tron.core.db2.common.IRevokingDB;
import org.tron.core.db2.common.Key;
import org.tron.core.db2.common.Value;
import org.tron.core.db2.common.WrappedByteArray;
import org.tron.core.db2.stateroot.PathStateBlockTransition;
import org.tron.core.db2.stateroot.PathStateRuntimeAttachment;
import org.tron.core.db2.stateroot.PathStateSnapshotDelta;
import org.tron.core.exception.RevokingStoreIllegalStateException;
import org.tron.core.exception.TronError;
import org.tron.core.store.CheckPointV2Store;
import org.tron.core.store.CheckTmpStore;

@Slf4j(topic = "DB")
public class SnapshotManager implements RevokingDatabase {

  public static final int DEFAULT_MIN_FLUSH_COUNT = 1;
  private P66CoupledMutationMaterializer p66Materializer;
  private ExecutorService artifactExecutor;
  private boolean blockFinalizationActive;
  private boolean synchronousLegacyRecovery;

  public synchronized void installP66SnapshotLane(Chainbase assets) {
    installP66SnapshotLane(assets, false);
  }

  public synchronized void installP66SnapshotLane(Chainbase assets, boolean recovering) {
    if (size != 0 || activeSession != 0 || p66Materializer != null
        || dbs.stream().anyMatch(db -> "account-asset".equals(db.getDbName()))) {
      throw new IllegalStateException("P66 Snapshot lane must be installed once before sessions");
    }
    Chainbase accounts = requireDatabase("account");
    Chainbase properties = requireDatabase("properties");
    if (!Snapshot.isRoot(accounts.getHead()) || !Snapshot.isRoot(assets.getHead())) {
      throw new IllegalStateException("P66 Snapshot installation requires root heads");
    }
    add(assets);
    if (!recovering) {
      ((SnapshotRoot) accounts.getHead()).useMaterializedCoupledMutations();
    }
    p66Materializer = new P66CoupledMutationMaterializer(accounts, assets, properties);
    ensureArtifactExecutor();
  }

  public synchronized void finishP66Recovery() {
    if (size != 0 || activeSession != 0 || p66Materializer == null) {
      throw new IllegalStateException("P66 recovery must finish before sessions");
    }
    ((SnapshotRoot) requireDatabase("account").getHead().getRoot())
        .useMaterializedCoupledMutations();
  }

  /** Whether the P66 AccountAsset lane is registered (form ON, or startup RECOVERY_ONLY). */
  public synchronized boolean isP66SnapshotLaneInstalled() {
    return p66Materializer != null;
  }

  /**
   * Removes a recovery-only lane (07-C, I-16): only with no layer and no session, only while the
   * account root still uses upstream {@code processAccount} semantics, and without closing the
   * native database the lane shares with {@code AccountAssetStore}.
   */
  public synchronized Chainbase uninstallP66SnapshotLane() {
    if (size != 0 || activeSession != 0 || p66Materializer == null) {
      throw new IllegalStateException("P66 Snapshot lane can only be uninstalled before sessions");
    }
    SnapshotRoot accountRoot = (SnapshotRoot) requireDatabase("account").getHead().getRoot();
    if (accountRoot.isCoupledMutationsMaterialized()) {
      throw new IllegalStateException("A materialized P66 Snapshot lane cannot be uninstalled");
    }
    Chainbase lane = requireDatabase("account-asset");
    dbs.remove(lane);
    ListeningExecutorService service = flushServices.remove(lane.getDbName());
    if (service != null) {
      ExecutorServiceManager.shutdownAndAwaitTermination(service,
          "flush-service-" + lane.getDbName());
    }
    p66Materializer = null;
    if (artifactExecutor != null && oldValueCollector == null
        && pathStateRuntimeAttachment == null) {
      ExecutorServiceManager.shutdownAndAwaitTermination(artifactExecutor, "block-final-archive");
      artifactExecutor = null;
    }
    return lane;
  }

  /**
   * RECOVERY_ONLY (07-C): legacy replay merges every root with a synchronous batch so the legacy
   * redo can be retired right after {@link #check()}. The merged result is key-for-key the
   * non-synchronous one; only the durability point moves.
   */
  public synchronized void setSynchronousLegacyRecovery(boolean synchronous) {
    if (synchronous && p66Materializer == null) {
      throw new IllegalStateException("Synchronous legacy recovery requires the P66 lane");
    }
    synchronousLegacyRecovery = synchronous;
  }

  /**
   * Whether the legacy checkpoint set that {@link #check()} would replay contains entries of
   * {@code dbName}: v1 {@code checkTmpStore}, or the v2 directories inside the two-minute replay
   * window (falling back to tmp when there is none, as {@code checkV2} does).
   */
  public synchronized boolean legacyCheckpointContains(String dbName) {
    if (hasDurableCommonCheckpointAuthority()) {
      return false;
    }
    byte[] prefix = simpleEncode(Objects.requireNonNull(dbName, "dbName"));
    List<String> checkpoints = getCheckpointList();
    if (!isV2Open() || checkpoints == null || checkpoints.isEmpty()) {
      if (!isV2Open() && checkpoints != null && !checkpoints.isEmpty()) {
        return false; // check() rejects this layout with CHECKPOINT_VERSION
      }
      return checkTmpStore != null && containsPrefix(checkTmpStore, prefix);
    }
    long latestTimestamp = Long.parseLong(checkpoints.get(checkpoints.size() - 1));
    for (String checkpoint : checkpoints) {
      if (latestTimestamp - Long.parseLong(checkpoint) > ONE_MINUTE_MILLS * 2) {
        continue;
      }
      TronDatabase<byte[]> store = getCheckpointDB(checkpoint);
      try {
        if (containsPrefix(store, prefix)) {
          return true;
        }
      } finally {
        store.close();
      }
    }
    return false;
  }

  private static boolean containsPrefix(TronDatabase<byte[]> store, byte[] prefix) {
    for (Map.Entry<byte[], byte[]> entry : store.getDbSource()) {
      byte[] key = entry.getKey();
      if (key.length >= prefix.length
          && Arrays.equals(Arrays.copyOf(key, prefix.length), prefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Retires every legacy redo after a RECOVERY_ONLY replay (07-C, 02 §3.12.1 B6): clears tmp with
   * a synchronous batch and deletes all v2 checkpoint directories oldest first (including those
   * outside the {@code checkV2} replay window), then syncs the parent directory. A crash midway
   * leaves only the newest suffix, whose content is already merged and replays idempotently.
   */
  public synchronized void retireLegacyRedo() {
    if (checkTmpStore != null) {
      Map<byte[], byte[]> deletes = new HashMap<>();
      for (Map.Entry<byte[], byte[]> entry : checkTmpStore.getDbSource()) {
        deletes.put(entry.getKey(), null);
      }
      if (!deletes.isEmpty()) {
        checkTmpStore.updateByBatchSynced(deletes);
      }
    }
    List<String> checkpoints = getCheckpointList();
    if (checkpoints == null || checkpoints.isEmpty()) {
      return;
    }
    Path directory = Paths.get(StorageUtils.getOutputDirectoryByDbName(CHECKPOINT_V2_DIR),
        CommonParameter.getInstance().getStorage().getDbDirectory(), CHECKPOINT_V2_DIR);
    for (String checkpoint : checkpoints) {
      if (!FileUtil.recursiveDelete(directory.resolve(checkpoint).toString())) {
        throw new TronDBException("Failed to retire legacy checkpoint " + checkpoint);
      }
    }
    try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
      channel.force(true);
    } catch (IOException e) {
      throw new TronDBException(e);
    }
  }

  private Chainbase requireDatabase(String name) {
    return dbs.stream().filter(db -> name.equals(db.getDbName())).findFirst()
        .orElseThrow(() -> new IllegalStateException("Missing P66 participant: " + name));
  }

  private static final int DEFAULT_STACK_MAX_SIZE = 256;
  private static final long ONE_MINUTE_MILLS = 60*1000L;
  private static final String CHECKPOINT_V2_DIR = "checkpoint";
  @Getter
  private List<Chainbase> dbs = new ArrayList<>();
  @Getter
  private int size = 0;
  private AtomicInteger maxSize = new AtomicInteger(DEFAULT_STACK_MAX_SIZE);

  private boolean disabled = true;
  // for test
  @Getter
  private int activeSession = 0;
  // for test
  @Setter
  private boolean unChecked = true;

  private volatile int flushCount = 0;

  private volatile boolean  hitDown;

  private Map<String, ListeningExecutorService> flushServices = new HashMap<>();

  private ScheduledExecutorService pruneCheckpointThread = null;
  private final String pruneName = "checkpoint-prune";

  @Autowired
  @Setter
  @Getter
  private CheckTmpStore checkTmpStore;

  @Setter
  private volatile int maxFlushCount = DEFAULT_MIN_FLUSH_COUNT;

  private int checkpointVersion = 1;   // default v1

  private OldValueCollector oldValueCollector;
  private ArchiveRuntimeAttachment archiveRuntimeAttachment;
  private PathStateRuntimeAttachment pathStateRuntimeAttachment;
  private CommonCheckpointRuntimeAttachment commonCheckpointRuntimeAttachment;
  private Long submittedArchiveHistoryEpoch;
  private BlockReverseDiffSink blockReverseDiffSink;
  @Getter
  private volatile long archiveReadableEpoch = -1;
  private volatile ArchiveWalBinding latestArchiveWalBinding;
  private volatile ArchiveWalBinding recoveredArchiveWalBinding;

  public SnapshotManager(String checkpointPath) {
  }

  private void ensureArtifactExecutor() {
    if (artifactExecutor == null) {
      artifactExecutor = new ThreadPoolExecutor(1, 1, 0L,
          TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1), task -> {
            Thread thread = new Thread(task, "block-final-archive");
            thread.setDaemon(true);
            return thread;
          });
    }
  }

  @PostConstruct
  public void init() {
    checkpointVersion = CommonParameter.getInstance().getStorage().getCheckpointVersion();
    // prune checkpoint
    if (isV2Open()) {
      pruneCheckpointThread = ExecutorServiceManager.newSingleThreadScheduledExecutor(pruneName);
      pruneCheckpointThread.scheduleWithFixedDelay(() -> {
        try {
          if (!unChecked) {
            pruneCheckpoint();
          }
        } catch (Throwable t) {
          logger.error("Exception in prune checkpoint", t);
        }
      }, 10000, 3600, TimeUnit.MILLISECONDS);
    }
  }

  public static String simpleDecode(byte[] bytes) {
    byte[] lengthBytes = Arrays.copyOf(bytes, 4);
    int length = Ints.fromByteArray(lengthBytes);
    byte[] value = Arrays.copyOfRange(bytes, 4, 4 + length);
    return new String(value);
  }

  // ------------------------------------------------------------------ maintenance layer
  // Design v2 ch02 §3.11.3, WP-02-12 (X2-08 phase two); ch07 §3.6, open question 10.

  /** One Store's maintenance layer; {@code carrier} is set once it is folded into a block. */
  private static final class MaintenanceSlot {
    private final Chainbase database;
    private final SnapshotRoot root;
    private final SnapshotImpl layer;
    private SnapshotImpl carrier;

    private MaintenanceSlot(Chainbase database, SnapshotRoot root, SnapshotImpl layer) {
      this.database = database;
      this.root = root;
      this.layer = layer;
    }
  }

  /** Open (unfolded) maintenance layer, linked right above every root; null when none. */
  private List<MaintenanceSlot> maintenance;
  /** Folded into the carrying block, kept detached until that block's target is published. */
  private List<MaintenanceSlot> foldedMaintenance;

  /**
   * Opens the maintenance layer: one {@link SnapshotImpl} without block identity above every
   * root, with its write window closed. Only on a node with a Common baseline, after
   * {@code CommonStartupRecovery} and before any startup migration (§3.10), so its content can
   * only come from the recovered state. It is not counted in {@code size}.
   */
  public synchronized void openMaintenanceLayer() {
    if (size != 0 || activeSession != 0 || maintenance != null || foldedMaintenance != null) {
      throw new IllegalStateException("The maintenance layer opens once, before any session");
    }
    for (Chainbase db : dbs) {
      if (!Snapshot.isRoot(db.getHead())) {
        throw new IllegalStateException("The maintenance layer requires root heads: "
            + db.getDbName());
      }
    }
    List<MaintenanceSlot> slots = new ArrayList<>(dbs.size());
    for (Chainbase db : dbs) {
      SnapshotRoot root = (SnapshotRoot) db.getHead();
      SnapshotImpl layer = (SnapshotImpl) root.advance();
      layer.markMaintenance();
      db.setHead(layer);
      slots.add(new MaintenanceSlot(db, root, layer));
    }
    maintenance = slots;
    logger.info("Maintenance layer opened above {} roots (design v2 ch02 §3.11.3)",
        slots.size());
  }

  /** Opens the write window of the registered startup writers (no-op without the layer). */
  public synchronized void beginMaintenanceWrites() {
    setMaintenanceWritesOpen(true);
  }

  /** Closes the write window; afterwards the phase-one rule applies to the layer. */
  public synchronized void endMaintenanceWrites() {
    setMaintenanceWritesOpen(false);
  }

  private void setMaintenanceWritesOpen(boolean open) {
    if (maintenance != null) {
      maintenance.forEach(slot -> slot.layer.setMaintenanceWritesOpen(open));
    }
  }

  /**
   * Whether {@code head} is an unfolded maintenance layer directly above its root. Its content
   * is not part of the persisted state C (it rides with the carrying block), so readers of the
   * root engine at C may treat such a head like the root.
   */
  public static boolean isMaintenanceLayerAboveRoot(Snapshot head) {
    return Snapshot.isImpl(head) && ((SnapshotImpl) head).isMaintenance()
        && Snapshot.isRoot(head.getPrevious());
  }

  /** Whether an unfolded maintenance layer is linked above the roots. */
  public synchronized boolean hasMaintenanceLayer() {
    return maintenance != null;
  }

  /** Whether a maintenance layer is open or folded into a block not yet published. */
  public synchronized boolean hasPendingMaintenance() {
    return maintenance != null || foldedMaintenance != null;
  }

  /**
   * §3.11.3 defensive check: Common startup recovery writes the roots below any layer, so a
   * maintenance layer that already exists then would hide the recovered values. Implementation
   * defect: fail closed.
   */
  public synchronized void requireNoMaintenanceLayer() {
    if (hasPendingMaintenance()) {
      throw new IllegalStateException("Maintenance layer exists before Common startup recovery "
          + "(design v2 ch02 §3.11.3): implementation defect");
    }
  }

  /**
   * Ends the startup part: closes the write window and drops the layer when it holds no
   * effective change (every entry equals the root, e.g. the optimized copy of
   * {@code properties} or a restated value), so such a start is the phase-one start. Returns the
   * number of effective entries per Store that stays for the carrying block.
   */
  public synchronized Map<String, Integer> settleMaintenanceLayer() {
    Map<String, Integer> effective = new java.util.TreeMap<>();
    if (maintenance == null) {
      return effective;
    }
    setMaintenanceWritesOpen(false);
    for (MaintenanceSlot slot : maintenance) {
      int count = 0;
      for (Map.Entry<Key, Value> entry : slot.layer.getDb()) {
        byte[] current = slot.root.get(entry.getKey().getBytes());
        boolean delete = entry.getValue().getOperator() == Value.Operator.DELETE;
        if (delete ? current != null
            : current == null || !Arrays.equals(current, entry.getValue().getBytes())) {
          count++;
        }
      }
      if (count > 0) {
        effective.put(slot.database.getDbName(), count);
      }
    }
    if (effective.isEmpty()) {
      discardMaintenanceLayer();
      logger.info("Maintenance layer had no effective change and was closed");
    } else {
      logger.info("Maintenance layer kept for the carrying block: effective entries {}",
          effective);
    }
    return effective;
  }

  /**
   * Unlinks an unfolded maintenance layer without applying it, as a crash would: the root is
   * unchanged and the next start recomputes the same content (§3.11.3). Startup rollback and
   * tests only; requires that no session layer sits above it.
   */
  public synchronized void discardMaintenanceLayer() {
    if (maintenance == null) {
      return;
    }
    for (MaintenanceSlot slot : maintenance) {
      if (slot.database.getHead() != slot.layer) {
        throw new IllegalStateException("Cannot discard a maintenance layer below a session");
      }
    }
    for (MaintenanceSlot slot : maintenance) {
      slot.root.setNext(null);
      slot.database.setHead(slot.root);
    }
    maintenance = null;
  }

  /**
   * Folds the maintenance layer into the first block finalized above it, the carrying block
   * (§3.11.3): the block layer takes every maintenance entry it did not overwrite itself and is
   * relinked onto the root, so P66 materialization, {@code BlockChangeView} capture, RD(B), the
   * PathState transition and the Common payload all see "maintenance ∪ block" with the root as
   * previous, and every walker of the finalized chain sees only block layers. The detached layer
   * is kept so a pop of the carrying block can restore it.
   */
  private void foldMaintenanceLayer(BlockSnapshotMeta meta) {
    if (maintenance == null) {
      return;
    }
    for (MaintenanceSlot slot : maintenance) {
      Snapshot head = slot.database.getHead();
      if (!Snapshot.isImpl(head) || head == slot.layer || head.getPrevious() != slot.layer) {
        throw new IllegalStateException("The carrying block layer must lie directly above the "
            + "maintenance layer: " + slot.database.getDbName());
      }
    }
    for (MaintenanceSlot slot : maintenance) {
      SnapshotImpl block = (SnapshotImpl) slot.database.getHead();
      block.mergeAhead(slot.layer);
      block.setPrevious(slot.root);
      slot.root.setNext(block);
      slot.layer.setNext(null);
      slot.carrier = block;
    }
    foldedMaintenance = maintenance;
    maintenance = null;
    logger.info("Maintenance layer folded into carrying block {}", meta.getBlockNumber());
  }

  /** §3.11.3: the carrying block was popped before publication; the next block carries. */
  private void restoreMaintenanceAfterCarrierPop() {
    List<MaintenanceSlot> folded = foldedMaintenance;
    if (folded == null) {
      return;
    }
    for (MaintenanceSlot slot : folded) {
      if (slot.database.getHead() != slot.root || slot.root.getNext() != slot.carrier) {
        return;
      }
    }
    for (MaintenanceSlot slot : folded) {
      slot.layer.setPrevious(slot.root);
      slot.root.setNext(slot.layer);
      slot.database.setHead(slot.layer);
      slot.carrier = null;
    }
    foldedMaintenance = null;
    maintenance = folded;
    logger.info("Carrying block popped; maintenance layer restored above the roots");
  }

  /** The carrying block left the chain through a published Common target: nothing to keep. */
  private void releasePublishedMaintenance() {
    List<MaintenanceSlot> folded = foldedMaintenance;
    if (folded == null) {
      return;
    }
    MaintenanceSlot first = folded.get(0);
    if (first.root.getNext() != first.carrier && first.database.getHead() != first.carrier) {
      foldedMaintenance = null;
      logger.info("Maintenance layer published with its carrying block");
    }
  }

  public ISession buildSession() {
    return buildSession(false);
  }

  public synchronized ISession buildSession(boolean forceEnable) {
    archiveWriteAdmission.run();
    requireNoBlockFinalization();
    if (disabled && !forceEnable) {
      return new Session(this);
    }

    boolean disableOnExit = disabled && forceEnable;
    if (forceEnable) {
      disabled = false;
    }

    if (size > maxSize.get() && !hitDown && maintenance == null) {
      flushCount = flushCount + (size - maxSize.get());
      updateSolidity(size - maxSize.get());
      size = maxSize.get();
      // Only this prefix is finalized. Spread history append before the batch checkpoint;
      // current commit(meta) remains revocable and must never write finalized history.
      if (commonCheckpointRuntimeAttachment != null) {
        try {
          commonCheckpointRuntimeAttachment.appendFinalizedHistory(flushCount);
        } catch (IOException | RuntimeException failure) {
          hitDown = true;
          throw new TronError("Finalized Archive append failed", failure, TronError.ErrCode.DB_FLUSH);
        }
      }
      flush();
    }

    advance();
    ++activeSession;
    return new Session(this, disableOnExit);
  }

  @Override
  public void setCursor(Chainbase.Cursor cursor) {
    dbs.forEach(db -> db.setCursor(cursor));
  }

  @Override
  public void setCursor(Chainbase.Cursor cursor, long offset) {
    dbs.forEach(db -> db.setCursor(cursor, offset));
  }

  @Override
  public void add(IRevokingDB db) {
    Chainbase revokingDB = (Chainbase) db;
    dbs.add(revokingDB);
    flushServices.put(revokingDB.getDbName(),
        MoreExecutors.listeningDecorator(ExecutorServiceManager.newSingleThreadExecutor(
            "flush-service-" + revokingDB.getDbName())));
  }

  private void advance() {
    dbs.forEach(db -> {
      Snapshot head = db.getHead();
      if (Snapshot.isImpl(head)) {
        // Only the active session head keeps a block read cache: a layer covered by a new
        // session must not retain one (one block of entries per store, dropped with the
        // layer) and must not serve entries cached before merge/flush rearranged the stack.
        ((SnapshotImpl) head).disableBlockReadCache();
      }
      Snapshot next = head.advance();
      ((SnapshotImpl) next).enableBlockReadCache();
      db.setHead(next);
    });
    ++size;
  }

  private void retreat() {
    dbs.forEach(db -> db.setHead(db.getHead().retreat()));
    --size;
    restoreMaintenanceAfterCarrierPop();
  }

  public synchronized void merge() {
    requireNoBlockFinalization();
    if (activeSession <= 0) {
      throw new RevokingStoreIllegalStateException(activeSession);
    }

    if (size < 2) {
      return;
    }

    dbs.forEach(db -> db.getHead().getPrevious().merge(db.getHead()));
    retreat();
    --activeSession;
  }

  public synchronized void revoke() {
    requireNoBlockFinalization();
    if (disabled) {
      return;
    }

    if (activeSession <= 0) {
      throw new RevokingStoreIllegalStateException(activeSession);
    }

    if (size <= 0) {
      return;
    }

    disabled = true;

    try {
      retreat();
    } finally {
      disabled = false;
    }
    --activeSession;
  }

  public synchronized void commit() {
    requireNoBlockFinalization();
    if (activeSession <= 0) {
      throw new RevokingStoreIllegalStateException(activeSession);
    }

    --activeSession;

    dbs.forEach(db -> {
      if (db.getHead().isOptimized()) {
        db.getHead().reloadToMem();
      }
    });
    logBlockReadCacheStats();
  }

  /** One DEBUG line per committed session with per-store block read-cache occupancy. */
  private void logBlockReadCacheStats() {
    if (!logger.isDebugEnabled()) {
      return;
    }
    StringBuilder stats = new StringBuilder("Block read cache (entries/hits):");
    for (Chainbase db : dbs) {
      Snapshot head = db.getHead();
      if (Snapshot.isImpl(head) && ((SnapshotImpl) head).hasBlockReadCache()) {
        stats.append(' ').append(db.getDbName()).append('=')
            .append(((SnapshotImpl) head).getBlockReadCacheSize())
            .append('/').append(((SnapshotImpl) head).getBlockReadCacheHits());
      }
    }
    logger.debug(stats.toString());
  }

  /**
   * Commits one block-final Snapshot layer and prepares all derived artifacts.
   *
   * <p>The P66 materializer first folds the Account/AccountAsset coupled mutation into this
   * Snapshot.  {@link BlockChangeView#capture(BlockSnapshotMeta, List)} then freezes the changed
   * keys and post-values, including a reference to the previous Snapshot for old-value lookup.
   * The archive and PathState branches consume that same immutable view in parallel.  This is a
   * preparation barrier: the archive branch builds a {@link BlockReverseDiff}, while the PathState
   * branch builds a transition and its Snapshot delta.  Neither branch is allowed to publish its
   * durable authority independently at this point.
   *
   * <p>After both branches complete, the artifacts are attached to each Store's Snapshot layer
   * and PathState is published.  Common checkpoint flush later writes the artifacts per Store and
   * forces the history files under one target.  Keeping this distinction explicit prevents a
   * future optimization from treating the two CPU tasks as independently durable writes.
   * Plain transaction/pending sessions must continue to use {@link #commit()} or merge/revoke.
   */
  public synchronized void commit(BlockSnapshotMeta meta) {
    finalizeBlock(meta, stages -> {
      stages.normalizeSnapshot();
      stages.startBlockDiff();
      stages.buildPathState();
      stages.completeArtifacts();
      stages.commitSession();
    }, () -> { });
  }

  private synchronized void finalizeBlock(BlockSnapshotMeta meta,
      Consumer<ISession.BlockFinalization> pipeline, Runnable sessionCommitted) {
    Objects.requireNonNull(meta, "meta");
    long startedNanos = System.nanoTime();
    if (activeSession <= 0) {
      throw new RevokingStoreIllegalStateException(activeSession);
    }

    foldMaintenanceLayer(meta);
    validateBlockMeta(meta);
    for (Chainbase db : dbs) {
      Snapshot head = db.getHead();
      if (!Snapshot.isImpl(head)) {
        throw new IllegalStateException(
            "Cannot bind block metadata to non-SnapshotImpl head: " + db.getDbName());
      }
    }

    if (blockFinalizationActive) {
      throw new IllegalStateException("Block finalization is already active");
    }
    BlockFinalization stages = new BlockFinalization(meta, startedNanos, sessionCommitted);
    blockFinalizationActive = true;
    try {
      Objects.requireNonNull(pipeline, "pipeline").accept(stages);
      if (stages.phase != 5) {
        throw new IllegalStateException("Block finalization did not commit its session");
      }
      stages.logCompletedStages();
    } finally {
      stages.open = false;
      try {
        stages.joinOutstandingArchive();
      } finally {
        blockFinalizationActive = false;
      }
    }
    logBlockReadCacheStats();
  }

  private void requireNoBlockFinalization() {
    if (blockFinalizationActive) {
      throw new IllegalStateException("Session mutation during block finalization");
    }
  }

  private final class BlockFinalization implements ISession.BlockFinalization {
    private final BlockSnapshotMeta meta;
    private final long startedNanos;
    private final Runnable sessionCommitted;
    private final Thread owner = Thread.currentThread();
    private final boolean commonPathState = pathStateRuntimeAttachment != null
        && pathStateRuntimeAttachment.isCommonCheckpointOnly();
    private final boolean parallel = oldValueCollector != null
        && pathStateRuntimeAttachment != null;
    private boolean open = true;
    private boolean legacySessionEnded;
    private int phase;
    private BlockChangeView changeView;
    private BlockReverseDiff reverseDiff;
    private PathStateBlockTransition pathStateTransition;
    private Future<BlockReverseDiff> archive;
    private final long[] archiveElapsed = new long[1];
    private Throwable pathFailure;
    private long frozenNanos;
    private long archiveNanos;
    private long pathNanos;
    private long pathElapsed;
    private long parallelStart;
    private long attachedNanos;

    private BlockFinalization(BlockSnapshotMeta meta, long startedNanos,
        Runnable sessionCommitted) {
      this.meta = meta;
      this.startedNanos = startedNanos;
      this.sessionCommitted = sessionCommitted;
    }

    private void requirePhase(int expected) {
      if (!open || Thread.currentThread() != owner || phase != expected) {
        throw new IllegalStateException("Invalid block finalization stage: " + phase);
      }
      // A failing stage cannot be retried against a partially prepared snapshot.
      phase = -1;
    }

    @Override
    public void normalizeSnapshot() {
      requirePhase(0);
      // P66 must be materialized before capture: both consumers need the same post-P66 physical
      // change view, otherwise AccountAsset and PathState can derive different block artifacts.
      if (p66Materializer != null) {
        long p66Start = System.nanoTime();
        P66CoupledMutationMaterializer.Statistics stats = p66Materializer.materialize();
        logger.info("P66 Snapshot materialized: head={}, changedAccounts={}, migratedAccounts={}, "
                + "emptyAssetSkips={}, assetPuts={}, assetDeletes={}, prefixQueries={}, "
                + "prefixRows={}, activationAccountScans=0, duplicateRootMigrations=0, "
                + "materializeMs={}", meta.getBlockNumber(), stats.changedAccounts,
            stats.migratedAccounts, stats.emptyAssetSkips, stats.assetPuts, stats.assetDeletes,
            stats.prefixQueries, stats.prefixRows, elapsedMillis(p66Start, System.nanoTime()));
      }
      if (oldValueCollector != null || pathStateRuntimeAttachment != null) {
        changeView = BlockChangeView.capture(meta, dbs);
      }
      frozenNanos = System.nanoTime();
      phase = 1;
    }

    @Override
    public void startBlockDiff() {
      requirePhase(1);
      parallelStart = System.nanoTime();
      if (parallel) {
        BlockChangeView frozen = changeView;
        archive = artifactExecutor.submit(() -> {
          long start = System.nanoTime();
          try {
            return Objects.requireNonNull(oldValueCollector.collect(frozen),
                "archive collector returned null");
          } finally {
            archiveElapsed[0] = System.nanoTime() - start;
          }
        });
      } else {
        reverseDiff = oldValueCollector == null ? null : Objects.requireNonNull(
            oldValueCollector.collect(changeView), "archive collector returned null");
      }
      archiveNanos = System.nanoTime();
      phase = 2;
    }

    @Override
    public void buildPathState() {
      requirePhase(2);
      long pathStart = System.nanoTime();
      try {
        // Exactly one capture per block. P66 and legacy paths differ only in scheduling/failure
        // policy; they do not collect or construct the trie twice.
        if (pathStateRuntimeAttachment != null) {
          pathStateTransition = pathStateRuntimeAttachment.capture(changeView);
        }
        if ((parallel || commonPathState) && pathStateTransition == null) {
          throw new IllegalStateException("PathState capture failed before block-final barrier",
              pathStateRuntimeAttachment.getFailure());
        }
      } catch (Throwable failure) {
        if (!parallel) {
          if (commonPathState) {
            throw rejectBlockArtifacts(new IllegalStateException(
                "Block-final PathState preparation failed", failure));
          }
          throw failure;
        }
        pathFailure = failure;
      }
      pathNanos = System.nanoTime();
      pathElapsed = pathNanos - pathStart;
      phase = 3;
    }

    @Override
    public void completeArtifacts() {
      requirePhase(3);
      if (parallel) {
        // Never cancel-and-revoke: both readers retain Snapshot references.  Releasing this block
        // layer before the other branch finishes would make old-value reads or PathState reads race
        // with Snapshot reclamation and would invalidate the shared-view contract.
        boolean interrupted = false;
        Throwable archiveFailure = null;
        reverseDiff = null;
        while (true) {
          try {
            reverseDiff = archive.get();
            break;
          } catch (InterruptedException failure) {
            interrupted = true;
          } catch (ExecutionException failure) {
            archiveFailure = failure.getCause();
            break;
          }
        }
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        if (pathFailure != null || archiveFailure != null || interrupted) {
          Throwable failure = pathFailure != null ? pathFailure : archiveFailure;
          IllegalStateException rejected = new IllegalStateException(
              "Block-final parallel barrier failed", failure);
          if (pathFailure != null && archiveFailure != null) {
            rejected.addSuppressed(archiveFailure);
          }
          pathStateRuntimeAttachment.fail(rejected);
          throw rejectBlockArtifacts(rejected);
        }
        archiveNanos = System.nanoTime();
        pathNanos = archiveNanos;
        logger.info("Block-final parallel barrier: head={}, archiveMs={}, pathMs={}, wallMs={}, "
                + "archiveP66Projection=0, pathP66Projection=0", meta.getBlockNumber(),
            TimeUnit.NANOSECONDS.toMillis(archiveElapsed[0]),
            TimeUnit.NANOSECONDS.toMillis(pathElapsed), elapsedMillis(parallelStart, pathNanos));
      }
      PathStateSnapshotDelta pathStateDelta = pathStateTransition == null ? null
          : pathStateRuntimeAttachment.preparedSnapshotDelta(pathStateTransition);

      // Expands optimized heads with inherited entries, not commit/flush. Keep AFTER collecting
      // changed entries: doing this before capture would turn unchanged keys into apparent changes.
      dbs.forEach(db -> {
        if (db.getHead().isOptimized()) {
          db.getHead().reloadToMem();
        }
      });

      // Shared references, not one copy of the block artifacts per database. The checkpoint
      // payload reader traverses per-store layers and deduplicates these artifacts by block identity.
      // All databases carry meta; only archive state databases carry the reverse diff / trie redo.
      for (Chainbase db : dbs) {
        boolean stateDatabase = ArchiveStoreScope.isStateDatabase(db.getDbName());
        ((SnapshotImpl) db.getHead()).attachBlockArtifacts(meta,
            stateDatabase ? reverseDiff : null, stateDatabase ? pathStateDelta : null);
      }
      attachedNanos = System.nanoTime();
      if (p66Materializer == null && !commonPathState) {
        --activeSession;
        legacySessionEnded = true;
      }
      // PathState publication advances the volatile/in-memory owner only after both prepared
      // artifacts have been attached.  Durable CURRENT advancement belongs to the checkpoint
      // materializer and must not be moved into either parallel prepare branch.
      if (pathStateRuntimeAttachment != null) {
        pathStateRuntimeAttachment.publish(pathStateTransition);
        if ((p66Materializer != null || commonPathState)
            && pathStateRuntimeAttachment.getFailure() != null) {
          throw rejectBlockArtifacts(new IllegalStateException(
              "Block-final PathState publication failed", pathStateRuntimeAttachment.getFailure()));
        }
      }
      phase = 4;
    }

    @Override
    public void commitSession() {
      requirePhase(4);
      if (!legacySessionEnded) {
        --activeSession;
      }
      sessionCommitted.run();
      phase = 5;
    }

    private void logCompletedStages() {
      long completedNanos = System.nanoTime();
      if (changeView != null && p66Materializer == null) {
        logger.info("Block-final artifact stages: head={}, freezeMs={}, archiveMs={}, "
                + "pathCaptureMs={}, attachMs={}, publishMs={}, totalMs={}",
            meta.getBlockNumber(), elapsedMillis(startedNanos, frozenNanos),
            elapsedMillis(frozenNanos, archiveNanos), elapsedMillis(archiveNanos, pathNanos),
            elapsedMillis(pathNanos, attachedNanos), elapsedMillis(attachedNanos, completedNanos),
            elapsedMillis(startedNanos, completedNanos));
      }
    }

    private void joinOutstandingArchive() {
      if (archive == null) {
        return;
      }
      boolean interrupted = false;
      while (true) {
        try {
          archive.get();
          break;
        } catch (InterruptedException failure) {
          interrupted = true;
        } catch (ExecutionException failure) {
          break;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Common PathState is required state, not a best-effort shadow. Reuse the database fatal-error
   * route so a failed prepare/adoption cannot leave the node serving with a failed state owner.
   * The parallel caller must join its Archive reader before reaching this method. Restart uses
   * the existing Common checkpoint recovery; no in-process retry or history rollback is added.
   */
  private IllegalStateException rejectBlockArtifacts(IllegalStateException failure) {
    if (pathStateRuntimeAttachment != null
        && pathStateRuntimeAttachment.isCommonCheckpointOnly()) {
      hitDown = true;
      pathStateRuntimeAttachment.fail(failure);
      logger.error("Fatal Common block artifact failure; restart recovery is required", failure);
      throw new TronError(failure, TronError.ErrCode.DB_FLUSH);
    }
    return failure;
  }

  private static long elapsedMillis(long startedNanos, long completedNanos) {
    return TimeUnit.NANOSECONDS.toMillis(completedNanos - startedNanos);
  }

  /** Previews optional producer metadata from the active block session without publishing it. */
  public synchronized byte[] previewPathStateRoot(BlockSnapshotMeta meta) {
    if (activeSession <= 0) {
      throw new RevokingStoreIllegalStateException(activeSession);
    }
    if (pathStateRuntimeAttachment == null) {
      return null;
    }
    if (p66Materializer != null) {
      p66Materializer.materialize();
    }
    return pathStateRuntimeAttachment.preview(BlockChangeView.capture(
        Objects.requireNonNull(meta, "meta"), dbs));
  }

  private void validateBlockMeta(BlockSnapshotMeta meta) {
    BlockSnapshotMeta previousMeta = null;
    for (Chainbase db : dbs) {
      if (!ArchiveStoreScope.isStateDatabase(db.getDbName())) {
        continue;
      }
      Snapshot head = db.getHead();
      if (!Snapshot.isImpl(head)) {
        continue;
      }
      Snapshot previous = head.getPrevious();
      BlockSnapshotMeta candidate = Snapshot.isImpl(previous)
          ? ((SnapshotImpl) previous).getBlockSnapshotMeta() : null;
      if (candidate != null && previousMeta != null && !previousMeta.equals(candidate)) {
        throw new IllegalStateException("Previous block metadata differs across state databases");
      }
      if (candidate != null) {
        previousMeta = candidate;
      }
    }
    if (previousMeta == null) {
      return;
    }
    if (meta.getEpoch() != previousMeta.getEpoch() + 1
        || meta.getBlockNumber() != previousMeta.getBlockNumber() + 1
        || !Arrays.equals(meta.getParentHash(), previousMeta.getBlockHash())) {
      throw new IllegalStateException(
          "Non-contiguous block snapshot metadata: previous=" + previousMeta + ", current=" + meta);
    }
  }

  /** Enables archive collection after all Chainbase stores have registered. */
  public synchronized void installArchiveCollector(OldValueCollector collector,
      BlockReverseDiffSink sink) {
    ArchiveStoreScope.validate(dbs);
    if (archiveRuntimeAttachment != null) {
      throw new IllegalStateException("Borrowed archive runtime is already attached");
    }
    if (commonCheckpointRuntimeAttachment != null) {
      throw new IllegalStateException("Common checkpoint runtime is already attached");
    }
    oldValueCollector = Objects.requireNonNull(collector, "collector");
    blockReverseDiffSink = Objects.requireNonNull(sink, "sink");
    if (pathStateRuntimeAttachment != null) {
      ensureArtifactExecutor();
    }
  }

  /** Installs Archive artifact capture without any legacy per-block or flush-time sink. */
  public synchronized void installCommonCheckpointArchiveCollector(OldValueCollector collector) {
    ArchiveStoreScope.validate(dbs);
    if (oldValueCollector != null || blockReverseDiffSink != null
        || archiveRuntimeAttachment != null || commonCheckpointRuntimeAttachment != null) {
      throw new IllegalStateException("Archive collaborators are already installed");
    }
    oldValueCollector = Objects.requireNonNull(collector, "collector");
    if (pathStateRuntimeAttachment != null) {
      ensureArtifactExecutor();
    }
  }

  /** Clears a partially installed common-checkpoint collector during startup rollback. */
  public synchronized void clearCommonCheckpointArchiveCollector() {
    if (commonCheckpointRuntimeAttachment != null || archiveRuntimeAttachment != null
        || blockReverseDiffSink != null) {
      throw new IllegalStateException("Cannot clear an active Archive persistence runtime");
    }
    oldValueCollector = null;
  }

  /** Atomically installs one borrowed archive runtime bundle after store registration. */
  public synchronized void attachArchiveRuntime(ArchiveRuntimeAttachment attachment) {
    ArchiveStoreScope.validate(dbs);
    ArchiveRuntimeAttachment candidate = Objects.requireNonNull(attachment, "attachment");
    if (archiveRuntimeAttachment != null) {
      throw new IllegalStateException("Archive runtime is already attached");
    }
    if (commonCheckpointRuntimeAttachment != null) {
      throw new IllegalStateException("Common checkpoint runtime is already attached");
    }
    if (oldValueCollector != null || blockReverseDiffSink != null) {
      throw new IllegalStateException("Legacy archive collaborators are already installed");
    }
    oldValueCollector = candidate.getCollector();
    blockReverseDiffSink = candidate.getSink();
    archiveRuntimeAttachment = candidate;
    if (pathStateRuntimeAttachment != null) {
      ensureArtifactExecutor();
    }
  }

  /** Detaches the exact borrowed bundle without closing resources owned by its runtime. */
  public synchronized ArchiveRuntimeAttachment detachArchiveRuntime(
      ArchiveRuntimeAttachment expected) {
    ArchiveRuntimeAttachment candidate = Objects.requireNonNull(expected, "expected");
    if (archiveRuntimeAttachment == null) {
      throw new IllegalStateException("Archive runtime is not attached");
    }
    if (archiveRuntimeAttachment != candidate) {
      throw new IllegalStateException("Cannot detach a foreign archive runtime");
    }
    archiveRuntimeAttachment = null;
    oldValueCollector = null;
    blockReverseDiffSink = null;
    submittedArchiveHistoryEpoch = null;
    archiveReadableEpoch = -1;
    return candidate;
  }

  /** Installs an independent non-consensus path-state block-final runtime. */
  public synchronized void attachPathStateRuntime(PathStateRuntimeAttachment attachment) {
    ArchiveStoreScope.validate(dbs);
    PathStateRuntimeAttachment candidate = Objects.requireNonNull(attachment, "attachment");
    if (pathStateRuntimeAttachment != null) {
      throw new IllegalStateException("Path-state runtime is already attached");
    }
    pathStateRuntimeAttachment = candidate;
    if (oldValueCollector != null) {
      ensureArtifactExecutor();
    }
  }

  /**
   * Installs the exclusive three-authority persistence owner. Archive collection and PathState
   * in-memory advancement remain attached separately, but their legacy durable callbacks are
   * never used while this attachment is present.
   */
  public synchronized void attachCommonCheckpointRuntime(
      CommonCheckpointRuntimeAttachment attachment) {
    ArchiveStoreScope.validate(dbs);
    CommonCheckpointRuntimeAttachment candidate = Objects.requireNonNull(attachment,
        "attachment");
    if (!candidate.isEnabled()) {
      throw new IllegalArgumentException("Common checkpoint runtime must be enabled");
    }
    if (commonCheckpointRuntimeAttachment != null) {
      throw new IllegalStateException("Common checkpoint runtime is already attached");
    }
    if (archiveRuntimeAttachment != null || blockReverseDiffSink != null) {
      throw new IllegalStateException(
          "Common checkpoint runtime is mutually exclusive with legacy Archive persistence");
    }
    if (oldValueCollector == null || pathStateRuntimeAttachment == null
        || !pathStateRuntimeAttachment.isCommonCheckpointOnly()) {
      throw new IllegalStateException(
          "Common checkpoint requires Archive capture and a checkpoint-only PathState runtime");
    }
    ensureArtifactExecutor();
    commonCheckpointRuntimeAttachment = candidate;
  }

  /** Detaches the exact Manager-owned common-checkpoint runtime without closing it. */
  public synchronized CommonCheckpointRuntimeAttachment detachCommonCheckpointRuntime(
      CommonCheckpointRuntimeAttachment expected) {
    CommonCheckpointRuntimeAttachment candidate = Objects.requireNonNull(expected, "expected");
    if (commonCheckpointRuntimeAttachment != candidate) {
      throw new IllegalStateException("Cannot detach a missing or foreign common runtime");
    }
    commonCheckpointRuntimeAttachment = null;
    oldValueCollector = null;
    return candidate;
  }

  /** Detaches the exact borrowed path-state runtime without closing its Manager-owned state. */
  public synchronized PathStateRuntimeAttachment detachPathStateRuntime(
      PathStateRuntimeAttachment expected) {
    PathStateRuntimeAttachment candidate = Objects.requireNonNull(expected, "expected");
    if (pathStateRuntimeAttachment == null) {
      throw new IllegalStateException("Path-state runtime is not attached");
    }
    if (pathStateRuntimeAttachment != candidate) {
      throw new IllegalStateException("Cannot detach a foreign path-state runtime");
    }
    pathStateRuntimeAttachment = null;
    return candidate;
  }

  /** Runs latest-state snapshot acquisition inside the canonical apply/flush monitor. */
  public synchronized void withArchiveStateBarrier(ArchiveStateAction action) throws IOException {
    Objects.requireNonNull(action, "action").run();
  }

  public void markArchiveReadableThrough(long epoch) {
    archiveReadableEpoch = epoch;
  }

  public synchronized void pop() {
    if (activeSession != 0) {
      throw new RevokingStoreIllegalStateException(
          String.format("activeSession has to be equal 0, current %d", activeSession));
    }

    if (size <= 0) {
      throw new RevokingStoreIllegalStateException(
          String.format("there is not snapshot to be popped, current: %d", size));
    }

    disabled = true;

    try {
      retreat();
    } finally {
      disabled = false;
    }
  }

  @Override
  public synchronized void fastPop() {
    if (activeSession != 0) {
      throw new RevokingStoreIllegalStateException(
          String.format("activeSession has to be equal 0, current %d", activeSession));
    }
    if (size <= 0) {
      throw new RevokingStoreIllegalStateException(
          String.format("there is not snapshot to be popped, current: %d", size));
    }
    if (submittedArchiveHistoryEpoch != null) {
      throw new IllegalStateException("Cannot pop while archive history flush is pending");
    }
    pop();
  }

  public synchronized void enable() {
    disabled = false;
  }

  @Override
  public int size() {
    return size;
  }

  public int getMaxSize() {
    return maxSize.get();
  }

  @Override
  public void setMaxSize(int maxSize) {
    this.maxSize.set(maxSize);
  }

  public synchronized void disable() {
    disabled = true;
  }

  @Override
  public void shutdown() {
    Closeable legacyArchiveSink = prepareArchiveShutdown();
    if (artifactExecutor != null) {
      ExecutorServiceManager.shutdownAndAwaitTermination(artifactExecutor, "block-final-archive");
    }
    ExecutorServiceManager.shutdownAndAwaitTermination(pruneCheckpointThread, pruneName);
    flushServices.forEach((key, value) -> ExecutorServiceManager.shutdownAndAwaitTermination(value,
        "flush-service-" + key));
    if (legacyArchiveSink != null) {
      try {
        legacyArchiveSink.close();
      } catch (IOException e) {
        logger.error("Failed to close archive history sink.", e);
      }
    }
  }

  private synchronized Closeable prepareArchiveShutdown() {
    submittedArchiveHistoryEpoch = null;
    if (archiveRuntimeAttachment != null) {
      archiveRuntimeAttachment = null;
      oldValueCollector = null;
      blockReverseDiffSink = null;
      archiveReadableEpoch = -1;
      return null;
    }
    if (commonCheckpointRuntimeAttachment != null) {
      commonCheckpointRuntimeAttachment = null;
      oldValueCollector = null;
      blockReverseDiffSink = null;
      archiveReadableEpoch = -1;
      return null;
    }
    return blockReverseDiffSink instanceof Closeable ? (Closeable) blockReverseDiffSink : null;
  }

  public void updateSolidity(int hops) {
    for (int i = 0; i < hops; i++) {
      for (Chainbase db : dbs) {
        db.getHead().updateSolidity();
      }
    }
  }

  public boolean shouldBeRefreshed() {
    return flushCount >= maxFlushCount;
  }

  private void refresh() {
    refresh(flushCount);
  }

  private void refresh(int count) {
    List<ListenableFuture<?>> futures = new ArrayList<>(dbs.size());
    Chainbase properties = null;
    if (oldValueCollector != null) {
      properties = dbs.stream()
          .filter(db -> "properties".equals(db.getDbName()))
          .findFirst()
          .orElse(null);
      if (properties != null) {
        // Account root projection reads the durable optimization flag. Make that dependency
        // deterministic when archive mode projects account-asset changes at block boundaries.
        refreshOne(properties, count);
      }
    }
    for (Chainbase db : dbs) {
      if (db == properties) {
        continue;
      }
      futures.add(flushServices.get(db.getDbName()).submit(() -> refreshOne(db, count)));
    }
    Future<?> future = Futures.allAsList(futures);
    try {
      future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TronDBException(e);
    } catch (ExecutionException e) {
      throw new TronDBException(e);
    }
  }

  private void refreshOne(Chainbase db, int count) {
    if (Snapshot.isRoot(db.getHead())) {
      return;
    }

    List<Snapshot> snapshots = new ArrayList<>();

    SnapshotRoot root = (SnapshotRoot) db.getHead().getRoot();
    Snapshot next = root;
    for (int i = 0; i < count; ++i) {
      next = next.getNext();
      snapshots.add(next);
    }

    root.merge(snapshots);

    root.resetSolidity();
    if (db.getHead() == next) {
      db.setHead(root);
    } else {
      next.getNext().setPrevious(root);
      root.setNext(next.getNext());
    }
  }

  public void flush() {
    flush(false);
  }

  @Override
  public void flushPending() {
    flush(true);
  }

  private Runnable archiveWriteAdmission = () -> { };

  public synchronized void setArchiveWriteAdmission(Runnable admission) {
    archiveWriteAdmission = Objects.requireNonNull(admission, "admission");
  }

  private synchronized void flush(boolean force) {
    if (unChecked || (force && flushCount == 0)) {
      return;
    }

    if (force || shouldBeRefreshed()) {
      if (!force) {
        archiveWriteAdmission.run();
      }
      try {
        long start = System.currentTimeMillis();
        if (commonCheckpointRuntimeAttachment != null) {
          if (flushCount <= 0) {
            return;
          }
          try {
            commonCheckpointRuntimeAttachment.checkpointAndRebase(flushCount);
          } catch (IOException | RuntimeException failure) {
            throw new TronDBException("Common checkpoint publication failed", failure);
          }
          releasePublishedMaintenance();
          flushCount = 0;
          logger.info("Common checkpoint flush cost: {} ms.",
              System.currentTimeMillis() - start);
          return;
        }
        BlockSnapshotMeta pathStateFlushTarget = pathStateFlushTarget();
        ArchiveWalBinding archiveBinding = publishArchiveHistoryForFlush();
        if (!isV2Open()) {
          deleteCheckpoint();
        }
        createCheckpoint(archiveBinding);

        long checkPointEnd = System.currentTimeMillis();
        if (archiveBinding != null && archiveRuntimeAttachment != null) {
          try {
            archiveRuntimeAttachment.publishCommittedPrefix(archiveBinding.getLast());
          } catch (IOException | RuntimeException failure) {
            throw new TronDBException("Archive committed-prefix publication failed", failure);
          }
        }
        refresh();
        if (pathStateRuntimeAttachment != null && pathStateFlushTarget != null) {
          pathStateRuntimeAttachment.flushBaseThrough(pathStateFlushTarget.getBlockNumber(),
              pathStateFlushTarget.getBlockHash());
        }
        if (archiveBinding != null && archiveRuntimeAttachment != null) {
          try {
            archiveRuntimeAttachment.publishReadableState(archiveBinding.getLast());
          } catch (IOException | RuntimeException failure) {
            throw new TronDBException("Archive readable-state publication failed", failure);
          }
        }
        if (archiveBinding != null) {
          ((DurableBlockReverseDiffSink) blockReverseDiffSink)
              .releaseThrough(archiveBinding.getLast().getEpoch());
          submittedArchiveHistoryEpoch = null;
        }
        flushCount = 0;
        logger.info("Flush cost: {} ms, create checkpoint cost: {} ms, refresh cost: {} ms.",
            System.currentTimeMillis() - start,
            checkPointEnd - start,
            System.currentTimeMillis() - checkPointEnd
        );
      } catch (TronDBException e) {
        logger.error(" Find fatal error, program will be exited soon.", e);
        hitDown = true;
        throw new TronError(e, TronError.ErrCode.DB_FLUSH);
      }
    }
  }

  private BlockSnapshotMeta pathStateFlushTarget() {
    if (pathStateRuntimeAttachment == null || flushCount == 0 || dbs.isEmpty()) {
      return null;
    }
    try {
      Snapshot next = dbs.get(0).getHead().getRoot();
      BlockSnapshotMeta target = null;
      for (int index = 0; index < flushCount; index++) {
        next = next.getNext();
        if (!Snapshot.isImpl(next)) {
          throw new IllegalStateException("Path-state flush range is missing a snapshot layer");
        }
        target = ((SnapshotImpl) next).getBlockSnapshotMeta();
        if (target == null) {
          throw new IllegalStateException("Path-state flush layer has no block metadata");
        }
      }
      return target;
    } catch (RuntimeException failure) {
      pathStateRuntimeAttachment.fail(failure);
      return null;
    }
  }

  private ArchiveWalBinding publishArchiveHistoryForFlush() {
    if (oldValueCollector == null) {
      return null;
    }
    if (!(blockReverseDiffSink instanceof DurableBlockReverseDiffSink)) {
      throw new TronDBException("Archive sink cannot prove durable history before checkpoint");
    }
    Chainbase stateDatabase = dbs.stream()
        .filter(db -> ArchiveStoreScope.isStateDatabase(db.getDbName()))
        .findFirst()
        .orElseThrow(() -> new TronDBException("Archive mode has no registered state database"));
    Snapshot next = stateDatabase.getHead().getRoot();
    List<BlockReverseDiff> prepared = new ArrayList<>(flushCount);
    BlockSnapshotMeta last = null;
    for (int i = 0; i < flushCount; i++) {
      next = next.getNext();
      if (!Snapshot.isImpl(next)) {
        throw new TronDBException("Archive flush range is missing a snapshot layer");
      }
      BlockSnapshotMeta meta = ((SnapshotImpl) next).getBlockSnapshotMeta();
      if (meta == null) {
        throw new TronDBException("Archive flush range contains a layer without block metadata");
      }
      if (last != null && meta.getEpoch() != last.getEpoch() + 1) {
        throw new TronDBException("Archive flush range is not epoch-contiguous");
      }
      BlockReverseDiff reverseDiff = ((SnapshotImpl) next).getPreparedArchiveBlock();
      if (reverseDiff == null || !meta.equals(reverseDiff.getMeta())) {
        throw new TronDBException(
            "Archive flush range contains a layer without its matching prepared payload");
      }
      prepared.add(reverseDiff);
      last = meta;
    }
    if (last == null) {
      return null;
    }
    try {
      DurableBlockReverseDiffSink durableSink =
          (DurableBlockReverseDiffSink) blockReverseDiffSink;
      if (submittedArchiveHistoryEpoch == null) {
        durableSink.acceptAll(prepared);
        submittedArchiveHistoryEpoch = last.getEpoch();
      } else if (submittedArchiveHistoryEpoch.longValue() != last.getEpoch()) {
        throw new ArchivePersistenceException(
            "Submitted archive history target does not match the pending flush range");
      }
      durableSink.awaitCommitted(last.getEpoch());
      List<BlockSnapshotMeta> expectedMetas = prepared.stream()
          .map(BlockReverseDiff::getMeta).collect(Collectors.toList());
      DurableHistoryMarkerRangeEvidence evidence =
          durableSink.createMarkerRangeEvidence(prepared.size());
      return ArchiveWalBinding.fromMarkers(evidence.read(expectedMetas));
    } catch (RuntimeException e) {
      throw new TronDBException("Archive history durability gate failed", e);
    }
  }

  public void createCheckpoint() {
    createCheckpoint(null);
  }

  private void createCheckpoint(ArchiveWalBinding archiveBinding) {
    TronDatabase<byte[]> checkPointStore = null;
    try {
      Map<WrappedByteArray, WrappedByteArray> batch = new HashMap<>();
      for (Chainbase db : dbs) {
        Snapshot head = db.getHead();
        if (Snapshot.isRoot(head)) {
          return;
        }

        String dbName = db.getDbName();

        if (Objects.equals(dbName, "trans-cache")) {
          // trans-cache is deprecated
          continue;
        }

        Snapshot next = head.getRoot();
        for (int i = 0; i < flushCount; ++i) {
          next = next.getNext();
          SnapshotImpl snapshot = (SnapshotImpl) next;
          DB<Key, Value> keyValueDB = snapshot.getDb();
          for (Map.Entry<Key, Value> e : keyValueDB) {
            Key k = e.getKey();
            Value v = e.getValue();
            batch.put(WrappedByteArray.of(Bytes.concat(simpleEncode(dbName), k.getBytes())),
                WrappedByteArray.of(v.encode()));
          }
        }
      }
      if (archiveBinding != null) {
        batch.put(WrappedByteArray.of(ArchiveWalBinding.getCheckpointKey()),
            WrappedByteArray.of(new ArchiveWalBindingCodec().encode(archiveBinding)));
      }
      if (isV2Open()) {
        String dbName = String.valueOf(System.currentTimeMillis());
        checkPointStore = getCheckpointDB(dbName);
      } else {
        checkPointStore = checkTmpStore;
      }

      checkPointStore.updateByBatch(batch.entrySet().stream()
              .map(e -> Maps.immutableEntry(e.getKey().getBytes(), e.getValue().getBytes()))
              .collect(HashMap::new, (m, k) -> m.put(k.getKey(), k.getValue()), HashMap::putAll));
      latestArchiveWalBinding = archiveBinding;

    } catch (Exception e) {
      throw new TronDBException(e);
    } finally {
      if (isV2Open() && checkPointStore != null) {
        checkPointStore.close();
      }
    }
  }

  private TronDatabase<byte[]> getCheckpointDB(String dbName) {
    return new CheckPointV2Store(CHECKPOINT_V2_DIR+"/"+dbName);
  }

  public List<String> getCheckpointList() {
    String dbPath = Paths.get(StorageUtils.getOutputDirectoryByDbName(CHECKPOINT_V2_DIR),
        CommonParameter.getInstance().getStorage().getDbDirectory()).toString();
    File file = new File(Paths.get(dbPath, CHECKPOINT_V2_DIR).toString());
    if (file.exists() && file.isDirectory()) {
      String[] subDirs = file.list();
      if (subDirs != null) {
        return Arrays.stream(subDirs).sorted().collect(Collectors.toList());
      }
    }
    return null;
  }

  private void deleteCheckpoint() {
    if(checkTmpStore == null) {
      // only occurs in mock test. TODO fix test
      return;
    }
    try {
      Map<byte[], byte[]> hmap = new HashMap<>();
      for (Map.Entry<byte[], byte[]> e : checkTmpStore.getDbSource()) {
        hmap.put(e.getKey(), null);
      }
      if (hmap.size() != 0) {
        checkTmpStore.getDbSource().updateByBatch(hmap);
      }
    } catch (Exception e) {
      throw new TronDBException(e);
    }
  }

  private void pruneCheckpoint() {
    if (unChecked) {
      return;
    }
    List<String> cpList = getCheckpointList();
    if (cpList == null) {
      return;
    }
    if (cpList.size() < 3) {
      return;
    }
    long latestTimestamp = Long.parseLong(cpList.get(cpList.size()-1));
    for (String cp: cpList.subList(0, cpList.size()-3)) {
      long timestamp = Long.parseLong(cp);
      if (latestTimestamp - timestamp <= ONE_MINUTE_MILLS*2) {
        break;
      }
      String checkpointPath = Paths.get(StorageUtils.getOutputDirectoryByDbName(CHECKPOINT_V2_DIR),
          CommonParameter.getInstance().getStorage().getDbDirectory(), CHECKPOINT_V2_DIR).toString();
      if (!FileUtil.recursiveDelete(Paths.get(checkpointPath, cp).toString())) {
        logger.error("checkpoint prune failed, timestamp: {}", timestamp);
        return;
      }
      logger.debug("checkpoint prune success, timestamp: {}", timestamp);
    }
  }

  // ensure run this method first after process start.
  @Override
  public void check() {
    recoveredArchiveWalBinding = null;
    if (hasDurableCommonCheckpointAuthority()) {
      logger.info("Common checkpoint authority established, skip legacy checkpoint recovery");
      unChecked = false;
      return;
    }
    if (!isV2Open()) {
      List<String> cpList = getCheckpointList();
      if (cpList != null && cpList.size() != 0) {
        String msg = "checkpoint check failed, the checkpoint version of database not match your "
            + "config file, please set storage.checkpoint.version = 2 in your config file "
            + "and restart the node.";
        throw new TronError(msg, TronError.ErrCode.CHECKPOINT_VERSION);
      }
      checkV1();
    } else {
      checkV2();
    }
  }

  private boolean hasDurableCommonCheckpointAuthority() {
    org.tron.core.config.args.Storage storage =
        CommonParameter.getInstance().getStorage();
    if (!storage.isCommonCheckpointEnabled()) {
      return false;
    }
    Path directory = Paths.get(CommonParameter.getInstance().getOutputDirectory(),
        storage.getCommonCheckpointDirectory()).normalize();
    return hasDurableCommonCheckpointAuthority(true, directory);
  }

  static boolean hasDurableCommonCheckpointAuthority(boolean enabled, Path directory) {
    if (!enabled) {
      return false;
    }
    Path admitted = Objects.requireNonNull(directory, "directory");
    return Files.isRegularFile(admitted.resolve(ChainbaseCheckpointMaterializer.CURRENT_FILE),
        LinkOption.NOFOLLOW_LINKS)
        || Files.isRegularFile(admitted.resolve(CommonCheckpointFile.FILE_NAME),
        LinkOption.NOFOLLOW_LINKS);
  }

  private void checkV1() {
    for (Chainbase db: dbs) {
      if (!Snapshot.isRoot(db.getHead())) {
        throw new IllegalStateException("First check.");
      }
    }
    recover(checkTmpStore);
    logger.info("checkpoint v1 recover success");
    unChecked = false;
  }

  private void checkV2() {
    logger.info("checkpoint version: {}", CommonParameter.getInstance().getStorage().getCheckpointVersion());
    logger.info("checkpoint sync: {}", CommonParameter.getInstance().getStorage().isCheckpointSync());
    List<String> cpList = getCheckpointList();
    if (cpList == null || cpList.size() == 0) {
      logger.info("checkpoint size is 0, using v1 recover");
      checkV1();
      deleteCheckpoint();
      return;
    }

    long latestTimestamp = Long.parseLong(cpList.get(cpList.size()-1));
    for (String cp: cpList) {
      long timestamp = Long.parseLong(cp);
      if (latestTimestamp - timestamp > ONE_MINUTE_MILLS*2) {
        continue;
      }
      TronDatabase<byte[]> checkPointV2Store = getCheckpointDB(cp);
      try {
        recover(checkPointV2Store);
      } finally {
        checkPointV2Store.close();
      }
    }
    logger.info("checkpoint v2 recover success");
    unChecked = false;
  }

  private void recover(TronDatabase<byte[]> tronDatabase) {
    List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>();
    ArchiveWalBinding recoveredBinding = null;
    for (Map.Entry<byte[], byte[]> entry : tronDatabase.getDbSource()) {
      byte[] key = Arrays.copyOf(entry.getKey(), entry.getKey().length);
      byte[] value = Arrays.copyOf(entry.getValue(), entry.getValue().length);
      if (ArchiveWalBinding.isCheckpointKey(key)) {
        if (recoveredBinding != null) {
          throw new ArchivePersistenceException(
              "Checkpoint contains duplicate Archive WAL bindings");
        }
        try {
          recoveredBinding = new ArchiveWalBindingCodec().decode(value);
        } catch (IllegalArgumentException invalid) {
          throw new ArchivePersistenceException(
              "Checkpoint Archive WAL binding is corrupt", invalid);
        }
      }
      entries.add(Maps.immutableEntry(key, value));
    }
    Map<String, Chainbase> dbMap = dbs.stream()
        .map(db -> Maps.immutableEntry(db.getDbName(), db))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    advance();
    for (Map.Entry<byte[], byte[]> e : entries) {
      byte[] key = e.getKey();
      byte[] value = e.getValue();
      if (ArchiveWalBinding.isCheckpointKey(key)) {
        continue;
      }
      String db = simpleDecode(key);
      if (dbMap.get(db) == null) {
        continue;
      }
      byte[] realKey = Arrays.copyOfRange(key, db.getBytes().length + 4, key.length);
      byte[] realValue = value.length == 1 ? null : Arrays.copyOfRange(value, 1, value.length);
      if (realValue != null) {
        dbMap.get(db).getHead().put(realKey, realValue);
      } else {
        byte op = value[0];
        if (Value.Operator.DELETE.getValue() == op) {
          dbMap.get(db).getHead().remove(realKey);
        } else {
          dbMap.get(db).getHead().put(realKey, new byte[0]);
        }
      }
    }

    for (Chainbase db : legacyRecoveryMergeOrder()) {
      SnapshotRoot root = (SnapshotRoot) db.getHead().getRoot();
      if (synchronousLegacyRecovery) {
        root.mergeLegacyRecoverySynced(db.getHead());
      } else {
        root.merge(db.getHead());
      }
    }
    retreat();
    if (recoveredBinding != null) {
      recoveredArchiveWalBinding = recoveredBinding;
    }
  }

  /**
   * Upstream order without the lane. With the lane, {@code account} must merge before
   * {@code account-asset}: {@code processAccount} writes its rows into the lane's replay layer,
   * which is only merged afterwards (I-15).
   */
  private List<Chainbase> legacyRecoveryMergeOrder() {
    if (p66Materializer == null) {
      return dbs;
    }
    List<Chainbase> ordered = new ArrayList<>(dbs.size());
    Chainbase lane = null;
    for (Chainbase db : dbs) {
      if ("account-asset".equals(db.getDbName())) {
        lane = db;
      } else {
        ordered.add(db);
      }
    }
    if (lane != null) {
      ordered.add(lane);
    }
    return ordered;
  }

  public ArchiveWalBinding getLatestArchiveWalBinding() {
    return latestArchiveWalBinding;
  }

  public ArchiveWalBinding getRecoveredArchiveWalBinding() {
    return recoveredArchiveWalBinding;
  }

  private boolean isV2Open() {
    return checkpointVersion == 2;
  }

  private byte[] simpleEncode(String s) {
    byte[] bytes = s.getBytes();
    byte[] length = Ints.toByteArray(bytes.length);
    byte[] r = new byte[4 + bytes.length];
    System.arraycopy(length, 0, r, 0, 4);
    System.arraycopy(bytes, 0, r, 4, bytes.length);
    return r;
  }

  @Slf4j(topic = "DB")
  @Getter // only for unit test
  public static class Session implements ISession {

    private SnapshotManager snapshotManager;
    private boolean applySnapshot = true;
    private boolean disableOnExit = false;
    private final int sessionDepth;

    public Session(SnapshotManager snapshotManager) {
      this(snapshotManager, false);
    }

    public Session(SnapshotManager snapshotManager, boolean disableOnExit) {
      this.snapshotManager = snapshotManager;
      this.disableOnExit = disableOnExit;
      this.sessionDepth = snapshotManager.activeSession;
    }

    @Override
    public void commit() {
      snapshotManager.commit();
      applySnapshot = false;
    }

    @Override
    public void commit(BlockSnapshotMeta meta) {
      snapshotManager.commit(meta);
      applySnapshot = false;
    }

    @Override
    public void finalizeBlock(BlockSnapshotMeta meta,
        Consumer<ISession.BlockFinalization> pipeline) {
      synchronized (snapshotManager) {
        if (!applySnapshot || sessionDepth != snapshotManager.activeSession) {
          throw new IllegalStateException("Session is completed or is not the active layer");
        }
        snapshotManager.finalizeBlock(meta, pipeline, () -> applySnapshot = false);
      }
    }

    @Override
    public void revoke() {
      if (applySnapshot) {
        snapshotManager.revoke();
      }

      applySnapshot = false;
    }

    @Override
    public void merge() {
      if (applySnapshot) {
        snapshotManager.merge();
      }

      applySnapshot = false;
    }

    @Override
    public void destroy() {
      try {
        if (applySnapshot) {
          snapshotManager.revoke();
        }
      } catch (Exception e) {
        logger.error("Revoke database error.", e);
      }
      if (disableOnExit) {
        snapshotManager.disable();
      }
    }

    @Override
    public void close() {
      try {
        if (applySnapshot) {
          snapshotManager.revoke();
        }
      } catch (Exception e) {
        logger.error("Revoke database error.", e);
        throw new RevokingStoreIllegalStateException(e);
      }
      if (disableOnExit) {
        snapshotManager.disable();
      }
    }
  }

}
