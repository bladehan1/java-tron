package org.tron.core.db2.archive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoverySnapshot;
import org.tron.core.db2.archive.ArchiveRecoveryExecutor.RecoveryStorage;
import org.tron.core.db2.archive.ArchiveRecoveryPlanner.ActionType;

public class ArchiveRecoveryExecutorTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void secondRestartReadsDurableProgressAndExecutesOnlyRemainingParticipant()
      throws Exception {
    Path directory = temporaryFolder.newFolder("second-crash").toPath();
    List<String> participants = Arrays.asList("account", "account-asset", "storage-row");
    DurableTestStorage.initialize(directory, 12, 10, 7,
        heads("account", 10L, "account-asset", 8L, "storage-row", 7L));

    DurableTestStorage firstStorage = new DurableTestStorage(directory, participants);
    ArchiveRecoveryExecutor first = new ArchiveRecoveryExecutor(firstStorage, action -> {
      if (action.getType() == ActionType.REPLAY_PARTICIPANT
          && "account-asset".equals(action.getParticipant())) {
        throw new IOException("injected crash after participant progress force");
      }
    });
    assertThrows(ArchivePersistenceException.class, first::recover);
    assertEquals(Arrays.asList("account-asset:9-10"), firstStorage.getReplays());

    RecoverySnapshot afterCrash = new DurableTestStorage(directory, participants).scan();
    assertEquals(10, afterCrash.getHistoryHead());
    assertEquals(10, afterCrash.getParticipantHeads().get("account-asset").longValue());
    assertEquals(7, afterCrash.getParticipantHeads().get("storage-row").longValue());
    assertEquals(7, afterCrash.getReaderVisibleHead());

    DurableTestStorage secondStorage = new DurableTestStorage(directory, participants);
    new ArchiveRecoveryExecutor(secondStorage).recover();
    assertEquals(Arrays.asList("storage-row:8-10"), secondStorage.getReplays());

    RecoverySnapshot recovered = new DurableTestStorage(directory, participants).scan();
    assertEquals(10, recovered.getHistoryHead());
    assertEquals(10, recovered.getCheckpointHead());
    assertEquals(10, recovered.getReaderVisibleHead());
    recovered.getParticipantHeads().values().forEach(head -> assertEquals(10, head.longValue()));

    DurableTestStorage thirdStorage = new DurableTestStorage(directory, participants);
    assertEquals(0, new ArchiveRecoveryExecutor(thirdStorage).recover().getActions().size());
    assertEquals(0, thirdStorage.getReplays().size());
  }

  private static Map<String, Long> heads(Object... values) {
    Map<String, Long> heads = new LinkedHashMap<>();
    for (int index = 0; index < values.length; index += 2) {
      heads.put((String) values[index], (Long) values[index + 1]);
    }
    return heads;
  }

  private static final class DurableTestStorage implements RecoveryStorage {
    private static final String HISTORY = "history.head";
    private static final String CHECKPOINT = "checkpoint.head";
    private static final String READER = "reader.head";

    private final Path directory;
    private final List<String> participants;
    private final List<String> replays = new ArrayList<>();

    private DurableTestStorage(Path directory, List<String> participants) {
      this.directory = directory;
      this.participants = new ArrayList<>(participants);
    }

    private static void initialize(Path directory, long historyHead, long checkpointHead,
        long readerHead, Map<String, Long> participantHeads) throws IOException {
      Files.createDirectories(directory);
      writeLong(directory.resolve(HISTORY), historyHead);
      writeLong(directory.resolve(CHECKPOINT), checkpointHead);
      writeLong(directory.resolve(READER), readerHead);
      for (Map.Entry<String, Long> entry : participantHeads.entrySet()) {
        writeLong(participantPath(directory, entry.getKey()), entry.getValue());
      }
    }

    @Override
    public RecoverySnapshot scan() throws IOException {
      Map<String, Long> participantHeads = new LinkedHashMap<>();
      for (String participant : participants) {
        participantHeads.put(participant, readLong(participantPath(directory, participant)));
      }
      return new RecoverySnapshot(readLong(directory.resolve(HISTORY)),
          readLong(directory.resolve(CHECKPOINT)), participantHeads,
          readLong(directory.resolve(READER)));
    }

    @Override
    public void truncateHistoryAndSync(long historyHead) throws IOException {
      writeLong(directory.resolve(HISTORY), historyHead);
    }

    @Override
    public void replayParticipantAndSyncProgress(String participant, long firstEpoch,
        long lastEpoch) throws IOException {
      replays.add(participant + ":" + firstEpoch + "-" + lastEpoch);
      writeLong(participantPath(directory, participant), lastEpoch);
    }

    @Override
    public void publishReaderHeadAndSync(long readerVisibleHead) throws IOException {
      writeLong(directory.resolve(READER), readerVisibleHead);
    }

    private List<String> getReplays() {
      return replays;
    }

    private static Path participantPath(Path directory, String participant) {
      return directory.resolve("participant-" + participant + ".head");
    }

    private static long readLong(Path path) throws IOException {
      byte[] encoded = Files.readAllBytes(path);
      if (encoded.length != Long.BYTES) {
        throw new IOException("Invalid durable test progress length");
      }
      return ByteBuffer.wrap(encoded).getLong();
    }

    private static void writeLong(Path path, long value) throws IOException {
      Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).putLong(value);
        buffer.flip();
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
        channel.force(true);
      }
      Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      HistorySegmentStore.syncDirectory(path.getParent());
    }
  }
}
