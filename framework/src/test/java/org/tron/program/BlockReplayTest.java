package org.tron.program;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.utils.BlockFile;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.consensus.ConsensusService;
import org.tron.core.db.RevokingDatabase;
import org.tron.core.net.TronNetDelegate;

public class BlockReplayTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void shouldVerifyACompleteFileWithoutApplying() throws Exception {
    BlockId parent = new BlockId(Sha256Hash.ZERO_HASH, 9);
    BlockCapsule[] blocks = blocks(parent, 10, 2);
    Path input = write(blocks);

    BlockReplay.ReplayResult result = BlockReplay.replay(input, null, null, 1,
        Long.MAX_VALUE, false);

    assertTrue(result.format().contains("mode=verify"));
    assertTrue(result.format().contains("processed=2"));
    assertTrue(result.format().contains("measured=1"));
  }

  @Test
  public void shouldVerifyThroughCommandLine() throws Exception {
    BlockId parent = new BlockId(Sha256Hash.ZERO_HASH, 9);
    Path input = write(blocks(parent, 10, 2));
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ByteArrayOutputStream error = new ByteArrayOutputStream();

    int exitCode = BlockReplay.execute(new String[] {"--input", input.toString()},
        new PrintStream(output), new PrintStream(error));

    assertEquals(error.toString(), 0, exitCode);
    assertTrue(output.toString().contains("mode=verify"));
    assertTrue(output.toString().contains("processed=2"));
  }

  @Test
  public void shouldVerifyConsecutiveDirectoryWithGlobalLimitAndWarmup() throws Exception {
    BlockId parent = new BlockId(Sha256Hash.ZERO_HASH, 9);
    BlockCapsule[] blocks = blocks(parent, 10, 4);
    Path directory = temporaryFolder.newFolder("chunks").toPath();
    write(directory.resolve("second.dat"), new BlockCapsule[] {blocks[2], blocks[3]});
    write(directory.resolve("first.dat"), new BlockCapsule[] {blocks[0], blocks[1]});

    BlockReplay.ReplayResult result = BlockReplay.replayInput(directory, null, null, 1, 3, false);

    assertTrue(result.format().contains("range=[10,12]"));
    assertTrue(result.format().contains("processed=3"));
    assertTrue(result.format().contains("warmup=1"));
    assertTrue(result.format().contains("measured=2"));
  }

  @Test
  public void shouldRejectParentMismatchBetweenChunks() throws Exception {
    BlockId parent = new BlockId(Sha256Hash.ZERO_HASH, 9);
    BlockCapsule first = blocks(parent, 10, 1)[0];
    BlockCapsule second = blocks(new BlockId(Sha256Hash.ZERO_HASH, 10), 11, 1)[0];
    Path directory = temporaryFolder.newFolder("parent-mismatch").toPath();
    write(directory.resolve("10.dat"), new BlockCapsule[] {first});
    write(directory.resolve("11.dat"), new BlockCapsule[] {second});

    assertThrows(IllegalArgumentException.class,
        () -> BlockReplay.replayInput(directory, null, null, 0, Long.MAX_VALUE, false));
  }

  @Test
  public void shouldApplyBlocksThroughSyncPath() throws Exception {
    BlockId parent = new BlockId(Sha256Hash.ZERO_HASH, 9);
    BlockCapsule[] blocks = blocks(parent, 10, 2);
    Path input = write(blocks);
    TronNetDelegate tronNetDelegate = mock(TronNetDelegate.class);
    ChainBaseManager chainBaseManager = mock(ChainBaseManager.class);
    when(chainBaseManager.getHeadBlockNum()).thenReturn(9L, 10L, 11L);
    when(chainBaseManager.getHeadBlockId()).thenReturn(parent, blocks[0].getBlockId(),
        blocks[1].getBlockId());

    BlockReplay.ReplayResult result = BlockReplay.replay(input, tronNetDelegate, chainBaseManager,
        0, Long.MAX_VALUE, true);

    ArgumentCaptor<BlockCapsule> captor = ArgumentCaptor.forClass(BlockCapsule.class);
    verify(tronNetDelegate, times(2)).processBlock(captor.capture(), eq(true));
    assertEquals(blocks[0].getBlockId(), captor.getAllValues().get(0).getBlockId());
    assertEquals(blocks[1].getBlockId(), captor.getAllValues().get(1).getBlockId());
    assertTrue(result.format().contains("mode=apply"));
    assertTrue(result.format().contains("processed=2"));
  }

  @Test
  public void shouldStartConsensusBeforeApplyingBlocks() {
    TronApplicationContext context = mock(TronApplicationContext.class);
    ConsensusService consensusService = mock(ConsensusService.class);
    when(context.getBean(ConsensusService.class)).thenReturn(consensusService);

    BlockReplay.startConsensus(context);

    verify(consensusService).start();
  }

  @Test
  public void shouldFlushPendingSnapshotsAfterSuccessfulReplay() {
    TronApplicationContext context = mock(TronApplicationContext.class);
    RevokingDatabase revokingDatabase = mock(RevokingDatabase.class);
    when(context.getBean(RevokingDatabase.class)).thenReturn(revokingDatabase);

    BlockReplay.flushPending(context);

    verify(revokingDatabase).flushPending();
  }

  @Test
  public void shouldRejectD0WithDifferentHead() throws Exception {
    BlockId fileParent = new BlockId(Sha256Hash.ZERO_HASH, 9);
    BlockCapsule[] blocks = blocks(fileParent, 10, 1);
    Path input = write(blocks);
    TronNetDelegate tronNetDelegate = mock(TronNetDelegate.class);
    ChainBaseManager chainBaseManager = mock(ChainBaseManager.class);
    when(chainBaseManager.getHeadBlockNum()).thenReturn(9L);
    when(chainBaseManager.getHeadBlockId())
        .thenReturn(new BlockId(Sha256Hash.of(true, new byte[] {1}), 9));

    assertThrows(IllegalArgumentException.class,
        () -> BlockReplay.replay(input, tronNetDelegate, chainBaseManager,
            0, Long.MAX_VALUE, true));
    verify(tronNetDelegate, times(0)).processBlock(any(BlockCapsule.class), eq(true));
  }

  private Path write(BlockCapsule[] blocks) throws Exception {
    Path input = temporaryFolder.getRoot().toPath().resolve("blocks.dat");
    return write(input, blocks);
  }

  private Path write(Path input, BlockCapsule[] blocks) throws Exception {
    BlockFile.write(input, blocks[0].getNum(), blocks[blocks.length - 1].getNum(), false,
        height -> {
          BlockCapsule block = blocks[(int) (height - blocks[0].getNum())];
          return new BlockFile.Record(height, block.getBlockId().getBytes(), block.getData());
        });
    return input;
  }

  private static BlockCapsule[] blocks(BlockId parent, long start, int count) {
    BlockCapsule[] blocks = new BlockCapsule[count];
    BlockId previous = parent;
    for (int i = 0; i < count; i++) {
      long height = start + i;
      blocks[i] = new BlockCapsule(height, previous, height * 3000, ByteString.EMPTY);
      previous = blocks[i].getBlockId();
    }
    return blocks;
  }
}
