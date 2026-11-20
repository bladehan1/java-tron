package org.tron.core.db;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.db2.ISession;
import org.tron.core.db2.stateroot.format.PathStateRootMetadata;
import org.tron.core.db2.stateroot.runtime.PathStateHead;
import org.tron.core.db2.stateroot.runtime.PathStateRuntimeAttachment;

public class ManagerPathStateHeaderStorageTest {
  private Manager manager;
  private BlockStore blocks;
  private PathStateRuntimeAttachment runtime;
  private PathStateHead head;
  private PathStateRootMetadata metadata;
  private BlockCapsule block;
  private byte[] root;

  @Before
  public void setUp() throws Exception {
    manager = new Manager();
    blocks = mock(BlockStore.class);
    ChainBaseManager chain = mock(ChainBaseManager.class);
    when(chain.getBlockStore()).thenReturn(blocks);
    runtime = mock(PathStateRuntimeAttachment.class);
    head = mock(PathStateHead.class);
    metadata = mock(PathStateRootMetadata.class);
    block = new BlockCapsule(10, Sha256Hash.ZERO_HASH, 30000, ByteString.EMPTY);
    byte[] privateKey = new byte[32];
    privateKey[31] = 1;
    block.sign(privateKey);
    root = new byte[32];
    Arrays.fill(root, (byte) 7);
    when(runtime.isReadyForHeaderDiagnostic(anyLong(), any())).thenReturn(true);
    when(head.getHead()).thenReturn(metadata);
    when(metadata.getBlockNumber()).thenReturn(block.getNum());
    when(metadata.getBlockHash()).thenReturn(block.getBlockId().getBytes());
    when(metadata.getStateRoot()).thenReturn(root);
    ReflectionTestUtils.setField(manager, "chainBaseManager", chain);
    ReflectionTestUtils.setField(manager, "pathStateRuntime", runtime);
    ReflectionTestUtils.setField(manager, "pathStateSnapshotHead", head);
  }

  @Test
  public void fillsMissingRootWithoutChangingSignedIdentity() {
    byte[] raw = block.getInstance().getBlockHeader().getRawData().toByteArray();
    byte[] signature = block.getInstance().getBlockHeader().getWitnessSignature().toByteArray();
    byte[] id = block.getBlockId().getBytes();
    fill();
    ArgumentCaptor<BlockCapsule> saved = ArgumentCaptor.forClass(BlockCapsule.class);
    verify(blocks).put(any(), saved.capture());
    assertArrayEquals(root, saved.getValue().getStateRoot());
    assertArrayEquals(root, block.getStateRoot());
    assertArrayEquals(raw,
        saved.getValue().getInstance().getBlockHeader().getRawData().toByteArray());
    assertArrayEquals(signature,
        saved.getValue().getInstance().getBlockHeader().getWitnessSignature().toByteArray());
    assertArrayEquals(id, new BlockCapsule(saved.getValue().getInstance()).getBlockId().getBytes());
  }

  @Test
  public void preservesBothMismatchingAndMalformedCarriedRoots() throws Exception {
    block.setStateRoot(new byte[32]);
    fill();
    assertArrayEquals(new byte[32], block.getStateRoot());
    block = new BlockCapsule(block.getInstance().toBuilder().setBlockHeader(
        block.getInstance().getBlockHeader().toBuilder()
            .setStateRoot(ByteString.copyFrom(new byte[] {1}))).build());
    fill();
    assertArrayEquals(new byte[] {1}, block.getStateRoot());
    verify(head, never()).getHead();
    verify(blocks, never()).put(any(), any());
  }

  @Test
  public void unavailableOrWrongIdentityDoesNotFill() throws Exception {
    when(runtime.isReadyForHeaderDiagnostic(anyLong(), any())).thenReturn(false);
    fill();
    verify(head, never()).getHead();
    when(runtime.isReadyForHeaderDiagnostic(anyLong(), any())).thenReturn(true);
    when(metadata.getBlockNumber()).thenReturn(9L);
    fill();
    when(metadata.getBlockNumber()).thenReturn(10L);
    when(metadata.getBlockHash()).thenReturn(new byte[32]);
    fill();
    when(head.getHead()).thenThrow(new IOException("unavailable"));
    fill();
    assertEquals(0, block.getStateRoot().length);
    verify(blocks, never()).put(any(), any());
  }

  @Test
  public void disabledPathStateDoesNotFill() {
    ReflectionTestUtils.setField(manager, "pathStateRuntime", null);
    fill();
    assertEquals(0, block.getStateRoot().length);
    verify(blocks, never()).put(any(), any());
  }

  @Test
  public void blockStoreFailurePropagatesAndDoesNotDecorateInput() {
    doThrow(new IllegalStateException("store failed")).when(blocks).put(any(), any());
    assertThrows(IllegalStateException.class, this::fill);
    assertEquals(0, block.getStateRoot().length);
  }

  @Test
  public void fillsAfterArtifactsBeforeCommitAndDiagnosesOriginalInput() {
    ISession session = mock(ISession.class);
    ISession.BlockFinalization stages = mock(ISession.BlockFinalization.class);
    doAnswer(call -> {
      Consumer<ISession.BlockFinalization> pipeline = call.getArgument(1);
      pipeline.accept(stages);
      return null;
    }).when(session).finalizeBlock(any(), any());
    ReflectionTestUtils.invokeMethod(manager, "finalizeBlockSession", session, block);
    InOrder order = inOrder(stages, blocks, runtime);
    order.verify(stages).completeArtifacts();
    order.verify(blocks).put(any(), any());
    order.verify(stages).commitSession();
    ArgumentCaptor<byte[]> carried = ArgumentCaptor.forClass(byte[].class);
    order.verify(runtime).diagnoseHeader(anyLong(), any(), carried.capture(), any());
    assertEquals(0, carried.getValue().length);
    assertArrayEquals(root, block.getStateRoot());
  }

  private void fill() {
    ReflectionTestUtils.invokeMethod(manager, "storeMissingPathStateRoot", block);
  }
}
