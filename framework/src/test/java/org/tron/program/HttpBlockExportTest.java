package org.tron.program;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.nio.file.Path;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.tron.common.utils.BlockFile;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.services.http.Util;
import org.tron.protos.Protocol.Block;
import org.tron.protos.Protocol.BlockHeader;
import org.tron.protos.Protocol.Transaction;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.protos.contract.BalanceContract.TransferContract;

public class HttpBlockExportTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void parsePrintedBlockResponseAndVerifyChunk() throws Exception {
    Block block = block(100, new byte[32], transferTransaction());
    String response = "{\"block\":[" + Util.printBlock(block, false) + "]}";

    List<BlockFile.Record> records = HttpBlockExport.parseResponse(response);

    assertEquals(1, records.size());
    assertEquals(100, records.get(0).getHeight());
    assertEquals(block, records.get(0).getBlock());

    Path output = temporaryFolder.newFile("100-100.dat").toPath();
    BlockFile.write(output, 100, 100, true, height -> records.get(0));
    HttpBlockExport.verifyChunk(output, 100, 100);
  }

  @Test
  public void rejectMismatchedBlockId() {
    Block block = block(100, new byte[32], transferTransaction());
    String response = "{\"block\":[" + Util.printBlock(block, false)
        .replaceFirst("\\\"blockID\\\":\\\".", "\\\"blockID\\\":\\\"f") + "]}";

    try {
      HttpBlockExport.parseResponse(response);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Computed block ID mismatch"));
      return;
    }
    throw new AssertionError("Expected mismatched block ID to fail");
  }

  private static Block block(long height, byte[] parentHash, Transaction transaction) {
    BlockHeader.raw raw = BlockHeader.raw.newBuilder()
        .setNumber(height)
        .setParentHash(com.google.protobuf.ByteString.copyFrom(parentHash))
        .setTimestamp(1_700_000_000_000L + height)
        .build();
    BlockHeader header = BlockHeader.newBuilder().setRawData(raw).build();
    Block block = Block.newBuilder().setBlockHeader(header).addTransactions(transaction).build();
    assertEquals(height, new BlockCapsule(block).getNum());
    return block;
  }

  private static Transaction transferTransaction() {
    TransferContract transfer = TransferContract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(new byte[] {0x41, 1}))
        .setToAddress(ByteString.copyFrom(new byte[] {0x41, 2}))
        .setAmount(10)
        .build();
    Transaction.Contract contract = Transaction.Contract.newBuilder()
        .setType(ContractType.TransferContract)
        .setParameter(Any.pack(transfer))
        .build();
    Transaction.raw raw = Transaction.raw.newBuilder()
        .addContract(contract)
        .setTimestamp(1_700_000_000_000L)
        .setExpiration(1_700_000_060_000L)
        .build();
    return Transaction.newBuilder().setRawData(raw)
        .addSignature(ByteString.copyFrom(new byte[] {1, 2, 3}))
        .build();
  }
}
