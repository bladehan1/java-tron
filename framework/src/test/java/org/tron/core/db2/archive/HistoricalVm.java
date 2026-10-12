package org.tron.core.db2.archive;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import org.bouncycastle.util.encoders.Hex;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.runtime.ProgramResult;
import org.tron.core.actuator.VMActuator;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.db.TransactionContext;
import org.tron.core.db.TransactionContext.ExecutionMode;
import org.tron.core.vm.Op;
import org.tron.core.vm.config.ConfigLoader;
import org.tron.core.vm.repository.HistoricalRepositoryProvider;
import org.tron.protos.Protocol;
import org.tron.protos.Protocol.Transaction.Contract.ContractType;
import org.tron.protos.contract.SmartContractOuterClass.TriggerSmartContract;

/** Runs historical constant calls exactly like Wallet does, plus tiny bytecode builders. */
final class HistoricalVm {

  static final byte[] OWNER = HistoricalTestState.address(1);

  private HistoricalVm() {
  }

  static ProgramResult call(HistoricalQuerySession session, byte[] contract) throws Exception {
    return call(session, OWNER, contract, new byte[0]);
  }

  static ProgramResult call(HistoricalQuerySession session, byte[] owner, byte[] contract,
      byte[] data) throws Exception {
    return call(session, owner, contract, data,
        new BlockCapsule(Protocol.Block.newBuilder().build()));
  }

  /** Executes in the context of block {@code block} (NUMBER, TIMESTAMP, ... come from it). */
  static ProgramResult call(HistoricalQuerySession session, byte[] owner, byte[] contract,
      byte[] data, BlockCapsule block) throws Exception {
    TriggerSmartContract trigger = TriggerSmartContract.newBuilder()
        .setOwnerAddress(ByteString.copyFrom(owner))
        .setContractAddress(ByteString.copyFrom(contract))
        .setData(ByteString.copyFrom(data)).build();
    TransactionCapsule transaction = new TransactionCapsule(trigger,
        ContractType.TriggerSmartContract);
    TransactionContext context = new TransactionContext(block, transaction, null, true, false,
        ExecutionMode.HISTORICAL_CONSTANT, session);
    VMActuator actuator = new VMActuator(true, HistoricalRepositoryProvider.INSTANCE);
    long previous = CommonParameter.getInstance().getConstantCallTimeoutMs();
    // Other VM tests in the same JVM leave the ConfigLoader test switch on; historical loading
    // refuses to run with it (fail closed), so pin it off for the call.
    boolean previousDisable = ConfigLoader.disable;
    CommonParameter.getInstance().setConstantCallTimeoutMs(60_000L);
    ConfigLoader.disable = false;
    try {
      session.checkActive();
      actuator.validate(context);
      actuator.execute(context);
      session.checkActive();
    } finally {
      org.tron.core.vm.config.VMConfig.clearLocalSnapshot();
      ConfigLoader.disable = previousDisable;
      CommonParameter.getInstance().setConstantCallTimeoutMs(previous);
    }
    return context.getProgramResult();
  }

  // ---------------------------------------------------------------------------------------
  // Bytecode builders (hex strings)

  static byte[] code(String... parts) {
    return Hex.decode(String.join("", parts));
  }

  static String push20(byte[] address21) {
    return "73" + Hex.toHexString(Arrays.copyOfRange(address21, 1, 21));
  }

  static String push20Raw(int lastByte) {
    byte[] address = new byte[21];
    address[20] = (byte) lastByte;
    return push20(address);
  }

  /** MSTORE the stack top at 0 and RETURN 32 bytes. */
  static final String RETURN_TOP = "60005260206000f3";

  /** PUSH1 0x2a, return it. */
  static final String RETURN_42 = "602a" + RETURN_TOP;

  /** {@code op} (CALL/DELEGATECALL/STATICCALL) to {@code target}, no data, 1,000,000 energy. */
  static String callOp(int op, String pushTarget) {
    String args = "6000600060006000";
    String value = op == Op.CALL || op == Op.CALLCODE ? "6000" : "";
    return args + value + pushTarget + "620f4240" + String.format("%02x", op);
  }

  /** Low-level call that ignores the success flag, then returns 42. */
  static byte[] callIgnoring(int op, byte[] target) {
    return code(callOp(op, push20(target)), "50", RETURN_42);
  }

  /** Solidity try/catch shape: the success flag decides the (normal) return value. */
  static byte[] callReturningFlag(int op, byte[] target) {
    return code(callOp(op, push20(target)), RETURN_TOP);
  }

  /** CALLTOKEN of {@code value} units of {@code tokenId} to {@code to}, ignoring the result. */
  static String callToken(byte[] to, long tokenId, int value) {
    return "6000600060006000" + String.format("62%06x", tokenId)
        + String.format("60%02x", value) + push20(to) + "620f4240" + "d0" + "50";
  }

  /** TOKENBALANCE(address, tokenId) returned as one word. */
  static byte[] tokenBalanceOf(byte[] address, long tokenId) {
    return code(push20(address), String.format("62%06x", tokenId), "d1", RETURN_TOP);
  }
}
