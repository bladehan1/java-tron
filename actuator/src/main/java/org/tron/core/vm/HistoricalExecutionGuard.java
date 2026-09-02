package org.tron.core.vm;

/** Runtime allowlist for capabilities reachable only after bytecode dispatch. */
public final class HistoricalExecutionGuard {

  private HistoricalExecutionGuard() {
  }

  public static void requirePrecompileAllowed(
      PrecompiledContracts.PrecompiledContract contract) {
    if (contract instanceof PrecompiledContracts.Identity
        || contract instanceof PrecompiledContracts.Sha256
        || contract instanceof PrecompiledContracts.Ripempd160
        || contract instanceof PrecompiledContracts.ECRecover
        || contract instanceof PrecompiledContracts.ModExp
        || contract instanceof PrecompiledContracts.BN128Addition
        || contract instanceof PrecompiledContracts.BN128Multiplication
        || contract instanceof PrecompiledContracts.BN128Pairing
        || contract instanceof PrecompiledContracts.EthRipemd160
        || contract instanceof PrecompiledContracts.Blake2F
        || contract instanceof PrecompiledContracts.P256Verify) {
      return;
    }
    throw new HistoricalCapabilityException(
        "Precompile is not allowed by historical execution: "
            + contract.getClass().getSimpleName());
  }
}
