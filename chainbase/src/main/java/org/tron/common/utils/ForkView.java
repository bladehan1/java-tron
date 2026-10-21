package org.tron.common.utils;

import org.tron.core.config.Parameter.ForkBlockVersionEnum;

/** Fork-activation decisions evaluated against one state. */
@FunctionalInterface
public interface ForkView {

  boolean pass(ForkBlockVersionEnum version);
}
